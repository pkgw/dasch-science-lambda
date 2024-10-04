use anyhow::{anyhow, Error};
use aws_config::SdkConfig;
use aws_sdk_s3;
use fitswcs_sys::cfitsio;
use libc::{c_char, c_int, c_long, c_longlong, c_void};
use once_cell::sync::{Lazy, OnceCell};
use std::{collections::HashMap, ffi::CStr, future::Future, io::Cursor, sync::Mutex};
use tokio::runtime;

use crate::s3buffer::S3Buffer;

#[derive(Debug)]
struct S3State {
    client: aws_sdk_s3::Client,
    bucket: String,
    key: String,
    offset: u64,
    buffer: S3Buffer,
}

impl S3State {
    fn new_from_fitsurl<S: AsRef<str>>(config: &SdkConfig, fitsurl: S) -> Result<Self, Error> {
        let fitsurl = fitsurl.as_ref();

        let (bucket, key) = fitsurl
            .split_once('/')
            .ok_or_else(|| anyhow!("invalid filename: no slash"))?;

        Ok(S3State {
            client: aws_sdk_s3::Client::new(config),
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            offset: 0,
            buffer: S3Buffer::default(),
        })
    }
}

static AWS_CONFIG: OnceCell<SdkConfig> = OnceCell::new();
static HANDLE_COUNTER: Lazy<Mutex<c_int>> = Lazy::new(|| Mutex::new(0));
static HANDLES: Lazy<Mutex<HashMap<c_int, S3State>>> = Lazy::new(|| Mutex::new(Default::default()));

/// Given a FITS handle from the CFITSIO layer, invoke an closure with
/// its corresponding S3State object.
fn with_handle<F>(handle: c_int, inner: F) -> c_int
where
    F: FnOnce(&mut S3State) -> c_int,
{
    let mut ht = HANDLES.lock().unwrap();
    let state = match ht.get_mut(&handle) {
        Some(s) => s,

        None => {
            eprintln!("S3 op failed: no such open handle #{}", handle);
            return cfitsio::FILE_NOT_OPENED;
        }
    };

    inner(state)
}

/// Spin up a temporary runtime to invoke an asynchronous function that returns
/// nothing on success, or a CFITSIO error code on error.
///
/// As far as I can tell, this needs to be separate from `with_handle()` because
/// async closures with arguments aren't yet available.
///
/// Note that this function does double duty: it launders async code, and also
/// launders results into plain integer status codes.
fn block_on<F: Future<Output = Result<(), c_int>>>(future: F) -> c_int {
    let rt = runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    match rt.block_on(future) {
        Ok(_) => 0,
        Err(c) => c,
    }
}

/// Perform global initialization of this driver. Called immediately upon
/// registration.
pub extern "C" fn s3fits_driver_init() -> c_int {
    println!("S3F: init");
    0
}

pub extern "C" fn s3fits_driver_fitsshutdown() -> c_int {
    println!("S3F: shutdown");
    0
}

pub extern "C" fn s3fits_driver_setoptions(options: c_int) -> c_int {
    println!("S3F: setoptions {}", options);
    0
}

pub unsafe extern "C" fn s3fits_driver_getoptions(options: *mut c_int) -> c_int {
    println!("S3F: getoptions");
    *options = 0;
    0
}

pub unsafe extern "C" fn s3fits_driver_getversion(version: *mut c_int) -> c_int {
    println!("S3F: getversion");
    *version = 0;
    0
}

/// Checkfile is used to alter the `urltype` ("s3://"), `infile` (everything
/// after the `://`), and/or `outfile` ("the name of the output file that the
/// input file is to be copied to prior to opening").
pub unsafe extern "C" fn s3fits_driver_checkfile(
    urltype: *const c_char,
    infile: *const c_char,
    outfile: *const c_char,
) -> c_int {
    let urltype = CStr::from_ptr(urltype);
    let infile = CStr::from_ptr(infile);
    let outfile = CStr::from_ptr(outfile);
    println!("S3F: checkfile {:?} {:?} {:?}", urltype, infile, outfile);
    0
}

/// Open a handle to the specified FITS file.
pub unsafe extern "C" fn s3fits_driver_fitsopen(
    filename: *const c_char,
    rwmode: c_int,
    driverhandle: *mut c_int,
) -> c_int {
    let filename = CStr::from_ptr(filename);
    let filename = String::from_utf8_lossy(filename.to_bytes());

    println!(
        "S3F: fitsopen {:?} {:?} {:?}",
        filename, rwmode, driverhandle
    );

    // We only work in read-only mode.
    if rwmode != cfitsio::READONLY {
        return cfitsio::FILE_NOT_OPENED;
    }

    let handle = {
        let mut hc = HANDLE_COUNTER.lock().unwrap();
        let result = *hc;
        *hc += 1;
        result
    };

    *driverhandle = handle;

    // Can't fail - this function only gets invoked if our driver gets
    // registered, and that can't happen without setting the config.
    let config = AWS_CONFIG.get().unwrap();

    let state = match S3State::new_from_fitsurl(config, &filename) {
        Ok(s) => s,

        Err(e) => {
            eprintln!("S3 fitsopen failed: {}", e);
            return cfitsio::FILE_NOT_OPENED;
        }
    };

    {
        let mut ht = HANDLES.lock().unwrap();
        ht.insert(handle, state);
    }

    0
}

pub extern "C" fn s3fits_driver_fitscreate(
    filename: *const c_char,
    driverhandle: *mut c_int,
) -> c_int {
    println!("S3F: fitscreate {:?} {:?}", filename, driverhandle);
    0
}

pub extern "C" fn s3fits_driver_fitstruncate(driverhandle: c_int, filesize: c_longlong) -> c_int {
    println!("S3F: fitstruncate {:?} {:?}", driverhandle, filesize);
    0
}

pub extern "C" fn s3fits_driver_fitsclose(driverhandle: c_int) -> c_int {
    println!("S3F: fitsclose {:?}", driverhandle);
    0
}

pub extern "C" fn s3fits_driver_fremove(filename: *const c_char) -> c_int {
    println!("S3F: fremove {:?}", filename);
    0
}

/// Get the size of the FITS data at the associated handle.
pub extern "C" fn s3fits_driver_size(driverhandle: c_int, sizex: *mut c_longlong) -> c_int {
    println!("S3F: size {:?}", driverhandle);

    with_handle(driverhandle, |state| {
        block_on(async move {
            let result = state
                .client
                .head_object()
                .bucket(&state.bucket)
                .key(&state.key)
                .send()
                .await
                .map_err(|e| {
                    eprintln!("S3 op failed: {}", e);
                    cfitsio::READ_ERROR
                })?;

            let cl = result.content_length.ok_or_else(|| {
                eprintln!("S3 op failed: no Content-Length available");
                cfitsio::READ_ERROR
            })?;

            unsafe {
                *sizex = cl as c_longlong;
            }

            Ok(())
        })
    })
}

pub extern "C" fn s3fits_driver_flush(driverhandle: c_int) -> c_int {
    println!("S3F: flush {:?}", driverhandle);
    0
}

pub extern "C" fn s3fits_driver_seek(driverhandle: c_int, offset: c_longlong) -> c_int {
    println!("S3F: seek {:?} {:?}", driverhandle, offset);

    with_handle(driverhandle, |state| {
        state.offset = offset as u64;
        0
    })
}

pub extern "C" fn s3fits_driver_fitsread(
    driverhandle: c_int,
    buffer: *mut c_void,
    nbytes: c_long,
) -> c_int {
    println!("S3F: fitsread {:?} {:?}", driverhandle, nbytes);

    // FIXME: should be using MaybeUninit here somehow, I think, but that
    // doesn't appear to be compatible with Cursor. We might need to manually
    // implement the copying rather than relying on `impl Write for Cursor<&mut
    // [u8]>`. There's a currently-unstable feature `maybe_uninit_slice` that
    // might be relevant.
    let buffer = unsafe { std::slice::from_raw_parts_mut(buffer as *mut u8, nbytes as usize) };
    let dest = Cursor::new(buffer);
    let nbytes = nbytes as u64;

    with_handle(driverhandle, |state| {
        block_on(async move {
            state
                .buffer
                .read_into(
                    state
                        .client
                        .get_object()
                        .bucket(&state.bucket)
                        .key(&state.key),
                    state.offset,
                    nbytes as usize,
                    dest,
                )
                .await
                .map_err(|e| {
                    eprintln!("S3 GetObject read failed: {}", e);
                    cfitsio::READ_ERROR
                })?;
            state.offset += nbytes;
            Ok(())
        })
    })
}

pub extern "C" fn s3fits_driver_fitswrite(
    driverhandle: c_int,
    buffer: *const c_void,
    nbytes: c_long,
) -> c_int {
    println!(
        "S3F: fitswrite {:?} {:?} {:?}",
        driverhandle, buffer, nbytes
    );
    0
}

pub fn register(config: SdkConfig) {
    let _ = AWS_CONFIG.set(config);

    let result = unsafe {
        cfitsio::fits_register_driver(
            c"s3://".as_ptr(),
            s3fits_driver_init as *const _,
            s3fits_driver_fitsshutdown as *const _,
            s3fits_driver_setoptions as *const _,
            s3fits_driver_getoptions as *const _,
            s3fits_driver_getversion as *const _,
            s3fits_driver_checkfile as *const _,
            s3fits_driver_fitsopen as *const _,
            s3fits_driver_fitscreate as *const _,
            s3fits_driver_fitstruncate as *const _,
            s3fits_driver_fitsclose as *const _,
            s3fits_driver_fremove as *const _,
            s3fits_driver_size as *const _,
            s3fits_driver_flush as *const _,
            s3fits_driver_seek as *const _,
            s3fits_driver_fitsread as *const _,
            s3fits_driver_fitswrite as *const _,
        )
    };

    println!("reg result: {}", result);
}
