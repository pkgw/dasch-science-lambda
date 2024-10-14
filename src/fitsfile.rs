use anyhow::{bail, Result};
use fitswcs_sys::cfitsio;
use libc::{self, c_char, c_int, c_longlong, c_void, size_t};
use ndarray::{Array, Ix2};
use std::{ffi::CString, io::Write, pin::Pin};

use crate::wcs;

#[derive(Debug)]
pub struct FitsFile {
    handle: cfitsio::FitsHandle,
    mem_buf: *mut c_void,
    mem_size: size_t,
}

/// We need to manually declare sendability due to the pointer type in the
/// struct. The only code that expects that pointer to be non-null is "gated"
/// inside a `Pin<Box<>>` type, in which case we're good. I hope.
unsafe impl Send for FitsFile {}

/// Our error handling is super lame.
macro_rules! try_cfitsio {
    ($status:expr) => {{
        let s = $status;

        if s != 0 {
            bail!("cfitsio error code {}", s);
        }
    }};
}

impl FitsFile {
    /// Open a FITS file
    pub fn open<S: AsRef<str>>(url: S) -> Result<Self> {
        let mut handle: cfitsio::FitsHandle = std::ptr::null_mut();
        let c_url = CString::new(url.as_ref())?;
        let mut status = 0;

        try_cfitsio!(unsafe {
            cfitsio::ffopen(&mut handle, c_url.as_ptr(), cfitsio::READONLY, &mut status)
        });

        Ok(FitsFile {
            handle,
            mem_buf: std::ptr::null_mut(),
            mem_size: 0,
        })
    }

    /// Create a new FITS "file" backed only in memory.
    ///
    /// The resulting object must be pinned because CFITSIO holds a pointer to
    /// our struct fields that hold the buffer information. If the object were
    /// to move, CFITIO's pointer would still point to the old location. It is
    /// pretty awesome that Rust gives us all of the tools to both and
    /// understand and handle the issues at play here.
    pub fn create_mem() -> Result<Pin<Box<Self>>> {
        const DELTASIZE: size_t = 1048576;

        let mut result = Box::new(FitsFile {
            handle: std::ptr::null_mut(),
            mem_buf: std::ptr::null_mut(),
            mem_size: 0,
        });

        let mut status = 0;

        unsafe {
            try_cfitsio!(cfitsio::ffimem(
                &mut result.handle,
                &mut result.mem_buf,
                &mut result.mem_size,
                DELTASIZE,
                libc::realloc as *const _,
                &mut status
            ));
        }

        Ok(Box::into_pin(result))
    }

    /// Move to the specified HDU. Unlike the underlying library, the HDU
    /// numbers here are zero-based.
    pub fn move_to_hdu(&mut self, hdunum: u16) -> Result<()> {
        let mut status = 0;

        try_cfitsio!(unsafe {
            cfitsio::ffmahd(
                self.handle,
                hdunum as c_int + 1,
                std::ptr::null_mut(),
                &mut status,
            )
        });

        Ok(())
    }

    /// Get the dimensions of the image in the current HDU. These are reversed
    /// from FITS/Fortran order to standard C order: (height, width) rather than
    /// (width, height).
    pub fn get_dimensions(&mut self) -> Result<Vec<u64>> {
        let mut naxis: c_int = 0;
        let mut status = 0;

        try_cfitsio!(unsafe { cfitsio::ffgidm(self.handle, &mut naxis, &mut status) });

        let mut buf = vec![0; naxis as usize];

        try_cfitsio!(unsafe {
            cfitsio::ffgiszll(self.handle, naxis, buf.as_mut_ptr(), &mut status)
        });

        let n = buf.len();
        let mut result = Vec::with_capacity(n);

        for i in 0..n {
            result.push(buf[n - 1 - i] as u64);
        }

        Ok(result)
    }

    /// Get a Wcs object based on the current HDU's headers.
    pub fn get_wcs(&mut self) -> Result<wcs::Wcs> {
        let mut header: *const c_char = std::ptr::null();
        let mut nkeys: c_int = 0;
        let mut status: c_int = 0;

        let wcs = unsafe {
            try_cfitsio!(cfitsio::ffcnvthdr2str(
                self.handle,
                0,
                std::ptr::null_mut(),
                0,
                &mut header,
                &mut nkeys,
                &mut status,
            ));

            let wcs = wcs::Wcs::new_raw(header, nkeys)?;
            libc::free(header as *mut _);
            wcs
        };

        Ok(wcs)
    }

    /// Read a rectangle of pixels from the image. We assume that the datatype
    /// is `c_short`. The pixel indices are 0-based, unlike how the underlying
    /// library expects.
    ///
    /// For DASCH's compressed images, the optimal read strategy is to read
    /// row-by-row, since each row is a "tile" in the compression mechanism.
    pub fn read_rectangle(
        &mut self,
        x0: usize,
        y0: usize,
        width: usize,
        height: usize,
    ) -> Result<Array<i16, Ix2>> {
        let mut arr = Array::uninit((height, width));
        let mut status = 0;
        let img_width = self.get_dimensions()?[1];
        let nelem = width as c_longlong;

        for iy in 0..height {
            let startelem = (y0 + iy) * img_width as usize + x0 + 1;
            let ptr = arr.get_mut_ptr((iy, 0)).unwrap();

            try_cfitsio!(unsafe {
                cfitsio::ffgpvi(
                    self.handle,
                    1,                       // group - always 1
                    startelem as c_longlong, // start pixel number
                    nelem,                   // number of pixels to read
                    0,                       // value to use for null/undefined
                    ptr as *mut _,
                    std::ptr::null_mut(), // output int: whether any null/undef values were encountered
                    &mut status,
                )
            });
        }

        Ok(unsafe { arr.assume_init() })
    }

    /// Write a basic image header.
    ///
    /// Hardcoding for DASCH's needs here.
    pub fn write_square_image_header(&mut self, size: u64) -> Result<()> {
        let mut status = 0;
        let naxes = [size as c_longlong, size as c_longlong];

        try_cfitsio!(unsafe { cfitsio::ffphpsll(self.handle, 16, 2, naxes.as_ptr(), &mut status) });

        Ok(())
    }

    /// Set a string-valued header keyword in the current HDU.
    ///
    /// Ideally we'd use a trait and type inference rather than type-specific
    /// methods, but the pointer juggling is enough of a pain that I don't want
    /// to deal with it right now.
    pub fn set_string_header<S1: AsRef<str>, S2: AsRef<str>>(
        &mut self,
        key: S1,
        value: S2,
    ) -> Result<()> {
        let key = CString::new(key.as_ref())?;
        let value = CString::new(value.as_ref())?;
        let mut status = 0;

        try_cfitsio!(unsafe {
            cfitsio::ffuky(
                self.handle,
                cfitsio::TSTRING,
                key.as_ptr(),
                value.as_ptr() as *const _,
                std::ptr::null(),
                &mut status,
            )
        });

        Ok(())
    }

    /// Set a f64-valued header keyword in the current HDU.
    pub fn set_f64_header<S: AsRef<str>>(&mut self, key: S, value: f64) -> Result<()> {
        let key = CString::new(key.as_ref())?;
        let mut status = 0;

        try_cfitsio!(unsafe {
            cfitsio::ffuky(
                self.handle,
                cfitsio::TDOUBLE,
                key.as_ptr(),
                &value as *const _ as *const _,
                std::ptr::null(),
                &mut status,
            )
        });

        Ok(())
    }

    /// Write image pixels. We assume that the datatype is `c_short`. The pixel
    /// indices are 0-based, unlike how the underlying library expects.
    pub fn write_pixels(&mut self, data: &Array<i16, Ix2>) -> Result<()> {
        let mut status = 0;
        let startelem = [1 as c_longlong, 1]; // 1-based pixel indexing

        try_cfitsio!(unsafe {
            cfitsio::ffppxll(
                self.handle,
                cfitsio::TSHORT,
                startelem.as_ptr(),
                data.len() as c_longlong,
                data.as_ptr() as *const _,
                &mut status,
            )
        });

        Ok(())
    }

    /// Consume a memory-buffered FITS file and write it into some Rust
    /// destination.
    ///
    /// Because memory-buffered FITS must be `Pin<Box<Self>>`, we use that as
    /// the receiver type to help ensure that this method is only callable on
    /// memory-buffered handles.
    pub fn into_stream<W: Write>(mut self: Pin<Box<Self>>, mut dest: W) -> Result<()> {
        let mut status = 0;

        if self.mem_buf.is_null() {
            panic!("into_stream() with null mem_buf");
        }

        unsafe {
            // Ensure that any pending I/O is finished!
            try_cfitsio!(cfitsio::ffclos(self.handle, &mut status));
            self.handle = std::ptr::null_mut();

            let slice =
                std::slice::from_raw_parts(self.mem_buf as *const u8, self.mem_size as usize);
            dest.write_all(slice)?;

            libc::free(self.mem_buf);
        }

        self.mem_buf = std::ptr::null_mut();
        self.mem_size = 0;
        Ok(())
    }
}

impl Drop for FitsFile {
    fn drop(&mut self) {
        let mut status = 0;

        if !self.handle.is_null() {
            unsafe {
                cfitsio::ffclos(self.handle, &mut status);
            }
            self.handle = std::ptr::null_mut();
        }

        // This shouldn't happen -- if you bothered to allocate a buffer, you
        // probably want to pull it out and do something with it. But, who are
        // we to say.
        if !self.mem_buf.is_null() {
            unsafe {
                libc::free(self.mem_buf);
            }
            self.mem_buf = std::ptr::null_mut();
        }
    }
}
