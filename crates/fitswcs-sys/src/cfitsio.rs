//! The small subset of CFITSIO's API that we need.

use libc::{c_char, c_int, c_long, c_longlong, c_short, c_void, size_t};

pub type FitsHandle = *mut c_void;

pub const READONLY: c_int = 0;
pub const FILE_NOT_OPENED: c_int = 104; // "could not open the named file"
pub const READ_ERROR: c_int = 108; // "error reading from FITS file"
pub const TSTRING: c_int = 16;
pub const TSHORT: c_int = 21;
pub const TDOUBLE: c_int = 82;

extern "C" {
    /// Register a new I/O driver with the library.
    pub fn fits_register_driver(
        prefix: *const c_char,
        init: *const c_void,
        fitsshutdown: *const c_void,
        setoptions: *const c_void,
        getoptions: *const c_void,
        getversion: *const c_void,
        checkfile: *const c_void,
        fitsopen: *const c_void,
        fitscreate: *const c_void,
        fitstruncate: *const c_void,
        fitsclose: *const c_void,
        fremove: *const c_void,
        size: *const c_void,
        flush: *const c_void,
        seek: *const c_void,
        fitsread: *const c_void,
        fitswrite: *const c_void,
    ) -> c_int;

    /// Open a FITS file.
    pub fn ffopen(
        handle: *mut FitsHandle,
        filename: *const c_char,
        iomode: c_int,
        status: *mut c_int,
    ) -> c_int;

    /// Create a new FITS file in memory. We need to use this API to have access
    /// to the buffer.
    pub fn ffimem(
        handle: *mut FitsHandle,
        memptr: *mut *mut c_void,
        memsize: *mut size_t,
        deltasize: size_t,
        realloc: *const c_void,
        status: *mut c_int,
    ) -> c_int;

    /// Move to absolute HDU number. HDU numbers are 1-based.
    pub fn ffmahd(
        handle: FitsHandle,
        hdunum: c_int,
        exttype: *mut c_int,
        status: *mut c_int,
    ) -> c_int;

    /// Get image number of dimensions
    pub fn ffgidm(handle: FitsHandle, naxis: *mut c_int, status: *mut c_int) -> c_int;

    /// Get image size, longlong mode
    pub fn ffgiszll(
        handle: FitsHandle,
        nlen: c_int,
        naxes: *mut c_longlong,
        status: *mut c_int,
    ) -> c_int;

    /// Write a basic image header, longlong mode
    pub fn ffphpsll(
        handle: FitsHandle,
        bitpix: c_int,
        naxis: c_int,
        naxes: *const c_longlong,
        status: *mut c_int,
    ) -> c_int;

    /// Update a HDU header
    pub fn ffuky(
        handle: FitsHandle,
        datatype: c_int,
        keyname: *const c_char,
        value: *const c_void,
        comment: *const c_char,
        status: *mut c_int,
    ) -> c_int;

    /// Extract HDU headers as string(s), converting as needed if the
    /// HDU is for a compressed image.
    pub fn ffcnvthdr2str(
        handle: FitsHandle,
        exclude_comm: c_int,        // skip comment-type headers
        exclist: *mut *mut c_char,  // list of keywords to exclude from result
        nexc: c_int,                // number of items in `exclist`
        header: *mut *const c_char, // output: the concatenated header
        nkeys: *mut c_int,          // output: number of header keys
        status: *mut c_int,
    ) -> c_int;

    /// Read pixel values, short-int format.
    pub fn ffgpvi(
        handle: FitsHandle,
        group: c_long,
        firstelem: c_longlong,
        nelem: c_longlong,
        nulval: c_short,
        array: *mut c_short,
        anynul: *mut c_int,
        status: *mut c_int,
    ) -> c_int;

    /// Write pixel values, longlong indexing.
    pub fn ffppxll(
        handle: FitsHandle,
        datatype: c_int,
        fpixel: *const c_longlong,
        nelem: c_longlong,
        array: *const c_void,
        status: *mut c_int,
    ) -> c_int;

    /// Close a handle, freeing the structure if this is the
    /// last one referencing the given file.
    pub fn ffclos(handle: FitsHandle, status: *mut c_int) -> c_int;
}
