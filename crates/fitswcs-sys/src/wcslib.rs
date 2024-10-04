//! The small subset of wcslib's API that we need.

use libc::{c_char, c_double, c_int, c_void};

pub type WcsPrm = *mut c_void;

pub const WCSHDR_ALL: c_int = 0xFFFFF;

extern "C" {
    /// Parse FITS headers for WCS.
    pub fn wcspih(
        header: *const c_char,
        nkeyrec: c_int,
        relax: c_int,
        ctrl: c_int,
        nreject: *mut c_int,
        nwcs: *mut c_int,
        wcs: *mut WcsPrm,
    ) -> c_int;

    /// World-to-pixel transformation
    pub fn wcss2p(
        wcs: WcsPrm,
        ncoord: c_int,
        nelem: c_int,
        world: *const c_double,
        phi: *mut c_double,
        theta: *mut c_double,
        imgcrd: *mut c_double,
        pixcrd: *mut c_double,
        stat: *mut c_int,
    ) -> c_int;

    /// Pixel-to-world transformation
    pub fn wcsp2s(
        wcs: WcsPrm,
        ncoord: c_int,
        nelem: c_int,
        pixcrd: *const c_double,
        imgcrd: *mut c_double,
        phi: *mut c_double,
        theta: *mut c_double,
        world: *mut c_double,
        stat: *mut c_int,
    ) -> c_int;

    /// Free a WCS structure.
    pub fn wcsfree(wcs: WcsPrm) -> c_int;
}
