//! A lame "sys" crate providing just enough bindings to [cfitsio] and [wcslib] to
//! meet the needs of the DASCH science data Lambda.
//!
//! [cfitsio]: https://heasarc.gsfc.nasa.gov/fitsio/
//! [wcslib]: https://www.atnf.csiro.au/people/mcalabre/WCS/

pub mod cfitsio;
pub mod wcslib;
