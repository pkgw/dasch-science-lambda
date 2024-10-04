# `fitswcs-sys`

This is a lame "sys" crate providing just enough bindings to [cfitsio] and
[wcslib] to meet the needs of the DASCH science data Lambda.

[cfitsio]: https://heasarc.gsfc.nasa.gov/fitsio/
[wcslib]: https://www.atnf.csiro.au/people/mcalabre/WCS/

The cfitsio build infrastructure is ripped off from [rust-fitsio]. Its license
is MIT/Apache-2, so that's what we adopt. The wcslib build script then derives
from that.

[rust-fitsio]: https://github.com/simonrw/rust-fitsio/

In retrospect, although the wcslib build can integrate with cfitsio, I think
that the actual static library has no dependencies on it, so these could be
separate "sys" crates. Whatever.
