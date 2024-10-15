//! Some stuff about plates, exposures, mosaics, etc.
//!
//! Ideally we'd centralize the DynamoDB serde types here, but I don't know if
//! there's a nice way to do that with projections, and it seems pretty helpful
//! to maintain those to keep data transfer sizes minimal.

use anyhow::Result;
use lambda_http::Error;
use once_cell::sync::Lazy;
use std::{
    collections::HashMap,
    io::{prelude::*, ErrorKind},
};

use crate::wcs::Wcs;

pub const PIXELS_PER_MM: f64 = 90.9090;

// These are from the DASCH SQL DB `scanner.series` table, looking at the
// non-NULL `fittedPlateScale` values when available, otherwise
// `nominalPlateScale`. Values are arcsec per millimeter.
pub static PLATE_SCALE_BY_SERIES: Lazy<HashMap<String, f64>> = Lazy::new(|| {
    [
        ("a", 59.57),
        ("ab", 590.), // nominal
        ("ac", 606.4),
        ("aco", 611.3),
        ("adh", 68.), // nominal
        ("ai", 1360.),
        ("ak", 614.5),
        ("al", 1200.), // nominal
        ("am", 610.8),
        ("an", 574.), // nominal
        ("ax", 695.7),
        ("ay", 694.2),
        ("b", 179.4),
        ("bi", 1446.),
        ("bm", 384.),
        ("bo", 800.), // nominal
        ("br", 204.),
        ("c", 52.56),
        ("ca", 596.),
        ("ctio", 18.),
        ("darnor", 890.), // nominal
        ("darsou", 890.), // nominal
        ("dnb", 577.3),
        ("dnr", 579.7),
        ("dny", 576.1),
        ("dsb", 574.5),
        ("dsr", 579.7),
        ("dsy", 581.8),
        ("ee", 330.),
        ("er", 390.), // nominal
        ("fa", 1298.),
        ("h", 59.6),
        ("hale", 11.06), // nominal
        ("i", 163.3),
        ("ir", 164.),
        ("j", 98.),     // nominal
        ("jdar", 560.), // nominal
        ("ka", 1200.),  // nominal
        ("kb", 1200.),  // nominal
        ("kc", 650.),   // nominal
        ("kd", 650.),   // nominal
        ("ke", 1160.),  // nominal
        ("kf", 1160.),  // nominal
        ("kg", 1160.),  // nominal
        ("kge", 1160.), // nominal
        ("kh", 1160.),  // nominal
        ("lwla", 36.687),
        ("ma", 93.7),
        ("mb", 390.),
        ("mc", 97.9),
        ("md", 193.),      // nominal
        ("me", 600.),      // nominal
        ("meteor", 1200.), // nominal
        ("mf", 167.3),
        ("na", 100.),
        ("pas", 95.64),
        ("poss", 67.19), // nominal
        ("pz", 1553.),
        ("r", 390.), // nominal
        ("rb", 395.5),
        ("rh", 391.3),
        ("rl", 290.), // nominal
        ("ro", 390.), // nominal
        ("s", 26.3),  // nominal
        ("sb", 26.),  // nominal
        ("sh", 26.),  // nominal
        ("x", 42.3),
        ("yb", 55.),
    ]
    .iter()
    .map(|t| (t.0.to_owned(), t.1))
    .collect()
});

/// The bin01 header is stored in the DynamoDB as bytes, which are gzipped text
/// of an ASCII FITS header file. This file consists of 80-character lines of
/// header text, separated by newlines, without a trailing newline.
///
/// As far as I can tell, the wcslib header parser will only handle data as they
/// are stored in FITS files: no newline separators allowed. So we need to munge
/// the data. We also need to give wcslib a count of headers.
///
/// We *also* need to hack the headers because wcslib only accepts our
/// distortion terms if the `CTYPEn` values end with `-TPV`; it seems that the
/// pipeline, which is based on wcstools/libwcs, generates non-standard headers.
pub fn load_b01_header<R: Read>(mut src: R) -> Result<Wcs, Error> {
    let mut header = Vec::new();
    let mut n_rec = 0;
    let mut buf = vec![0; 80];

    loop {
        // The final record does not have a newline character,
        // so we can't read in chunks of 81.

        if let Err(e) = src.read_exact(&mut buf[..]) {
            if e.kind() == ErrorKind::UnexpectedEof {
                break;
            } else {
                return Err(e.into());
            }
        }

        // TAN/TPV hack. With the rigid FITS keyword structure, we know exactly where to
        // look:
        if buf.starts_with(b"CTYPE") && buf[15..].starts_with(b"-TAN") {
            buf[15..19].clone_from_slice(b"-TPV");
        }

        header.append(&mut buf);
        n_rec += 1;
        buf.resize(80, 0); // the `append` truncates `buf`

        if let Err(e) = src.read_exact(&mut buf[..1]) {
            if e.kind() == ErrorKind::UnexpectedEof {
                break;
            } else {
                return Err(e.into());
            }
        }

        if buf[0] != b'\n' {
            return Err(format!(
                "malformatted ASCII-FITS header: expected newline, got {:x}",
                buf[0]
            )
            .into());
        }
    }

    Ok(unsafe { Wcs::new_raw(header.as_ptr() as *const _, n_rec) }?)
}
