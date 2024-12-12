//! Some stuff about plates, exposures, mosaics, etc, as well as the
//! `mosaic_package` endpoint.

use anyhow::{bail, Result};
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_s3::presigning::PresigningConfig;
use lambda_http::Error;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashMap,
    io::{prelude::*, ErrorKind},
};

use crate::{
    dynamo_types::plates_mosaic_package::*, http_types::Response, simple_response,
    wcs::WcsCollection, PLATES_TABLE_NAME, USER_BUCKET,
};

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

/// Mapping from numerical series ID to series identifier. This captures the
/// DASCH SQL table `photometry.photseries`. Valid IDs range from 1 to 99,
/// inclusive.
pub const PLATE_SERIES_BY_ID: &[&str] = &[
    "INVALID", // 0
    "a",       // 1
    "ac",      // 2
    "adh",     // 3
    "al",      // 4
    "am",      // 5
    "b",       // 6
    "bm",      // 7
    "br",      // 8
    "c",       // 9
    "d",       // 10
    "dnb",     // 11
    "dnr",     // 12
    "dny",     // 13
    "dsb",     // 14
    "dsr",     // 15
    "dsy",     // 16
    "fa",      // 17
    "h",       // 18
    "i",       // 19
    "ir",      // 20
    "j",       // 21
    "ma",      // 22
    "mb",      // 23
    "mc",      // 24
    "md",      // 25
    "meteor",  // 26
    "mf",      // 27
    "na",      // 28
    "r",       // 29
    "rb",      // 30
    "rh",      // 31
    "rl",      // 32
    "sb",      // 33
    "sh",      // 34
    "x",       // 35
    "ctio",    // 36
    "ab",      // 37
    "acmisc",  // 38
    "aco",     // 39
    "ai",      // 40
    "ak",      // 41
    "an",      // 42
    "ax",      // 43
    "ay",      // 44
    "bc",      // 45
    "bi",      // 46
    "bo",      // 47
    "ca",      // 48
    "darnor",  // 49
    "darsou",  // 50
    "ee",      // 51
    "er",      // 52
    "hsl",     // 53
    "jdar",    // 54
    "misc",    // 55
    "m",       // 56
    "n",       // 57
    "nviews",  // 58
    "oa",      // 59
    "o",       // 60
    "p",       // 61
    "qa",      // 62
    "qb",      // 63
    "q",       // 64
    "ra",      // 65
    "ro",      // 66
    "rp",      // 67
    "s",       // 68
    "sp",      // 69
    "sq",      // 70
    "t",       // 71
    "vq",      // 72
    "wa",      // 73
    "w",       // 74
    "y",       // 75
    "z",       // 76
    "solar",   // 77
    "ww",      // 78
    "ayroe",   // 79
    "yb",      // 80
    "pz",      // 81
    "me",      // 82
    "pas",     // 83
    "hale",    // 84
    "lwla",    // 85
    "ka",      // 86
    "kb",      // 87
    "kc",      // 88
    "kd",      // 89
    "ke",      // 90
    "kf",      // 91
    "kg",      // 92
    "kge",     // 93
    "kh",      // 94
    "ad",      // 95
    "e",       // 96
    "u",       // 97
    "v",       // 98
    "poss",    // 99
];

// An inversion of PLATE_SERIES_BY_ID
pub static PLATE_ID_BY_SERIES: Lazy<HashMap<String, u8>> = Lazy::new(|| {
    PLATE_SERIES_BY_ID[1..]
        .iter()
        .enumerate()
        .map(|t| ((**t.1).to_owned(), (t.0 + 1) as u8))
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
pub fn load_b01_header<R: Read>(mut src: R) -> Result<WcsCollection, Error> {
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

    Ok(unsafe { WcsCollection::new_raw(header.as_ptr() as *const _, n_rec) }?)
}

/// DASCH WCS headers are constructed as follows: if there's only one solution,
/// it appears with the ' ' "tag", which is index 0 according to WCS lib. If
/// there is more than one solution, the ' ' tag always has the *most recent*
/// solution, and solutions are accumulated under tags "A", "B", "C", etc. So if
/// we want solnum 0 in a 1-solution header, we look at tag 0; otherwise we look
/// at tag "A". The most recent solution appears twice: under the " " tag, *and*
/// under the appropriate letter.
pub fn wcslib_solnum(solnum: usize, n_solutions: usize) -> Result<usize> {
    if solnum >= n_solutions {
        bail!(
            "illegal WCS solution number {} (0-based); there are only {}",
            solnum,
            n_solutions
        );
    }

    Ok(if n_solutions == 1 {
        0
    } else {
        solnum + 1 // 1 <=> "A", 2 <=> "B", etc
    })
}

// The mosaic_package endpoint

#[derive(Deserialize)]
struct MosaicPackageRequest {
    plate_id: String,
    binning: u8,
}

#[derive(Serialize)]
struct MosaicPackageResponse {
    base_fits_url: String,
    metadata: PlatesResult,
}

pub async fn handle_mosaic_package(
    req: Option<Value>,
    dc: &aws_sdk_dynamodb::Client,
    s3: &aws_sdk_s3::Client,
    pc: &PresigningConfig,
) -> Result<Response, Error> {
    Ok(implement_mosaic_package(
        serde_json::from_value(req.ok_or_else(|| -> Error { "no request payload".into() })?)?,
        dc,
        s3,
        pc,
    )
    .await?)
}

async fn implement_mosaic_package(
    request: MosaicPackageRequest,
    dc: &aws_sdk_dynamodb::Client,
    s3: &aws_sdk_s3::Client,
    pc: &PresigningConfig,
) -> Result<Response, Error> {
    // Early validation

    let is_bin01 = match request.binning {
        1 => true,
        16 => false,
        _ => {
            return Err("illegal binning parameter".into());
        }
    };

    // Get the target plate info.

    let result = dc
        .get_item()
        .table_name(PLATES_TABLE_NAME)
        .key("plateId", AttributeValue::S(request.plate_id.clone()))
        .projection_expression(PROJECTION_EXPRESSION)
        .send()
        .await?;

    let item = result
        .item
        .ok_or_else(|| -> Error { format!("no such plate_id `{}`", request.plate_id).into() })?;

    let item: PlatesResult = serde_dynamo::from_item(item)?;

    // With that, getting the mosaic is pretty easy.

    let mos_data = item.mosaic.as_ref().ok_or_else(|| -> Error {
        format!(
            "plate `{}` has no registered FITS mosaic information (never scanned?)",
            request.plate_id
        )
        .into()
    })?;

    let bin = if is_bin01 { "01" } else { "16" };
    let tnx = if is_bin01 { "tnx" } else { "" };
    let key = mos_data
        .s3_key_template
        .replace("{bin}", bin)
        .replace("{tnx}", tnx);
    let presign = s3
        .get_object()
        .bucket(USER_BUCKET)
        .key(&key)
        .presigned(pc.clone())
        .await?;

    // All done!

    simple_response(&MosaicPackageResponse {
        base_fits_url: presign.uri().to_string(),
        metadata: item,
    })
}
