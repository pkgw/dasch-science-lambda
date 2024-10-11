//! The exposure query API service.
//!
//! Given an RA/dec, query all known exposures that overlap it.
//!
//! This is somewhat redundant with the Starglass plate search API, but for
//! science users we need to provide much more detailed information. To
//! accomplish this, we need to go to the DynamoDB to pull out information about
//! each matching plate. At that point, the Starglass API isn't really providing
//! anything that we can't easily do ourselves. We reuse the same set of
//! sky-binned CSV files that that API uses to narrow down the list of plates to
//! search.

use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_s3;
use base64::{engine::general_purpose::STANDARD, write::EncoderWriter};
use flate2::read::GzDecoder;
use lambda_runtime::{Error, LambdaEvent};
use serde::Deserialize;
use std::{
    collections::HashMap,
    io::{prelude::*, ErrorKind},
};
use tokio::io::AsyncBufReadExt;

const BUCKET: &str = "dasch-prod-user";

use crate::wcs::Wcs;

#[derive(Deserialize)]
pub struct Request {
    ra_deg: f64,
    dec_deg: f64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlatesResult {
    astrometry: Option<PlatesAstrometryResult>,
    mosaic: Option<PlatesMosaicResult>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlatesAstrometryResult {
    #[serde(with = "serde_bytes")]
    b01_header_gz: Vec<u8>,
    n_solutions: usize,
    rotation_delta: isize,
    exposures: Vec<PlatesExposureResult>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlatesExposureResult {
    source: Option<String>,
    date_acc_days: Option<f64>,
    date_source: Option<String>,
    dec_deg: Option<f64>,
    dur_min: Option<f64>,
    midpoint_date: Option<String>,
    number: u8,
    ra_deg: Option<f64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlatesMosaicResult {
    b01_height: usize,
    b01_width: usize,
    creation_date: String,
    legacy_rotation: isize,
    mos_num: u8,
    scan_num: u8,
}

struct SolExp {
    sol_num: i8,
    exp_num: i8,
}

pub async fn handle_queryexps(
    event: LambdaEvent<Request>,
    dc: &aws_sdk_dynamodb::Client,
    s3: &aws_sdk_s3::Client,
    binning: &crate::gscbin::GscBinning,
) -> Result<Vec<String>, Error> {
    let (request, context) = event.into_parts();
    let cfg = context.env_config;
    println!("*** fn name={} version={}", cfg.function_name, cfg.version);

    // Early validation, with NaN-sensitive logic

    if !(request.ra_deg >= 0. && request.ra_deg <= 360.) {
        return Err("illegal ra_deg parameter".into());
    }

    if !(request.dec_deg >= -90. && request.dec_deg <= 90.) {
        return Err("illegal dec_deg parameter".into());
    }

    // Get the approximate list of plates from the coarse binning.

    let dec_bin = binning.get_dec_bin(request.dec_deg);
    let total_bin = binning.get_total_bin(dec_bin, request.ra_deg);
    let s3_key = format!("dasch-dr7-coverage-bins/test{}.csv", total_bin); // !!!!!!!

    let resp = s3.get_object().bucket(BUCKET).key(&s3_key).send().await?;
    let body = resp.body.into_async_read();
    let mut lines = body.lines();

    let mut candidates: HashMap<String, Vec<SolExp>> = HashMap::new();

    while let Some(line) = lines.next_line().await? {
        let mut pieces = line.split(',');
        let plateid = pieces.next();
        let sol_num = pieces.next();
        let exp_num = pieces.next();

        if exp_num.is_none() {
            continue;
        }

        let plateid = plateid.unwrap();

        let sol_num = match str::parse(sol_num.unwrap()) {
            Ok(n) => n,
            Err(_) => continue,
        };

        let exp_num = match str::parse(exp_num.unwrap()) {
            Ok(n) => n,
            Err(_) => continue,
        };

        let solexps = candidates.entry(plateid.to_owned()).or_default();
        solexps.push(SolExp { sol_num, exp_num });
    }

    println!("got {} plates", candidates.len());

    // Get the information we need about this plate and validate the basic request.

    //let plates_table = format!("dasch-{}-dr7-plates", super::ENVIRONMENT);
    //let result = dc
    //    .get_item()
    //    .table_name(plates_table)
    //    .key("plateId", AttributeValue::S(request.plate_id.clone()))
    //    .projection_expression(
    //        "astrometry.b01HeaderGz,\
    //        astrometry.nSolutions,\
    //        astrometry.rotationDelta,\
    //        mosaic.b01Height,\
    //        mosaic.b01Width,\
    //        mosaic.s3KeyTemplate",
    //    )
    //    .send()
    //    .await?;
    //let item = result
    //    .item
    //    .ok_or_else(|| -> Error { format!("no such plate_id `{}`", request.plate_id).into() })?;
    //let item: PlatesResult = serde_dynamo::from_item(item)?;
    //let mos_data = item.mosaic.ok_or_else(|| -> Error {
    //    format!(
    //        "plate `{}` has no registered FITS mosaic information (never scanned?)",
    //        request.plate_id
    //    )
    //    .into()
    //})?;
    //let astrom_data = item.astrometry.ok_or_else(|| -> Error {
    //    format!(
    //        "plate `{}` has no registered astrometric solutions",
    //        request.plate_id
    //    )
    //    .into()
    //})?;
    //if request.solution_number >= astrom_data.n_solutions {
    //    return Err(format!(
    //        "requested astrometric solution #{} (0-based) for plate `{}` but it only has {} solutions",
    //        request.solution_number,
    //        request.plate_id,
    //        astrom_data.n_solutions
    //    )
    //    .into());
    //}
    //if astrom_data.rotation_delta != 0 {
    //    return Err(format!(
    //        "XXX rotation_delta {} for plate `{}`",
    //        astrom_data.rotation_delta, request.plate_id,
    //    )
    //    .into());
    //}
    //if request.solution_number != 0 {
    //    return Err(format!(
    //        "XXX solnum {} for plate `{}`",
    //        request.solution_number, request.plate_id,
    //    )
    //    .into());
    //}

    let mut rows = Vec::new();

    // Done

    Ok(rows)
}

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
fn load_b01_header<R: Read>(mut src: R) -> Result<Wcs, Error> {
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
