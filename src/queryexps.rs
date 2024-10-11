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

use anyhow::Result;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_s3;
use base64::{engine::general_purpose::STANDARD, write::EncoderWriter};
use flate2::read::GzDecoder;
use lambda_runtime::{Error, LambdaEvent};
use once_cell::sync::Lazy;
use serde::Deserialize;
use std::{
    collections::HashMap,
    io::{prelude::*, ErrorKind},
};
use tokio::io::AsyncBufReadExt;

use crate::{fitsfile::FitsFile, wcs::Wcs};

const BUCKET: &str = "dasch-prod-user";

const PIXELS_PER_MM: f64 = 90.9090;

// These are from the DASCH SQL DB `scanner.series` table, looking at the
// non-NULL `fittedPlateScale` values when available, otherwise
// `nominalPlateScale`. Values are arcsec per millimeter.
static PLATE_SCALE_BY_SERIES: Lazy<HashMap<String, f64>> = Lazy::new(|| {
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
    plate_id: String,
    plate_number: usize,
    series: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlatesAstrometryResult {
    #[serde(with = "serde_bytes")]
    b01_header_gz: Option<Vec<u8>>,
    n_solutions: Option<usize>,
    rotation_delta: Option<isize>,
    exposures: Vec<Option<PlatesExposureResult>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlatesExposureResult {
    center_source: Option<String>,
    date_acc_days: Option<f64>,
    date_source: Option<String>,
    dec_deg: Option<f64>,
    dur_min: Option<f64>,
    midpoint_date: Option<String>,
    number: i8,
    ra_deg: Option<f64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlatesMosaicResult {
    b01_height: usize,
    b01_width: usize,
    creation_date: String,
    legacy_rotation: isize,
    mos_num: i8,
    scan_num: i8,
}

#[derive(Debug)]
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

    //println!("got {} plates", candidates.len());

    // Get the detailed plate information. DynamoDB provides a batch_get_item
    // endpoint that manages to meet our needs, but it's annoying to use.

    let mut rows = vec!["series\t\
        platenum\t\
        scannum\t\
        mosnum\t\
        expnum\t\
        solnum\t\
        class\t\
        ra\t\
        dec\t\
        exptime\t\
        jd\t\
        epoch\t\
        wcssource\t\
        scandate\t\
        mosdate\t\
        centerdist\t\
        edgedist"
        .to_owned()];

    loop {
        let mut keys = Vec::new();

        // XXXXX TEMP
        let mut k = HashMap::with_capacity(1);
        k.insert(
            "plateId".to_owned(),
            AttributeValue::S("am25350".to_owned()),
        );
        keys.push(k);
        let mut k = HashMap::with_capacity(1);
        k.insert("plateId".to_owned(), AttributeValue::S("b51503".to_owned()));
        keys.push(k);

        let keyattr = aws_sdk_dynamodb::types::KeysAndAttributes::builder()
            .set_keys(Some(keys))
            .projection_expression(
                "astrometry.b01HeaderGz,\
                astrometry.exposures,\
                astrometry.nSolutions,\
                astrometry.rotationDelta,\
                mosaic.b01Height,\
                mosaic.b01Width,\
                mosaic.creationDate,\
                mosaic.legacyRotation,\
                mosaic.mosNum,\
                mosaic.scanNum,\
                plateId,\
                plateNumber,\
                series",
            )
            .build()?;

        let resp = dc
            .batch_get_item()
            .request_items(format!("dasch-{}-dr7-plates", super::ENVIRONMENT), keyattr)
            .send()
            .await?;

        let mut chunk: Vec<PlatesResult> = serde_dynamo::from_items(
            resp.responses
                .unwrap()
                .remove("dasch-dev-dr7-plates")
                .unwrap(),
        )?;
        dbg!(resp.unprocessed_keys);

        for item in chunk.drain(..) {
            // "Impossible" to get a plate ID that's not in our candidates list:
            let solexps = candidates.get(&item.plate_id).unwrap();
            process_one(&request, item, &solexps[..], &mut rows);
        }

        break; // XXXXXXXXXXXXXXXXXXXXX
    }

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

    // Done

    Ok(rows)
}

fn process_one(req: &Request, plate: PlatesResult, solexps: &[SolExp], rows: &mut Vec<String>) {
    // First order of business is to prepare to construct a WCS object for every
    // solexp that we need to check. Even if we have some precise astrometric
    // solutions, we might *also* have catalog-only exposures for which we need
    // to construct approximate WCS, so we need to be prepared to handle either.

    let mos = plate.mosaic.as_ref();
    let astrom = plate.astrometry.as_ref();

    let mut solved_wcs = astrom
        .and_then(|a| a.b01_header_gz.as_ref())
        .and_then(|gzh| load_b01_header(GzDecoder::new(&gzh[..])).ok());

    let n_solutions = if solved_wcs.is_none() {
        0
    } else {
        astrom.and_then(|a| a.n_solutions).unwrap_or(0)
    };

    let (width, height) = if let Some(mosdata) = mos {
        // The astrometric solution that we're using may be based on a plate
        // image that has been rotated relative to the mosaic that's actually
        // "on file". If the rotation is 90 or 270 degrees, that means that we
        // need to swap the effective dimensions.
        let wh = (mosdata.b01_width, mosdata.b01_height);

        match astrom.and_then(|a| a.rotation_delta) {
            Some(-270) | Some(-90) | Some(90) | Some(270) => (wh.1, wh.0),
            _ => wh,
        }
    } else if plate.series == "a" {
        // No mosaic, so we have to guess the plate size. The legacy DASCH
        // pipeline assumes 10" for everything except the A series, for which it
        // assumes 17". We assume the long dimension and squareness, because
        // we're being optimistic and don't know the plate's orientation on the
        // sky.
        (39255, 39255) // 17 inches, 90.909 pixels per mm
    } else {
        (23091, 23091) // 10 inches, 90.909 pixels per mm
    };

    let naxis_for_approx = usize::max(width, height);

    // This is degrees per pixel:
    let pixel_scale = PLATE_SCALE_BY_SERIES
        .get(&plate.series)
        .map(|pl| pl / PIXELS_PER_MM / 3600.);

    // Finally we're ready to go

    for solexp in solexps {
        #[allow(unused_assignments)]
        let mut maybe_temp_wcs = None;
        let mut this_wcs = None;
        let mut this_width = width;
        let mut this_height = height;
        let mut this_exp = None;

        if solexp.sol_num >= 0 && (solexp.sol_num as usize) < n_solutions {
            // Yay, we have real WCS for this one. We can only get here if solved_wcs is Some.
            this_wcs = Some(solved_wcs.as_mut().unwrap());
        }

        // We want to find the exposure record of interest. The list of
        // exposures is sorted to match the full solutions, and so is *not*
        // in exposure order, and also contains null rows.

        if solexp.exp_num >= 0 {
            for maybe_exp in astrom.map(|a| &a.exposures[..]).unwrap_or(&[]) {
                if let Some(exp) = maybe_exp {
                    if exp.number != solexp.exp_num {
                        continue;
                    }

                    // We have a match!

                    this_exp = maybe_exp.as_ref();

                    // If we don't have a real WCS solution yet, we may be able
                    // to do an approximate test based on the coarse exposure
                    // data. This only works if we have a pixel scale and if the
                    // exposure has useful centering information.

                    if this_wcs.is_none() && !pixel_scale.is_none() {
                        // Every exposure of interest *should* have useful
                        // RA/Dec info since otherwise it shouldn't be in our
                        // bin list, but let's check.

                        if let (Some(ra), Some(dec)) = (exp.ra_deg, exp.dec_deg) {
                            // These are all placeholder values observed in the
                            // data. We should strip them out of the DynamoDB:

                            if ra != 999. && ra != -99. && dec != 99. && dec != -99. {
                                // We found the exposure, and we can use it for
                                // WCS. This is a dumb way to synthesize WCS,
                                // but here we are.

                                let ps = pixel_scale.unwrap(); // checked above

                                let make_wcs = || -> Result<Wcs> {
                                    let mut tmp_fits = FitsFile::create_mem()?;
                                    tmp_fits.write_square_image_header(naxis_for_approx as u64)?;
                                    tmp_fits.set_string_header("CTYPE1", "RA---TAN")?;
                                    tmp_fits.set_string_header("CTYPE2", "DEC--TAN")?;
                                    tmp_fits.set_f64_header("CRVAL1", ra)?;
                                    tmp_fits.set_f64_header("CRVAL2", dec)?;
                                    tmp_fits.set_f64_header(
                                        "CRPIX1",
                                        0.5 * (naxis_for_approx as f64 + 1.),
                                    )?; // 1-based pixel coords
                                    tmp_fits.set_f64_header(
                                        "CRPIX2",
                                        0.5 * (naxis_for_approx as f64 + 1.),
                                    )?;
                                    tmp_fits.set_f64_header("CD1_1", -ps)?;
                                    tmp_fits.set_f64_header("CD2_2", ps)?;
                                    Ok(tmp_fits.get_wcs()?)
                                };

                                if let Ok(wcs) = make_wcs() {
                                    // It worked!!!

                                    maybe_temp_wcs = Some(wcs);
                                    this_wcs = maybe_temp_wcs.as_mut();
                                    this_width = naxis_for_approx;
                                    this_height = naxis_for_approx;
                                }
                            }
                        }
                    }

                    // Regardless of how well that all went, we're done
                    // searching.
                    break;
                }
            }
        }

        // We tried our best. There *should* always be a WCS to use, but if not,
        // treat this plate+solexp as a non-match: ignore it.

        let this_wcs = match this_wcs {
            Some(w) => w,
            None => continue,
        };

        println!("got wcs for {}/{:?}", plate.plate_id, solexp);

        // Finally we can check whether this plate+solexp actually intersects
        // with the point of interest!

        let (x, y) = match this_wcs.world_to_pixel_scalar(req.ra_deg, req.dec_deg) {
            Ok(c) => c,
            Err(_) => continue,
        };

        if x < -0.5 || x > (this_width as f64 - 0.5) || y < -0.5 || y > (this_height as f64 - 0.5) {
            continue;
        }

        // The point of interest actually intersects the plate!

        let scan_num = mos.map(|m| m.scan_num).unwrap_or(-1);
        let mos_num = mos.map(|m| m.mos_num).unwrap_or(-1);
        let plate_class = "";

        let center_x = 0.5 * (this_width as f64 - 1.);
        let center_y = 0.5 * (this_height as f64 - 1.);
        let center_text = this_wcs
            .pixel_to_world_scalar(center_x, center_y)
            .map(|(r, d)| format!("{:.6}\t{:.6}", r, d))
            .unwrap_or_else(|_e| "\t".to_owned());

        // Distance between search point and plate center, in cm. This is
        // straightforward to calculate in pixel space, because pixels per cm is
        // a constant. NB: can't use hypot() here right now because it triggers
        // an undefined glibc symbol version in the Amazon OS image.
        let center_dist = f64::sqrt(f64::powi(x - center_x, 2) + f64::powi(y - center_y, 2))
            / (10. * PIXELS_PER_MM);

        // Distance between search point and closest plate edge, in cm. Really
        // what we mean here is the "mosaic edge".
        let edge_dist = f64::min(
            x + 0.5,
            f64::min(
                y + 0.5,
                f64::min(
                    this_width as f64 - (0.5 + x),
                    this_height as f64 - (0.5 + y),
                ),
            ),
        ) / (10. * PIXELS_PER_MM);

        let exptime_text = this_exp
            .and_then(|e| e.dur_min)
            .map(|d| format!("{:.2}", d))
            .unwrap_or_default();
        let jd = "";
        let epoch = 2000.0;
        let wcs_source = this_exp
            .and_then(|e| e.center_source.as_ref())
            .map(|s| s.as_ref())
            .unwrap_or("");
        let scandate = "";
        let mosdate = mos.map(|m| m.creation_date.as_ref()).unwrap_or("");

        let row = format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{:.1}\t{:.1}",
            plate.series,
            plate.plate_number,
            scan_num,
            mos_num,
            solexp.exp_num,
            solexp.sol_num,
            plate_class,
            center_text, // 2 columns
            exptime_text,
            jd,
            epoch,
            wcs_source,
            scandate,
            mosdate,
            center_dist,
            edge_dist,
        );
        rows.push(row);
    }
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
