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
use flate2::read::GzDecoder;
use lambda_http::Error;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use tokio::io::AsyncBufReadExt;

use crate::{
    mosaics::{load_b01_header, wcslib_solnum, PIXELS_PER_MM, PLATE_SCALE_BY_SERIES},
    wcs::WcsCollection,
};

const BUCKET: &str = "dasch-prod-user";

/// Sync with `json-schemas/queryexps_request.json`, which then needs to be
/// synced into S3.
#[derive(Deserialize)]
pub struct Request {
    pub ra_deg: f64,
    pub dec_deg: f64,
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
    #[serde(default, with = "serde_bytes")]
    // should be Option<>, but not sure how to nest the custom deserializer
    b01_header_gz: Vec<u8>,
    n_solutions: Option<usize>,
    rotation_delta: Option<isize>,
    exposures: Vec<Option<PlatesExposureResult>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlatesExposureResult {
    center_source: Option<String>,
    //date_acc_days: Option<f64>,
    //date_source: Option<String>,
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
    mos_num: i8,
    scan_num: i8,
}

#[derive(Debug)]
struct SolExp {
    sol_num: i8,
    exp_num: i8,
}

pub async fn handler(
    req: Option<Value>,
    dc: &aws_sdk_dynamodb::Client,
    s3: &aws_sdk_s3::Client,
    binning: &crate::gscbin::GscBinning,
) -> Result<Value, Error> {
    Ok(serde_json::to_value(
        implementation(
            serde_json::from_value(req.ok_or_else(|| -> Error { "no request payload".into() })?)?,
            dc,
            s3,
            binning,
        )
        .await?,
    )?)
}

pub async fn implementation(
    request: Request,
    dc: &aws_sdk_dynamodb::Client,
    s3: &aws_sdk_s3::Client,
    binning: &crate::gscbin::GscBinning,
) -> Result<Vec<String>, Error> {
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
    let s3_key = format!("dasch-dr7-coverage-bins/{}.csv", total_bin);

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

    println!("Coarse bin query got {} plates", candidates.len());

    // Get the detailed plate information. DynamoDB provides a batch_get_item
    // endpoint that manages to meet our needs, but it's annoying to use.

    let mut rows = vec!["series,\
        platenum,\
        scannum,\
        mosnum,\
        expnum,\
        solnum,\
        class,\
        ra,\
        dec,\
        exptime,\
        expdate,\
        epoch,\
        wcssource,\
        scandate,\
        mosdate,\
        centerdist,\
        edgedist"
        .to_owned()];

    let base_builder = aws_sdk_dynamodb::types::KeysAndAttributes::builder().projection_expression(
        "astrometry.b01HeaderGz,\
        astrometry.exposures,\
        astrometry.nSolutions,\
        astrometry.rotationDelta,\
        mosaic.b01Height,\
        mosaic.b01Width,\
        mosaic.creationDate,\
        mosaic.mosNum,\
        mosaic.scanNum,\
        plateId,\
        plateNumber,\
        series",
    );

    let table_name = format!("dasch-{}-dr7-plates", super::ENVIRONMENT);
    let mut unprocessed_keys: Option<HashMap<String, aws_sdk_dynamodb::types::KeysAndAttributes>> =
        None;
    let mut remaining_ids = candidates.keys();
    const MAX_PER_BATCH: usize = 100;
    let mut all_submitted = false;

    loop {
        // Continue from previous iteration, maybe. We can pass
        // `unprocessed_keys` straight to `set_request_items()`, but once we do
        // that there's no way to mutate it, so we can't "top off" our request.
        // The type structure of this API is pretty gnarly.

        let mut keys = unprocessed_keys
            .take()
            .and_then(|mut t| t.remove(&table_name))
            .map(|kv| kv.keys)
            .unwrap_or_default();

        // Top up our request to the maximum count. (Amazon says that if your
        // requests don't get fully filled, you should back off the size of your
        // batch requests. I don't think that will be a problem for us?)

        while !all_submitted && keys.len() < MAX_PER_BATCH {
            if let Some(pid) = remaining_ids.next() {
                // I see no better way to do this ...
                let mut k = HashMap::with_capacity(1);
                k.insert("plateId".to_owned(), AttributeValue::S(pid.to_owned()));
                keys.push(k);
            } else {
                all_submitted = true;
                break;
            }
        }

        if all_submitted && keys.is_empty() {
            break;
        }

        // Ready to submit

        let resp = dc
            .batch_get_item()
            .request_items(
                &table_name,
                base_builder.clone().set_keys(Some(keys)).build()?,
            )
            .send()
            .await?;

        let mut chunk: Vec<PlatesResult> = serde_dynamo::from_items(
            resp.responses
                .unwrap()
                .remove(&table_name)
                .unwrap_or_default(),
        )?;

        for item in chunk.drain(..) {
            // "Impossible" to get a plate ID that's not in our candidates list:
            let solexps = candidates.get(&item.plate_id).unwrap();
            process_one(&request, item, &solexps[..], &mut rows);
        }

        unprocessed_keys = resp.unprocessed_keys;
    }

    Ok(rows)
}

fn process_one(req: &Request, plate: PlatesResult, solexps: &[SolExp], rows: &mut Vec<String>) {
    // First order of business is to prepare to construct a WCS object for every
    // solexp that we need to check. Even if we have some precise astrometric
    // solutions, we might *also* have catalog-only exposures for which we need
    // to construct approximate WCS, so we need to be prepared to handle either.

    let mos = plate.mosaic.as_ref();
    let astrom = plate.astrometry.as_ref();

    let mut solved_wcs = astrom.map(|a| &a.b01_header_gz).and_then(|gzh| {
        if gzh.is_empty() {
            None
        } else {
            load_b01_header(GzDecoder::new(&gzh[..])).ok()
        }
    });

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
        let mut this_wcslib_solnum = 0;
        let mut this_wcs = None;
        let mut this_width = width;
        let mut this_height = height;
        let mut this_exp = None;

        if solexp.sol_num >= 0 && (solexp.sol_num as usize) < n_solutions {
            // Yay, we have real WCS for this one. We can only get here if
            // solved_wcs is Some, and we just checked the sol_num is valid.
            this_wcs = Some(solved_wcs.as_mut().unwrap());
            this_wcslib_solnum = wcslib_solnum(solexp.sol_num as usize, n_solutions).unwrap();
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
                                // We found the exposure, and we can and should use it for
                                // WCS.

                                let ps = pixel_scale.unwrap(); // checked above
                                let crpix = 0.5 * (naxis_for_approx as f64 + 1.);
                                maybe_temp_wcs =
                                    Some(WcsCollection::new_tan(ra, dec, crpix, crpix, ps));
                                this_wcs = maybe_temp_wcs.as_mut();
                                this_wcslib_solnum = 0;
                                this_width = naxis_for_approx;
                                this_height = naxis_for_approx;
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

        let mut this_wcs = match this_wcs.map(|w| w.get(this_wcslib_solnum)) {
            Some(Ok(w)) => w,
            _ => continue,
        };

        // Finally we can check whether this plate+solexp actually intersects
        // with the point of interest!

        let (x, y) = match this_wcs.world_to_pixel_scalar(req.ra_deg, req.dec_deg) {
            Ok(c) => c,
            Err(_) => continue,
        };

        if x < -0.5 || x > (this_width as f64 - 0.5) || y < -0.5 || y > (this_height as f64 - 0.5) {
            continue;
        }

        // The point of interest actually intersects the plate! Gather the data
        // to report it.

        let scan_num = mos.map(|m| m.scan_num).unwrap_or(-1);
        let mos_num = mos.map(|m| m.mos_num).unwrap_or(-1);
        let plate_class = "";

        let center_x = 0.5 * (this_width as f64 - 1.);
        let center_y = 0.5 * (this_height as f64 - 1.);
        let center_text = this_wcs
            .pixel_to_world_scalar(center_x, center_y)
            .map(|(r, d)| format!("{:.6},{:.6}", r, d))
            .unwrap_or_else(|_e| ",".to_owned());

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
        let expdate_text = this_exp
            .and_then(|e| e.midpoint_date.as_ref())
            .map(|s| s.as_ref())
            .unwrap_or("");
        let epoch = 2000.0;
        let wcs_source = this_exp
            .and_then(|e| e.center_source.as_ref())
            .map(|s| s.to_lowercase())
            .unwrap_or("".to_owned());
        let scandate = ""; // TODO: need to import this into the DB
        let mosdate = mos.map(|m| m.creation_date.as_ref()).unwrap_or("");

        let row = format!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{:.1},{:.1}",
            plate.series,
            plate.plate_number,
            scan_num,
            mos_num,
            solexp.exp_num,
            solexp.sol_num,
            plate_class,
            center_text, // 2 columns
            exptime_text,
            expdate_text,
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
