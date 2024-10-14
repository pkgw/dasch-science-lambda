// TODO? we should probably move to serde-dynamo for strongly-typed handling

use aws_sdk_dynamodb::types::AttributeValue;
use lambda_http::Error;
use serde::Deserialize;
use serde_json::Value;

use crate::gscbin::D2R;
use crate::refnums::refnum_to_text;

const EXTERNAL_COLUMNS: &[&str] = &[
    "ref_text",
    "ref_number",
    "gscBinIndex",
    "raDeg",
    "decDeg",
    "draAsec",
    "ddecAsec",
    "posEpoch",
    "pmRaMasyr",
    "pmDecMasyr",
    "uPMRaMasyr",
    "uPMDecMasyr",
    "stdmag",
    "color",
    "vFlag",
    "magFlag",
    "class",
];

const INTERNAL_COLUMNS: &[&str] = &[
    "refText",
    "refNumber",
    "gscBinIndex",
    "ra",
    "dec",
    "draAsec",
    "ddecAsec",
    "posEpoch",
    "raPM",
    "decPM",
    "raSigmaPM",
    "decSigmaPM",
    "stdmag",
    "color",
    "vFlag",
    "magFlag",
    "class",
];

/// Sync with `json-schemas/querycat_request.json`, which then needs to be
/// synced into S3.
#[derive(Deserialize)]
pub struct Request {
    refcat: String,
    ra_deg: f64,
    dec_deg: f64,
    radius_arcsec: f64,
}

pub async fn handler(
    req: Option<Value>,
    dc: &aws_sdk_dynamodb::Client,
    binning: &crate::gscbin::GscBinning,
) -> Result<Value, Error> {
    Ok(serde_json::to_value(
        implementation(
            serde_json::from_value(req.ok_or_else(|| -> Error { "no request payload".into() })?)?,
            dc,
            binning,
        )
        .await?,
    )?)
}

pub async fn implementation(
    request: Request,
    dc: &aws_sdk_dynamodb::Client,
    binning: &crate::gscbin::GscBinning,
) -> Result<Vec<String>, Error> {
    let mut lines = Vec::new();

    // Validation

    match request.refcat.as_ref() {
        "apass" | "atlas" => {}
        _ => {
            return Err("illegal refcat parameter".into());
        }
    }

    // Use this logic style to catch NaNs:
    if !(request.ra_deg >= 0. && request.ra_deg <= 360.) {
        return Err("illegal ra_deg parameter".into());
    }

    if !(request.dec_deg >= -90. && request.dec_deg <= 90.) {
        return Err("illegal dec_deg parameter".into());
    }

    if !(request.radius_arcsec > 0. && request.radius_arcsec < 3600.) {
        return Err("illegal radius_arcsec parameter".into());
    }

    let cat_table = format!("dasch-{}-dr7-refcat-{}", super::ENVIRONMENT, request.refcat);
    let radius_deg = request.radius_arcsec / 3600.0;
    let min_dec = f64::max(request.dec_deg - radius_deg, -90.0);
    let max_dec = f64::min(request.dec_deg + radius_deg, 90.0);
    let bin0 = binning.get_dec_bin(min_dec);
    let bin1 = binning.get_dec_bin(max_dec);

    let cos_dec = f64::min(f64::cos(min_dec * D2R), f64::cos(max_dec * D2R));

    let (ra_bound_1, ra_bound_2) = if cos_dec <= 0. {
        ((0., 360.0), None)
    } else {
        let search_radius_ra = radius_deg / cos_dec;
        let min_ra = request.ra_deg - search_radius_ra;
        let max_ra = request.ra_deg + search_radius_ra;

        if min_ra <= 0. && max_ra >= 360. {
            // We cover all RA's, which might happen with a reasonable radius if
            // we're right at the poles. This is OK.
            ((0., 360.0), None)
        } else if min_ra < 0. {
            // We need to break our search into two RA chunks:
            // (0, naive-max) and (wrapped-naive-min, 360)
            ((0., max_ra), Some((min_ra + 360., 360.)))
        } else if max_ra > 360. {
            // Analogous to the previous case
            ((min_ra, 360.), Some((0., max_ra - 360.)))
        } else {
            ((min_ra, max_ra), None)
        }
    };

    lines.push(EXTERNAL_COLUMNS.join(","));

    for ibin in bin0..=bin1 {
        lines = read_dec_bin(
            lines,
            &cat_table,
            ibin,
            ra_bound_1.0,
            ra_bound_1.1,
            &request,
            dc,
            binning,
        )
        .await?;

        if let Some(b2) = ra_bound_2 {
            lines =
                read_dec_bin(lines, &cat_table, ibin, b2.0, b2.1, &request, dc, binning).await?;
        }
    }

    Ok(lines)
}

async fn read_dec_bin(
    mut lines: Vec<String>,
    cat_table: &str,
    dec_bin: usize,
    box_ra_min: f64,
    box_ra_max: f64,
    request: &Request,
    dc: &aws_sdk_dynamodb::Client,
    binning: &crate::gscbin::GscBinning,
) -> Result<Vec<String>, Error> {
    let tbin0 = binning.get_total_bin(dec_bin, box_ra_min);
    let tbin1 = binning.get_total_bin(dec_bin, box_ra_max);
    let mut cells = Vec::new();

    let radius_deg = request.radius_arcsec / 3600.0;

    // For computing RA separations below -- the "effective" RA of the search
    // center might need to vary if we've partitioned the search into two
    // sub-boxes in RA.
    let eff_search_ra = request.ra_deg
        + if request.ra_deg < box_ra_min {
            // Our box has RA ~ 359 while the search center has RA ~ 1.
            360.
        } else if request.ra_deg > box_ra_max {
            // Our box has RA ~ 1 while the search center has RA ~ 359.
            -360.
        } else {
            0.
        };

    for itbin in tbin0..=tbin1 {
        let mut stream = dc
            .query()
            .table_name(cat_table)
            .expression_attribute_names("#p", "gscBinIndex")
            .expression_attribute_values(":bin", AttributeValue::N(itbin.to_string()))
            .key_condition_expression("#p = :bin")
            .into_paginator()
            .items()
            .send();

        while let Some(item) = stream.next().await {
            let item = item?;
            cells.clear();

            let ra_deg = item
                .get("ra")
                .and_then(|av| av.as_n().ok())
                .and_then(|text| text.parse::<f64>().ok());

            let dec_deg = item
                .get("dec")
                .and_then(|av| av.as_n().ok())
                .and_then(|text| text.parse::<f64>().ok());

            let (ra_deg, dec_deg) = match (ra_deg, dec_deg) {
                (Some(r), Some(d)) => (r, d),
                _ => continue,
            };

            // Now we can evaluate if this source actually matches the
            // positional search. Note that we're actually evaluating a box, not
            // a conical radius.
            //
            // Unlike "classical" querycat, we ignore the uncertainty introduced
            // by the proper motion term.

            // If the limiting values go unphysical, no problem.
            if dec_deg < request.dec_deg - radius_deg || dec_deg > request.dec_deg + radius_deg {
                continue;
            }

            let factor = (D2R * dec_deg).cos();

            // If the search box spans the RA = 0 = 360 line, this function will
            // be called twice to handle the wraparound, so we can also be
            // cavalier with the limits here.

            let (min_ra, max_ra) = if factor <= 0. {
                (0., 360.)
            } else {
                (
                    eff_search_ra - radius_deg / factor,
                    eff_search_ra + radius_deg / factor,
                )
            };

            if ra_deg < min_ra || ra_deg > max_ra {
                continue;
            }

            let mut delta_ra = request.ra_deg - ra_deg;

            if delta_ra < -180. {
                delta_ra += 360.;
            } else if delta_ra > 180. {
                delta_ra -= 360.;
            }

            let factor = (D2R * 0.5 * (dec_deg + request.dec_deg)).cos();

            let sep = (
                3600. * factor * delta_ra,
                3600. * (request.dec_deg - dec_deg),
            );

            for col in INTERNAL_COLUMNS {
                match *col {
                    "refText" => {
                        let val = item
                            .get("refNumber")
                            .and_then(|av| av.as_n().ok())
                            .and_then(|text| text.parse::<u64>().ok())
                            .map(|n| refnum_to_text(n))
                            .unwrap_or_else(|| "UNDEFINED".to_owned());
                        cells.push(val);
                    }

                    "draAsec" => {
                        cells.push(format!("{}", sep.0));
                    }

                    "ddecAsec" => {
                        cells.push(format!("{}", sep.1));
                    }

                    "posEpoch" => {
                        cells.push("2000.000".to_string());
                    }

                    _ => match item.get(*col) {
                        None => {
                            cells.push("".to_string());
                        }

                        Some(val) => match val {
                            AttributeValue::N(s) => cells.push(s.clone()),
                            AttributeValue::S(s) => cells.push(s.clone()),
                            _ => cells.push("".to_string()),
                        },
                    },
                }
            }

            lines.push(cells.join(","));
        }
    }

    Ok(lines)
}
