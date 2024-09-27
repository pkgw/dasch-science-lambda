use aws_sdk_dynamodb::types::AttributeValue;
use lambda_runtime::{Error, LambdaEvent};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::gscbin::D2R;

#[derive(Deserialize)]
pub struct Request {
    refcat: String,
    ra_deg: f64,
    dec_deg: f64,
    radius_arcsec: f64,
}

pub async fn handle_querycat(
    event: LambdaEvent<Request>,
    dc: &aws_sdk_dynamodb::Client,
    binning: &crate::gscbin::GscBinning,
) -> Result<Value, Error> {
    let (event, context) = event.into_parts();
    let cfg = context.env_config;
    println!("*** fn name={} version={}", cfg.function_name, cfg.version);

    let radius_deg = event.radius_arcsec / 3600.0;
    let min_dec = f64::max(event.dec_deg - radius_deg, -90.0);
    let max_dec = f64::min(event.dec_deg + radius_deg, 90.0);
    let bin0 = binning.get_dec_bin(min_dec);
    let bin1 = binning.get_dec_bin(max_dec);
    println!("+++ bins: {bin0} {bin1}");

    let cos_dec = f64::min(f64::cos(min_dec * D2R), f64::cos(max_dec * D2R));

    let (ra_bound_1, ra_bound_2) = if cos_dec <= 0. {
        ((0., 360.0), None)
    } else {
        let search_radius_ra = radius_deg / cos_dec;
        let min_ra = event.ra_deg - search_radius_ra;
        let max_ra = event.ra_deg + search_radius_ra;

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

    println!("+++ bounds: {:?}, {:?}", ra_bound_1, ra_bound_2);

    for ibin in bin0..=bin1 {
        read_dec_bin(ibin, ra_bound_1.0, ra_bound_1.1, dc, binning).await?;

        if let Some(b2) = ra_bound_2 {
            read_dec_bin(ibin, b2.0, b2.1, dc, binning).await?;
        }
    }

    Ok(json!({ "message": format!("Hello, {}!", event.refcat) }))
}

async fn read_dec_bin(
    dec_bin: usize,
    ra_min: f64,
    ra_max: f64,
    dc: &aws_sdk_dynamodb::Client,
    binning: &crate::gscbin::GscBinning,
) -> Result<(), Error> {
    let tbin0 = binning.get_total_bin(dec_bin, ra_min);
    let tbin1 = binning.get_total_bin(dec_bin, ra_max);

    for itbin in tbin0..=tbin1 {
        println!("+++ query: {dec_bin} {tbin0} {tbin1} {itbin}");

        let mut stream = dc
            .query()
            .table_name("dasch_dev_refcat_apass")
            .expression_attribute_names("#p", "gsc_bin64_chunk") // todo: => index
            .expression_attribute_values(":bin", AttributeValue::N(itbin.to_string()))
            .key_condition_expression("#p = :bin")
            .into_paginator()
            .items()
            .send();

        while let Some(item) = stream.next().await {
            println!("***   {:?}", item);
        }
    }

    Ok(())
}
