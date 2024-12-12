//! The lightcurve retrieval API.
//!
//! The response type schema is in `json-schemas/generic_csv.json`, which
//! needs to be synced into S3 for the API documentation framework.

use aws_sdk_dynamodb::types::AttributeValue;
use binary_serde::{BinarySerde, Endianness};
use lambda_http::Error;
use serde::Deserialize;
use serde_json::Value;
use std::collections::{hash_map::Entry, HashMap};

use crate::{
    dynamo_types::refcat_lightcurve::*,
    http_types::Response,
    make_refcat_table_name,
    photdata::{get_limiting_records, LimitsPlateRecord, MagRecord, OutputRecord},
    simple_response, USER_BUCKET,
};

/// Sync with `json-schemas/lightcurve_request.json`, which then needs to be
/// synced into S3.
#[derive(Deserialize)]
struct Request {
    refcat: String,
    gsc_bin_index: u32,
    ref_number: u64,
}

pub async fn handler(
    req: Option<Value>,
    dc: &aws_sdk_dynamodb::Client,
    s3c: &aws_sdk_s3::Client,
    bin2: &crate::gscbin::GscBinning,
) -> Result<Response, Error> {
    Ok(implementation(
        serde_json::from_value(req.ok_or_else(|| -> Error { "no request payload".into() })?)?,
        dc,
        s3c,
        bin2,
    )
    .await?)
}

async fn implementation(
    request: Request,
    dc: &aws_sdk_dynamodb::Client,
    s3c: &aws_sdk_s3::Client,
    bin2: &crate::gscbin::GscBinning,
) -> Result<Response, Error> {
    // Initial validation

    match request.refcat.as_ref() {
        "apass" | "atlas" => {}
        _ => {
            return Err("illegal refcat parameter".into());
        }
    }

    // Fetch information about the source from the refcat

    let refcat_table = make_refcat_table_name(&request.refcat);

    let result = dc
        .get_item()
        .table_name(refcat_table)
        .key(
            "gscBinIndex",
            AttributeValue::N(request.gsc_bin_index.to_string()),
        )
        .key(
            "refNumber",
            AttributeValue::N(request.ref_number.to_string()),
        )
        .projection_expression(PROJECTION_EXPRESSION)
        .expression_attribute_names("#COUNT", "count")
        .expression_attribute_names("#DEC", "dec")
        .expression_attribute_names("#OFFSET", "offset")
        .send()
        .await?;

    let item = result.item.ok_or_else(|| -> Error {
        format!(
            "no such source #{} in refcat {} (GSC bin {})",
            request.ref_number, request.refcat, request.gsc_bin_index
        )
        .into()
    })?;

    let item: RefcatItem = serde_dynamo::from_item(item)?;

    // Fetch the detections (if there are any)

    let mut outputs = HashMap::new();

    if let Some(phot) = item.phot {
        let file_number = 1024 * (request.gsc_bin_index >> 10);
        let s = phot.offset;
        let e = s + phot.count * MagRecord::SERIALIZED_SIZE as u32 - 1;

        let s3_key = format!(
            "dasch-dr7-phot-{}/mags/{}.dat",
            &request.refcat, file_number
        );

        // TODO: do this record-at-a-time, with minimal buffering, instead of
        // accumulating these big dumb chunks.

        let result = s3c
            .get_object()
            .bucket(USER_BUCKET)
            .key(&s3_key)
            .range(format!("bytes={}-{}", s, e))
            .send()
            .await?
            .body
            .collect()
            .await?
            .to_vec();

        for c in result.chunks_exact(MagRecord::SERIALIZED_SIZE) {
            let rec = MagRecord::binary_deserialize(c, Endianness::Little).unwrap();

            // This test should always succeed, but be paranoid in case
            // something has gone wrong with the data management.
            if rec.ref_number == request.ref_number {
                outputs.insert(
                    (rec.series_id, rec.plate_number, rec.solution_number),
                    rec.into_output(0),
                );
            }
        }
    }

    // Fetch the upper-limit data. Fill in any apparent non-detections, as well
    // as the mosaic-number info, which is missing from the magfile data.

    let limbuf = get_limiting_records(&request.refcat, item.ra, item.dec, s3c, bin2).await?;

    for c in limbuf.chunks_exact(LimitsPlateRecord::SERIALIZED_SIZE) {
        let rec = LimitsPlateRecord::binary_deserialize(c, Endianness::Little).unwrap();
        let e = outputs.entry((
            rec.series_id as u8,
            rec.plate_number,
            rec.solution_number as u8,
        ));

        match e {
            Entry::Occupied(mut o) => {
                // This plate/solution has a detection. We need to fill in its
                // mosaic number.
                o.get_mut().mosaic_number = rec.mosaic_number as i8;
            }

            Entry::Vacant(v) => {
                // No detection, at least as far as the pipeline's
                // source-matching efforts can tell. Fill in the upper limit.
                v.insert(rec.into());
            }
        }
    }

    // Now that we have everything, flatten into a list and sort it by JD.

    let mut outputs: Vec<_> = outputs.drain().map(|(_k, v)| v).collect();
    outputs.sort_by(|a, b| a.date_jd.partial_cmp(&b.date_jd).unwrap());

    // With all that done, emitting is easy.

    let mut lines = vec![OutputRecord::csv_header()];
    lines.extend(outputs.drain(..).map(|r| r.as_csv_row()));
    simple_response(&lines)
}
