//! The cutout API service
//!
//! This is probably the most sophisticated service that we have. We query
//! DynamoDB to get information about a plate and its astrometry; then use
//! wcslib to figure out what part of the full-size plate we need to sample;
//! then stream data from the compressed FITS file on S3; then resample onto the
//! target coordinate system.
//!
//! Fortunately, our resulting cutout size stays within the 6 MB limit given to
//! buffered Lambdas, which means we can operate in the cheaper buffered mode.
//! The result of a buffered Lambda can only be JSON, so we return a complete
//! FITS file as a Base64-encoded string. (TODO: gzip it.)

use aws_sdk_dynamodb::types::AttributeValue;
use lambda_runtime::{Error, LambdaEvent};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct Request {
    plate_id: String,
    solution_number: usize,
    center_ra_deg: f64,
    center_dec_deg: f64,
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
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlatesMosaicResult {
    b01_height: usize,
    b01_width: usize,
    s3_key_template: String,
}

pub async fn handle_cutout(
    event: LambdaEvent<Request>,
    dc: &aws_sdk_dynamodb::Client,
) -> Result<String, Error> {
    let (request, context) = event.into_parts();
    let cfg = context.env_config;
    println!("*** fn name={} version={}", cfg.function_name, cfg.version);

    // Get the information we need about this plate and validate the basic request.

    let plates_table = format!("dasch-{}-dr7-plates", super::ENVIRONMENT);

    let result = dc
        .get_item()
        .table_name(plates_table)
        .key("plateId", AttributeValue::S(request.plate_id.clone()))
        .projection_expression(
            "astrometry.b01HeaderGz,\
            astrometry.nSolutions,\
            astrometry.rotationDelta,\
            mosaic.b01Height,\
            mosaic.b01Width,\
            mosaic.s3KeyTemplate",
        )
        .send()
        .await?;

    let item = result
        .item
        .ok_or_else(|| -> Error { format!("no such plate_id `{}`", request.plate_id).into() })?;

    let item: PlatesResult = serde_dynamo::from_item(item)?;
    let mos_data = item.mosaic.ok_or_else(|| -> Error {
        format!(
            "plate `{}` has no registered FITS mosaic information (never scanned?)",
            request.plate_id
        )
        .into()
    })?;
    let astrom_data = item.astrometry.ok_or_else(|| -> Error {
        format!(
            "plate `{}` has no registered astrometric solutions",
            request.plate_id
        )
        .into()
    })?;

    if request.solution_number >= astrom_data.n_solutions {
        return Err(format!(
            "requested astrometric solution #{} (0-based) for plate `{}` but it only has {} solutins",
            request.solution_number,
            request.plate_id,
            astrom_data.n_solutions
        )
        .into());
    }

    //

    Ok("yo".to_owned())
}
