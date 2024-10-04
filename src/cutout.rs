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

pub async fn handle_cutout(
    event: LambdaEvent<Request>,
    dc: &aws_sdk_dynamodb::Client,
) -> Result<String, Error> {
    let (request, context) = event.into_parts();
    let cfg = context.env_config;
    println!("*** fn name={} version={}", cfg.function_name, cfg.version);

    Ok("yo".to_owned())
}
