//! API services to get presigned S3 links.
//!
//! These are very simple API endpoints that return presigned S3 links for
//! various data resources. While the lambda itself can only return JSON without
//! properly setting HTTP redirect headers, the API Gateway layer includes
//! functionality that can transform our response into a proper HTTP
//! redirection.

use lambda_http::Error;
use serde::Deserialize;
use serde_json::{json, Value};

/// Sync with `json-schemas/presign_photcal_asdf_request.json`, which then needs to be
/// synced into S3.
#[derive(Deserialize)]
struct PhotcalAsdfRequest {
    hexid: String,
}

fn location_response<D: std::fmt::Display>(value: D) -> Value {
    json!({
        "location": value.to_string(),
    })
}

pub async fn handle_photcal_asdf(
    req: Option<Value>,
    s3: &aws_sdk_s3::Client,
) -> Result<Value, Error> {
    Ok(implement_photcal_asdf(
        serde_json::from_value(req.ok_or_else(|| -> Error { "no request payload".into() })?)?,
        s3,
    )
    .await?)
}

async fn implement_photcal_asdf(
    request: PhotcalAsdfRequest,
    _s3: &aws_sdk_s3::Client,
) -> Result<Value, Error> {
    Ok(location_response(format!(
        "https://google.com/?q={}",
        request.hexid
    )))
}
