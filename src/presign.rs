//! API services to get presigned S3 links.
//!
//! These are very simple API endpoints that return presigned S3 links for
//! various data resources.

use aws_sdk_s3::presigning::PresigningConfig;
use lambda_http::Error;
use serde::Deserialize;
use serde_json::Value;

use crate::http_types::{Body, Response, ResponseBuilder, StatusCode};

const INFRA_BUCKET: &str = "dasch-prod-infra";

#[derive(Deserialize)]
struct PhotcalAsdfRequest {
    /// In the live system, this field is autofilled by the proxyevent wrapper
    /// from a path parameter.
    hexid: String,
}

pub async fn handle_photcal_asdf(
    req: Option<Value>,
    s3: &aws_sdk_s3::Client,
    pc: &PresigningConfig,
) -> Result<Response, Error> {
    Ok(implement_photcal_asdf(
        serde_json::from_value(req.ok_or_else(|| -> Error { "no request payload".into() })?)?,
        s3,
        pc,
    )
    .await?)
}

async fn implement_photcal_asdf(
    request: PhotcalAsdfRequest,
    s3: &aws_sdk_s3::Client,
    pc: &PresigningConfig,
) -> Result<Response, Error> {
    let key = format!("pipeline/photcal/{}.asdf", request.hexid);

    let prereq = s3
        .get_object()
        .bucket(INFRA_BUCKET)
        .key(&key)
        .presigned(pc.clone())
        .await?;

    let builder = ResponseBuilder::new()
        .header("Location", prereq.uri())
        .status(StatusCode::TEMPORARY_REDIRECT);

    Ok(builder.body(Body::Empty)?)
}
