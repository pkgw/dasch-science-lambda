//! API services to get presigned S3 links.
//!
//! These are very simple API endpoints that return presigned S3 links for
//! various data resources. For processing convenience, the response is a 307
//! redirect to the desired link, also containing JSON content capturing the
//! link.

use aws_sdk_s3::presigning::PresigningConfig;
use lambda_http::Error;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{
    http_types::{Body, HttpOptionExt, Response, ResponseBuilder, StatusCode},
    INFRA_BUCKET,
};

/// Return a generic redirection response. The status code will be 307, with a
/// Location header, and the response body content will be JSON matching the
/// `json-schemas/generic_location.json` schema.
pub fn redirect_response<S: AsRef<str>>(location: S) -> Result<Response, Error> {
    let location = location.as_ref();
    let builder = ResponseBuilder::new()
        .header("Location", location)
        .status(StatusCode::TEMPORARY_REDIRECT);
    let text = serde_json::to_string(&json!({"location": location}))?;
    Ok(builder.body(Body::Text(text))?)
}

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
        serde_json::from_value(req.ok_or_bad_request("no request payload")?)?,
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

    redirect_response(prereq.uri())
}
