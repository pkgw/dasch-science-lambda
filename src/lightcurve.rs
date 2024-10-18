//! The lightcurve retrieval API.

use lambda_http::Error;
use serde::Deserialize;
use serde_json::Value;

/// Sync with `json-schemas/lightcurve_request.json`, which then needs to be
/// synced into S3.
#[derive(Deserialize)]
pub struct Request {
    refcat: String,
    _ref_number: u64,
}

pub async fn handler(
    req: Option<Value>,
    binning: &crate::gscbin::GscBinning,
) -> Result<Value, Error> {
    Ok(serde_json::to_value(
        implementation(
            serde_json::from_value(req.ok_or_else(|| -> Error { "no request payload".into() })?)?,
            binning,
        )
        .await?,
    )?)
}

pub async fn implementation(
    request: Request,
    _binning: &crate::gscbin::GscBinning,
) -> Result<Vec<String>, Error> {
    let lines = Vec::new();

    // Validation

    match request.refcat.as_ref() {
        "apass" | "atlas" => {}
        _ => {
            return Err("illegal refcat parameter".into());
        }
    }

    Ok(lines)
}
