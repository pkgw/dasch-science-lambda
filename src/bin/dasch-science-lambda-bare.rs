//! "Bare" version of the DASCH science Lambda implementations.
//!
//! This executable defines a server that you can easily interact with locally.
//! For the cloud deployment, we need to use the "proxy event" version, which
//! has additional infrastructure to interact with AWS API Gateway's "proxy
//! event" framework.

use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use serde_json::Value;

use dasch_science_lambda::Services;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let svcs = Services::init().await?;
    let ref_svcs = &svcs;

    run(service_fn(|event: LambdaEvent<Value>| async move {
        let (payload, context) = event.into_parts();
        ref_svcs
            .dispatch(context.invoked_function_arn, Some(payload))
            .await
    }))
    .await?;
    Ok(())
}
