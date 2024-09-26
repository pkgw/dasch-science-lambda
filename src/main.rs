use lambda_runtime::{service_fn, Error, LambdaEvent};
use serde_json::{json, Value};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let config = aws_config::load_from_env().await;
    let client = aws_sdk_dynamodb::Client::new(&config);

    let func = service_fn(|event| func(event, &client));
    lambda_runtime::run(func).await?;
    Ok(())
}

async fn func(event: LambdaEvent<Value>, dc: &aws_sdk_dynamodb::Client) -> Result<Value, Error> {
    let (event, context) = event.into_parts();
    let first_name = event["firstName"].as_str().unwrap_or("world");

    println!("\nRID: {}", context.request_id);
    println!("inv fn ARN: {}", context.invoked_function_arn);
    let cfg = context.env_config;
    println!("fn name: {}", cfg.function_name);
    println!("version: {}\n", cfg.version);

    Ok(json!({ "message": format!("Hello, {}!", first_name) }))
}
