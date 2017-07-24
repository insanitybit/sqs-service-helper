use rusoto_core::{default_tls_client, Region};

use std::fs::OpenOptions;
use slog;
use slog::Logger;
use slog_json;

use slog::{Drain, FnValue};
use futures_cpupool::CpuPool;
use tokio_timer::*;
use futures::*;
use futures::future::ok;
use std::time::Duration;
use rusoto_sqs::{Sqs, SqsClient, CreateQueueRequest};
use rusoto_credential::{ProvideAwsCredentials, ChainProvider, ProfileProvider};
use rusoto_sns::SnsClient;

use std::sync::Mutex;
use std::env;
use hyper;

static mut TIMER: Option<Timer> = None;

const NANOS_PER_MILLI: u32 = 1000_000;
const MILLIS_PER_SEC: u64 = 1000;

pub fn set_timer() {
    unsafe {
        TIMER = Some(Timer::default());
    }
}

pub fn get_timer() -> Timer {
    unsafe {
        TIMER.clone().unwrap().clone()
    }
}


pub fn millis(d: Duration) -> u64 {
    // A proper Duration will not overflow, because MIN and MAX are defined
    // such that the range is exactly i64 milliseconds.
    let secs_part = d.as_secs() * MILLIS_PER_SEC;
    let nanos_part = d.subsec_nanos() / NANOS_PER_MILLI;
    secs_part + nanos_part as u64
}

#[cfg_attr(feature = "flame_it", flame)]
pub fn new_sqs_client<P>(sqs_provider: &P) -> SqsClient<P, hyper::Client>
    where P: ProvideAwsCredentials + Clone + Send + 'static
{
    SqsClient::new(
        default_tls_client().unwrap(),
        sqs_provider.clone(),
        Region::UsEast1
    )
}

#[cfg_attr(feature = "flame_it", flame)]
pub fn new_sns_client<P>(sns_provider: &P) -> SnsClient<P, hyper::Client>
    where P: ProvideAwsCredentials + Clone + Send + 'static
{
    SnsClient::new(
        default_tls_client().unwrap(),
        sns_provider.clone(),
        Region::UsEast1
    )
}


#[cfg_attr(feature = "flame_it", flame)]
pub fn create_queue<P>(pool: &CpuPool, provider: &P, queue_name: &str, timer: &'static Timer) -> String
    where P: ProvideAwsCredentials + Clone + Send + 'static,
{
    let create_queue_request = CreateQueueRequest {
        attributes: None,
        queue_name: queue_name.to_owned()
    };

    let _provider = provider.clone();
    let _queue_name = queue_name.to_owned();

    let queue_url = timeout_ms! {
        pool.clone(),
        move || {
            ok(SqsClient::new(
                default_tls_client().unwrap(),
                _provider,
                Region::UsEast1
            ).create_queue(&create_queue_request)
                .unwrap_or_else(|e| panic!("Failed to create queue {} with {}", _queue_name, e)))
        },
        5_500,
        timer
    };

    match queue_url {
        Ok(url) => url.queue_url.expect("Queue url was None"),
        _ => panic!("Timeout while trying to create queue: {}", queue_name)
    }
}

#[cfg_attr(feature = "flame_it", flame)]
pub fn get_profile_provider() -> ChainProvider {
    let profile = match env::var("AWS_PROFILE") {
        Ok(val) => val.to_string(),
        Err(_) => "default".to_string(),
    };

    let mut profile_provider = ProfileProvider::new().unwrap();
    profile_provider.set_profile(profile);
    ChainProvider::with_profile_provider(profile_provider)
}

pub fn init_logger(log_path: &str) -> Logger {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(log_path)
        .expect(&format!("Failed to create log file {}", log_path));

    slog::Logger::root(
        Mutex::new(slog_json::Json::default(file)).map(slog::Fuse),
        o!("version" => env!("CARGO_PKG_VERSION"),
           "place" =>
              FnValue(move |info| {
                  format!("{}:{} {}",
                          info.file(),
                          info.line(),
                          info.module())
              }))
    )
}