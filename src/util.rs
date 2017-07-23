use autoscaling::*;
use visibility::*;
use consumer::*;
use delete::*;
use processor::*;

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

use rusoto_sns::{Sns};

use rusoto_sqs::{Sqs, SqsClient, CreateQueueRequest};
use rusoto_credential::{ProvideAwsCredentials, ChainProvider, ProfileProvider};
use rusoto_sns::SnsClient;

use std::sync::{Arc, Mutex};
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

use lru_time_cache::LruCache;

#[derive(Debug)]
pub enum Topic {
    Cached(String),
    Created(String)
}

impl Topic {
    pub fn get(self) -> String {
        match self {
            Topic::Cached(t) | Topic::Created(t) => t,
        }
    }
}


use lru_time_cache::Entry;
use rusoto_sns::CreateTopicInput;

pub struct TopicCreator<SN>
    where SN: Sns + Send + Sync + 'static,
{
    sns_client: Arc<SN>,
    topic_cache: LruCache<String, String>,
}

impl<SN> TopicCreator<SN>
    where SN: Sns + Send + Sync + 'static,
{
    pub fn new(sns_client: Arc<SN>) -> TopicCreator<SN> {
        TopicCreator {
            sns_client,
            topic_cache: LruCache::with_expiry_duration_and_capacity(Duration::from_secs(60 * 60), 500),
        }
    }

    pub fn get_or_create(&mut self, topic_name: &str) -> Result<Topic, String> {
        let entry = self.topic_cache.entry(topic_name.to_owned());

        match entry {
            Entry::Occupied(oc) => {
                Ok(Topic::Cached(oc.into_mut().to_owned()))
            }
            Entry::Vacant(vac) => {
                let create_topic_input = CreateTopicInput {
                    name: topic_name.to_owned()
                };
                let arn_res = self.sns_client.create_topic(&create_topic_input);

                match arn_res {
                    Ok(arn) => {
                        match arn.topic_arn {
                            Some(arn) => {
                                //                                info!("Created topic: {}", arn);
                                vac.insert(arn.clone());
                                Ok(Topic::Created(arn))
                            }
                            None => Err("returned arn was None".to_owned())
                        }
                    }
                    Err(e) => {
                        Err(format!("{}", e))
                    }
                }
            }
        }
    }
}

use processor::{MessageHandlerActor, MessageHandler};

pub fn easy_init<F, P>(new: F, processor_count: usize, logger: Logger)
    -> impl Consumer
    where P: MessageHandler + Send + 'static,
          F: Fn(MessageHandlerActor) -> P,
{

    set_timer();

    let provider = get_profile_provider();

    let queue_url = "some queue url".to_owned();

    let sqs_client = Arc::new(new_sqs_client(&provider));

    let throttler = MedianThrottler::new(logger.clone());
    let throttler = ThrottlerActor::new(throttler);

    let deleter = MessageDeleterBroker::new(
        |_| {
            MessageDeleter::new(sqs_client.clone(), queue_url.clone(), logger.clone())
        },
        5,
        None
    );

    let deleter = MessageDeleteBuffer::new(deleter, Duration::from_millis(50));
    let deleter = MessageDeleteBufferActor::new(deleter);

    let delete_flusher = DeleteBufferFlusher::new(deleter.clone(), Duration::from_secs(1));
    DeleteBufferFlusherActor::new(delete_flusher.clone());

    let broker = VisibilityTimeoutExtenderBroker::new(
        |_| {
            VisibilityTimeoutExtender::new(sqs_client.clone(),
                                           queue_url.clone(),
                                           deleter.clone(),
                                           throttler.clone(),
                                           logger.clone())
        },
        5,
        None
    );

    let sc = Some(get_short_circuit());
    let buffer = VisibilityTimeoutExtenderBuffer::new(broker, 2, sc.clone());
    let buffer = VisibilityTimeoutExtenderBufferActor::new(buffer);

    let flusher = BufferFlushTimer::new(buffer.clone(), Duration::from_millis(200));
    let _flusher = BufferFlushTimerActor::new(flusher);

    let consumer_throttler = ConsumerThrottler::new(logger.clone());
    let consumer_throttler = ConsumerThrottlerActor::new(consumer_throttler);

    let state_manager = SqsMessageStateManager::new(buffer,
                                                    deleter.clone(),
                                                    logger.clone());

    let state_manager = MessageStateManagerActor::new(state_manager);

    let processor = MessageHandlerBroker::new(
        new,
        processor_count,
        1000,
        state_manager.clone(),
        sc.clone(),
        logger.clone()
    );

    let mut sqs_broker = ConsumerBroker::new(
        |actor| {
            DelayMessageConsumer::new(sqs_client.clone(),
                                      queue_url.clone(),
                                      actor,
                                      state_manager.clone(),
                                      processor.clone(),
                                      throttler.clone(),
                                      logger.clone())
        },
        1,
        None,
        logger.clone()
    );

    consumer_throttler.register_consumer(sqs_broker.clone());

    throttler.register_consumer_throttler(consumer_throttler);

    sqs_broker.consume();

    sqs_broker
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