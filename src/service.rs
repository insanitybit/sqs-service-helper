use autoscaling::*;
use visibility::*;
use consumer::*;
use delete::*;
use processor::*;

use slog::Logger;
use util::*;

use std::sync::Arc;
use std::time::Duration;

pub struct Service {
    consumers: ConsumerBroker
}

impl Service {
    pub fn start(&mut self) {
        self.consumers.consume();
    }

    pub fn stop(&mut self) {
        self.consumers.shut_down();
    }
}

#[derive(Default)]
pub struct SqsServiceBuilder<F, P>
    where P: MessageHandler + Send + 'static,
          F: Fn(MessageHandlerActor) -> P,
{
    new: Option<F>,
    queue_url: Option<String>,
    consumer_count: Option<usize>,
    deleter_count: Option<usize>,
    deleter_buffer_flush_period: Option<Duration>,
    visibility_extender_count: Option<usize>,
    visibility_buffer_flush_period: Option<Duration>,
    msg_handler_count: Option<usize>,
    msg_handler_max_queue_depth: Option<usize>,
    throttle_consumers: Option<bool>,
    short_circuit: Option<bool>,
    logger: Option<Logger>
}

impl<F, P> SqsServiceBuilder<F, P>
    where P: MessageHandler + Send + 'static,
          F: Fn(MessageHandlerActor) -> P,
{
    pub fn with_message_handler(&mut self, new: F) -> &mut Self {
        self.new = Some(new);
        self
    }

    pub fn with_queue_url(&mut self, queue_url: String) -> &mut Self {
        self.queue_url = Some(queue_url);
        self
    }

    pub fn with_consumer_count(&mut self, arg: usize) -> &mut Self {
        self.consumer_count = Some(arg);
        self
    }

    pub fn with_deleter_count(&mut self, arg: usize) -> &mut Self {
        self.deleter_count = Some(arg);
        self
    }

    pub fn with_deleter_buffer_flush_period(&mut self, arg: Duration) -> &mut Self {
        self.deleter_buffer_flush_period = Some(arg);
        self
    }

    pub fn with_visibility_extender_count(&mut self, arg: usize) -> &mut Self {
        self.visibility_extender_count = Some(arg);
        self
    }

    pub fn with_visibility_buffer_flush_period(&mut self, arg: Duration) -> &mut Self {
        self.visibility_buffer_flush_period = Some(arg);
        self
    }

    pub fn with_msg_handler_count(&mut self, arg: usize) -> &mut Self {
        self.msg_handler_count = Some(arg);
        self
    }

    pub fn with_msg_handler_max_queue_depth(&mut self, arg: usize) -> &mut Self {
        self.msg_handler_max_queue_depth = Some(arg);
        self
    }

    pub fn with_throttle_consumers(&mut self, arg: bool) -> &mut Self {
        self.throttle_consumers = Some(arg);
        self
    }

    pub fn with_short_circuit(&mut self, arg: bool) -> &mut Self {
        self.short_circuit = Some(arg);
        self
    }

    pub fn with_logger(&mut self, arg: Logger) -> &mut Self {
        self.logger = Some(arg);
        self
    }

    pub fn build(self) -> Service {
        let new = self.new
            .expect("new");

        let queue_url = self.queue_url
            .expect("queue_url");

        let consumer_count = self.consumer_count
            .unwrap_or(1);

        let deleter_count = self.deleter_count
            .expect("deleter_count");

        let deleter_buffer_flush_period = self.deleter_buffer_flush_period
            .expect("deleter_buffer_flush_period");

        let visibility_extender_count = self.visibility_extender_count
            .expect("visibility_extender_count");

        let visibility_buffer_flush_period = self.visibility_buffer_flush_period
            .expect("visibility_buffer_flush_period");

        let msg_handler_count = self.msg_handler_count
            .expect("msg_handler_count");

        let msg_handler_max_queue_depth = self.msg_handler_max_queue_depth
            .expect("msg_handler_max_queue_depth");

        let throttle_consumers = self.throttle_consumers
            .expect("throttle_consumers");

        let short_circuit = self.short_circuit
            .expect("short_circuit");

        let logger = self.logger
            .expect("logger");


        set_timer();

        let provider = get_profile_provider();

        let sqs_client = Arc::new(new_sqs_client(&provider));

        let throttler = MedianThrottler::new(logger.clone());
        let throttler = ThrottlerActor::new(throttler);

        let deleter = MessageDeleterBroker::new(
            |_| {
                MessageDeleter::new(sqs_client.clone(), queue_url.clone(), logger.clone())
            },
            deleter_count,
            None
        );

        let deleter = MessageDeleteBuffer::new(deleter, deleter_buffer_flush_period);
        let deleter = MessageDeleteBufferActor::new(deleter);

        let delete_flusher = DeleteBufferFlusher::new(deleter.clone(), deleter_buffer_flush_period);
        let _delete_flusher = DeleteBufferFlusherActor::new(delete_flusher.clone());

        let broker = VisibilityTimeoutExtenderBroker::new(
            |_| {
                VisibilityTimeoutExtender::new(sqs_client.clone(),
                                               queue_url.clone(),
                                               deleter.clone(),
                                               throttler.clone(),
                                               logger.clone())
            },
            visibility_extender_count,
            None
        );

        let sc = if short_circuit {
            Some(get_short_circuit())
        } else {
            None
        };

        let buffer = VisibilityTimeoutExtenderBuffer::new(broker,
                                                          visibility_buffer_flush_period,
                                                          sc.clone());

        let buffer = VisibilityTimeoutExtenderBufferActor::new(buffer);

        let flusher = BufferFlushTimer::new(buffer.clone(), visibility_buffer_flush_period);
        let _flusher = BufferFlushTimerActor::new(flusher);

        let consumer_throttler = ConsumerThrottler::new(logger.clone());
        let consumer_throttler = ConsumerThrottlerActor::new(consumer_throttler);

        let state_manager = SqsMessageStateManager::new(buffer,
                                                        deleter.clone(),
                                                        logger.clone());

        let state_manager = MessageStateManagerActor::new(state_manager);

        let processor = MessageHandlerBroker::new(
            new,
            msg_handler_count,
            msg_handler_max_queue_depth,
            state_manager.clone(),
            sc.clone(),
            logger.clone()
        );

        let sqs_broker = ConsumerBroker::new(
            |actor| {
                DelayMessageConsumer::new(sqs_client.clone(),
                                          queue_url.clone(),
                                          actor,
                                          state_manager.clone(),
                                          processor.clone(),
                                          throttler.clone(),
                                          logger.clone())
            },
            consumer_count,
            None,
            logger.clone()
        );


        if throttle_consumers {
            consumer_throttler.register_consumer(sqs_broker.clone());
            throttler.register_consumer_throttler(consumer_throttler);
        }

        Service {
            consumers: sqs_broker
        }
    }
}