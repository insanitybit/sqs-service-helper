use visibility::*;
use autoscaling::*;
use processor::*;

use rusoto_sqs::{Sqs, ReceiveMessageRequest};
use slog::Logger;
use uuid;

use std::time::{Instant, Duration};
use std::thread;
use std::sync::Arc;

use two_lock_queue::{Sender, Receiver, RecvTimeoutError, unbounded, channel};

pub const MAX_INFLIGHT_MESSAGES: usize = 100;

pub trait Consumer {
    fn consume(&mut self);
    fn throttle(&mut self, how_long: Duration);
    fn shut_down(&mut self);
    fn route_msg(&mut self, msg: ConsumerMessage);
}

pub struct DelayMessageConsumer<C, M, T, SQ>
    where C: Consumer + Send + 'static,
          M: MessageStateManager + Send + 'static,
          T: Throttler + Send + 'static,
          SQ: Sqs + Send + Sync + 'static,
{
    sqs_client: Arc<SQ>,
    queue_url: String,
    actor: C,
    vis_manager: M,
    processor: MessageHandlerBroker,
    throttler: T,
    throttle: Duration,
    logger: Logger
}

impl<C, M, T, SQ> DelayMessageConsumer<C, M, T, SQ>
    where C: Consumer + Send + 'static,
          M: MessageStateManager + Send + 'static,
          T: Throttler + Send + 'static,
          SQ: Sqs + Send + Sync + 'static,
{
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn new(sqs_client: Arc<SQ>,
               queue_url: String,
               actor: C,
               vis_manager: M,
               processor: MessageHandlerBroker,
               throttler: T,
               logger: Logger)
               -> DelayMessageConsumer<C, M, T, SQ>
    {
        DelayMessageConsumer {
            sqs_client,
            queue_url,
            actor,
            vis_manager,
            processor,
            throttler,
            throttle: Duration::from_millis(500),
            logger
        }
    }
}

impl<C, M, T, SQ> Consumer for DelayMessageConsumer<C, M, T, SQ>
    where C: Consumer + Send + 'static,
          M: MessageStateManager + Send + 'static,
          T: Throttler + Send + 'static,
          SQ: Sqs + Send + Sync + 'static,
{
    #[cfg_attr(feature = "flame_it", flame)]
    fn consume(&mut self) {
        let msg_request = ReceiveMessageRequest {
            max_number_of_messages: Some(10),
            queue_url: self.queue_url.to_owned(),
            wait_time_seconds: Some(20),
            ..Default::default()
        };

        let now = Instant::now();
        // If we receive a network error we'll sleep for a few ms and retry
        let messages = match self.sqs_client.receive_message(&msg_request) {
            Ok(res) => {
                res.messages
            }
            Err(e) => {
                warn!(self.logger, "Failed to receive sqs message. {}", e);
                return;
            }
        };

        let dur = Instant::now() - now;

        if let Some(mut messages) = messages {
            let o_len = messages.len();
            messages.sort_by(|a, b| a.receipt_handle.cmp(&b.receipt_handle));
            messages.dedup_by(|a, b| a.receipt_handle == b.receipt_handle);

            if o_len != messages.len() {
                warn!(self.logger, "Contained duplicate messages!");
            }

            let messages: Vec<_> = messages.iter().filter_map(|msg| {
                match msg.receipt_handle {
                    Some(ref receipt) if msg.body.is_some() => {
                        let now = Instant::now();
                        self.vis_manager.register(receipt.to_owned(), Duration::from_secs(30), now);
                        self.throttler.message_start(receipt.to_owned(), now);
                        Some(msg)
                    }
                    _ => None
                }
            }).collect();

            trace!(self.logger, "Processing {} messages", messages.len());

            for message in messages {
                self.processor.process(message.clone());
            }

            if dur < self.throttle {
                thread::sleep(self.throttle - dur);
            }
        }
    }

    fn throttle(&mut self, how_long: Duration)
    {
        self.throttle = how_long;
    }

    fn shut_down(&mut self) {}

    fn route_msg(&mut self, msg: ConsumerMessage) {
        match msg {
            ConsumerMessage::Consume => self.consume(),
            ConsumerMessage::Throttle { how_long } => self.throttle(how_long),
            ConsumerMessage::ShutDown => {
                self.shut_down();
                return;
            }
        };

        self.actor.consume();
    }
}

#[derive(Debug)]
pub enum ConsumerMessage
{
    Consume,
    Throttle { how_long: Duration },
    ShutDown
}

#[derive(Clone)]
pub struct ConsumerActor
{
    sender: Sender<ConsumerMessage>,
    p_sender: Sender<ConsumerMessage>,
    id: String
}

impl ConsumerActor
{
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn new<C, F>(new: F)
                     -> ConsumerActor
        where C: Consumer + Send + 'static,
              F: Fn(ConsumerActor) -> C,
    {
        let (sender, receiver) = unbounded();
        let (p_sender, p_receiver) = unbounded();
        let id = uuid::Uuid::new_v4().to_string();

        let actor = ConsumerActor {
            sender,
            p_sender,
            id,
        };

        let mut _actor = new(actor.clone());

        let recvr = receiver.clone();
        let p_recvr = p_receiver.clone();
        thread::spawn(
            move || {
                loop {
                    if let Ok(msg) = p_recvr.try_recv() {
                        _actor.route_msg(msg);
                        continue
                    }

                    match recvr.recv_timeout(Duration::from_secs(30)) {
                        Ok(msg) => {
                            _actor.route_msg(msg);
                            continue
                        }
                        Err(RecvTimeoutError::Disconnected) => {
                            break
                        }
                        Err(RecvTimeoutError::Timeout) => {
                            continue
                        }
                    }
                }
            });

        actor
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn from_queue<C, F>(new: &F,
                            sender: Sender<ConsumerMessage>,
                            receiver: Receiver<ConsumerMessage>,
                            p_sender: Sender<ConsumerMessage>,
                            p_receiver: Receiver<ConsumerMessage>)
                            -> ConsumerActor
        where C: Consumer + Send + 'static,
              F: Fn(ConsumerActor) -> C,
    {
        let id = uuid::Uuid::new_v4().to_string();

        let actor = ConsumerActor {
            sender,
            p_sender,
            id,
        };

        let mut _actor = new(actor.clone());

        let recvr = receiver.clone();
        let p_recvr = p_receiver.clone();
        thread::spawn(
            move || {
                loop {
                    if let Ok(msg) = p_recvr.try_recv() {
                        _actor.route_msg(msg);
                        continue
                    }

                    match recvr.recv_timeout(Duration::from_secs(30)) {
                        Ok(msg) => {
                            _actor.route_msg(msg);
                            continue
                        }
                        Err(RecvTimeoutError::Disconnected) => {
                            break
                        }
                        Err(RecvTimeoutError::Timeout) => {
                            continue
                        }
                    }
                }
            });

        actor
    }
}

impl Consumer for ConsumerActor {
    #[cfg_attr(feature = "flame_it", flame)]
    fn consume(&mut self) {
        self.sender.send(ConsumerMessage::Consume)
            .expect("Underlying consumer has died");
    }

    #[cfg_attr(feature = "flame_it", flame)]
    fn throttle(&mut self, how_long: Duration) {
        self.p_sender.send(ConsumerMessage::Throttle { how_long })
            .expect("Underlying consumer has died");
    }

    fn shut_down(&mut self) {
        let _ = self.p_sender.send(ConsumerMessage::ShutDown);
    }

    fn route_msg(&mut self, msg: ConsumerMessage) {
        self.sender.send(msg).unwrap();
    }
}

#[derive(Clone)]
pub struct ConsumerBroker
{
    pub workers: Vec<ConsumerActor>,
    pub worker_count: usize,
    sender: Sender<ConsumerMessage>,
    p_sender: Sender<ConsumerMessage>,
    new: ConsumerActor,
    logger: Logger,
    id: String
}

impl ConsumerBroker
{
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn new<T, C, F>(new: F,
                        worker_count: usize,
                        max_queue_depth: T,
                        logger: Logger)
                        -> ConsumerBroker
        where C: Consumer + Send + 'static,
              F: Fn(ConsumerActor) -> C,
              T: Into<Option<usize>>,
    {
        let id = uuid::Uuid::new_v4().to_string();

        let (sender, receiver) = max_queue_depth.into().map_or(unbounded(), channel);
        let (p_sender, p_receiver) = unbounded();

        let workers: Vec<_> = (0..worker_count)
            .map(|_| ConsumerActor::from_queue(&new, sender.clone(), receiver.clone(),
                                               p_sender.clone(), p_receiver.clone()))
            .collect();

        let worker_count = workers.len();

        ConsumerBroker {
            workers,
            worker_count,
            sender: sender.clone(),
            p_sender: p_sender.clone(),
            new: ConsumerActor::from_queue(&new, sender.clone(), receiver.clone(),
                                           p_sender.clone(), p_receiver.clone()),
            logger,
            id
        }
    }

    pub fn add_consumer(&mut self) {
        if self.worker_count < 50 {
            self.workers.push(self.new.clone());
            self.worker_count += 1;
        }
        debug!(self.logger, "Adding consumer: {}", self.worker_count);
    }

    pub fn drop_consumer(&mut self) {
        if self.worker_count > 1 {
            self.workers.pop();
            self.worker_count -= 1;
        }
        debug!(self.logger, "Dropping consumer: {}", self.worker_count);
    }

    pub fn shut_down(&mut self) {
        self.workers.clear();
        self.worker_count = 0;
    }

    pub fn shut_down_blocking(&mut self) {
        self.workers.clear();
        self.worker_count = 0;
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn consume(&self) {
        self.sender.send(
            ConsumerMessage::Consume
        ).unwrap();
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn throttle(&self, how_long: Duration) {
        self.sender.send(
            ConsumerMessage::Throttle { how_long }
        ).unwrap();
    }
}

#[derive(Clone)]
pub struct ConsumerThrottler {
    consumer_broker: Option<ConsumerBroker>,
    logger: Logger
}

impl ConsumerThrottler {
    pub fn new(logger: Logger)
        -> ConsumerThrottler {
        ConsumerThrottler {
            consumer_broker: None,
            logger
        }
    }

    pub fn throttle(&self, how_long: Duration) {
        match self.consumer_broker {
            Some(ref consumer_broker) => {
                for _ in 0..consumer_broker.worker_count {
                    consumer_broker.throttle(how_long)
                }
            }
            None => error!(self.logger, "No consumer registered with ConsumerThrottler")
        }
    }

    pub fn register_consumer(&mut self, consumer: ConsumerBroker) {
        self.consumer_broker = Some(consumer);
    }

    pub fn drop_consumer(&mut self) {
        self.consumer_broker.as_mut().unwrap().drop_consumer()
    }

    pub fn add_consumer(&mut self) {
        self.consumer_broker.as_mut().unwrap().add_consumer()
    }

    fn route_msg(&mut self, msg: ConsumerThrottlerMessage) {
        match msg {
            ConsumerThrottlerMessage::Throttle { how_long } => self.throttle(how_long),
            ConsumerThrottlerMessage::RegisterconsumerBroker { consumer } => self.register_consumer(consumer),
            ConsumerThrottlerMessage::DropConsumer => self.drop_consumer(),
            ConsumerThrottlerMessage::AddConsumer => self.add_consumer(),
        }
    }
}

pub enum ConsumerThrottlerMessage
{
    Throttle { how_long: Duration },
    RegisterconsumerBroker { consumer: ConsumerBroker },
    DropConsumer,
    AddConsumer,
}

#[derive(Clone)]
pub struct ConsumerThrottlerActor
{
    sender: Sender<ConsumerThrottlerMessage>,
    p_sender: Sender<ConsumerThrottlerMessage>,
    id: String
}

impl ConsumerThrottlerActor
{
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn new(actor: ConsumerThrottler)
               -> ConsumerThrottlerActor
    {
        let (sender, receiver) = unbounded();
        let (p_sender, p_receiver) = unbounded();
        let id = uuid::Uuid::new_v4().to_string();

        let mut actor = actor;

        let recvr = receiver.clone();
        thread::spawn(
            move || {
                loop {
                    if let Ok(msg) = p_receiver.try_recv() {
                        actor.route_msg(msg);
                    }

                    match recvr.recv_timeout(Duration::from_secs(30)) {
                        Ok(msg) => {
                            actor.route_msg(msg);
                        }
                        Err(RecvTimeoutError::Disconnected) => {
                            break
                        }
                        Err(RecvTimeoutError::Timeout) => {}
                    }
                }
            });

        ConsumerThrottlerActor {
            sender: sender.clone(),
            p_sender: p_sender.clone(),
            id: id,
        }
    }

    pub fn register_consumer(&self, consumer: ConsumerBroker) {
        self.sender.send(ConsumerThrottlerMessage::RegisterconsumerBroker { consumer })
            .expect("ConsumerThrottlerActor.register_consumer receivers have died");
    }

    pub fn throttle(&self, how_long: Duration) {
        self.p_sender.send(ConsumerThrottlerMessage::Throttle { how_long })
            .expect("Underlying consumer has died");
    }

    pub fn add_consumer(&self) {
        self.p_sender.send(ConsumerThrottlerMessage::AddConsumer)
            .expect("Underlying consumer has died");
    }

    pub fn drop_consumer(&self) {
        self.p_sender.send(ConsumerThrottlerMessage::DropConsumer)
            .expect("Underlying consumer has died");
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use mocks::*;

    use std::collections::HashMap;
    use util::init_logger;

    #[test]
    pub fn test_consumer() {
        let logger = init_logger("test_consumer.log");

        let mock_consumer = MockConsumer::new();
        let mock_state_manager = MockMessageStateManager::new();

        let (processor_sender, processor_receiver) = unbounded();
        let sndr = processor_sender.clone();
        let rcvr = processor_receiver.clone();
        let mock_processor_broker = MessageHandlerBroker::new(
            move |_| {
                MockProcessor::from_queue(sndr.clone(),
                                          rcvr.clone())
            },
            1,
            None,
            mock_state_manager.clone(),
            None,
            logger.clone()
        );

        let throttler = MockThrottler::new();

        let mut consumer = DelayMessageConsumer::new(
            Arc::new(new_sqs_client()),
            "".to_owned(),
            mock_consumer.clone(),
            mock_state_manager.clone(),
            mock_processor_broker.clone(),
            throttler.clone(),
            logger.clone()
        );

        consumer.consume();
        consumer.shut_down();

        thread::sleep(Duration::from_secs(2));

        let mut state_msgs = vec![];

        loop {
            match mock_state_manager.receiver.try_recv() {
                Ok(msg) => state_msgs.push(msg),
                _ => break
            }
        }

        let mut receipt_count = HashMap::new();

        let (registers, deregisters): (Vec<_>, _) =
            state_msgs.into_iter().partition(
                |m| {
                    match *m {
                        MessageStateManagerMessage::RegisterVariant { ref receipt, .. }
                        => {
                            *receipt_count.entry(receipt.clone()).or_insert(0) += 1;
                            true
                        }
                        MessageStateManagerMessage::DeregisterVariant { ref receipt, .. }
                        => {
                            *receipt_count.entry(receipt.clone()).or_insert(0) -= 1;
                            false
                        }
                    }
                }
            );

        assert_eq!(registers.len(), deregisters.len());

        // For every receipt there should be one register and one deregister
        // Registers increment, Deregisters decrement, hence compare to 0
        for count in receipt_count.values() {
            assert_eq!(*count, 0);
        }

        processor_receiver
            .try_recv()
            .expect("Processor never received a message");


        let mut throttler_msgs = vec![];

        loop {
            match throttler.receiver.try_recv() {
                Ok(m) => throttler_msgs.push(m),
                _ => break
            }
        }

        let mut receipt_count = HashMap::new();

        let (msg_starts, _msg_stops): (Vec<_>, _) =
            throttler_msgs.into_iter().partition(
                |m| {
                    match *m {
                        ThrottlerMessage::Start { ref receipt, .. }
                        => {
                            *receipt_count.entry(receipt.clone()).or_insert(0) += 1;
                            true
                        }
                        ThrottlerMessage::Stop { ref receipt, .. }
                        => {
                            *receipt_count.entry(receipt.clone()).or_insert(0) -= 1;
                            false
                        }
                        _ => panic!("Unexpected message")
                    }
                }
            );

        assert_eq!(msg_starts.len(), 10);
    }
}