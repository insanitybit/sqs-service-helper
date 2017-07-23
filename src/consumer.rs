use std::time::{Instant, Duration};
use std::thread;

use rusoto_sqs::{Sqs, ReceiveMessageRequest};
use dogstatsd::{Client};
use std::sync::Arc;
use slog_scope;
use visibility::*;
use autoscaling::*;
use uuid;
use processor::*;

use two_lock_queue::{Sender, Receiver, RecvTimeoutError, unbounded, channel};

pub const MAX_INFLIGHT_MESSAGES: usize = 100;

pub trait Consumer {
    fn consume(&mut self);
    fn throttle(&mut self, Duration);
    fn shut_down(&mut self);
    fn route_msg(&mut self, ConsumerMessage);
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
    throttle: Duration
}

impl<C, M, T, SQ> DelayMessageConsumer<C, M, T, SQ>
    where C: Consumer + Send + 'static,
          M: MessageStateManager + Send + 'static,
          T: Throttler + Send + 'static,
          SQ: Sqs + Send + Sync + 'static,
{
    #[cfg_attr(feature="flame_it", flame)]
    pub fn new(sqs_client: Arc<SQ>,
               queue_url: String,
               actor: C,
               vis_manager: M,
               processor: MessageHandlerBroker,
               throttler: T)
               -> DelayMessageConsumer<C, M, T, SQ>
    {
        DelayMessageConsumer {
            sqs_client,
            queue_url,
            actor,
            vis_manager,
            processor,
            throttler,
            throttle: Duration::from_millis(500)
        }
    }
}

impl<C, M, T, SQ> Consumer for DelayMessageConsumer<C, M, T, SQ>
    where C: Consumer + Send + 'static,
          M: MessageStateManager + Send + 'static,
          T: Throttler + Send + 'static,
          SQ: Sqs + Send + Sync + 'static,
{
    #[cfg_attr(feature="flame_it", flame)]
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
                warn!(slog_scope::logger(), "Failed to receive sqs message. {}", e);
                return;
            }
        };

        let dur = Instant::now() - now;

        if let Some(mut messages) = messages {
            let o_len = messages.len();
            messages.sort_by(|a, b| a.receipt_handle.cmp(&b.receipt_handle));
            messages.dedup_by(|a, b| a.receipt_handle == b.receipt_handle);

            if o_len != messages.len() {
                warn!(slog_scope::logger(), "Contained duplicate messages!");
            }

            let messages: Vec<_> = messages.iter().filter_map(|msg| {
                match msg.receipt_handle {
                    Some(ref receipt) if msg.body.is_some() => {
                        let now = Instant::now();
                        self.vis_manager.register(receipt.to_owned(), Duration::from_secs(30), now);
                        self.throttler.message_start(receipt.to_owned(), now);
                        Some(msg)
                    }
                    _   => None
                }
            }).collect();

            trace!(slog_scope::logger(), "Processing {} messages", messages.len());

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

    fn shut_down(&mut self) {

    }

    fn route_msg(&mut self, msg: ConsumerMessage) {
        match msg {
            ConsumerMessage::Consume  => self.consume(),
            ConsumerMessage::Throttle {how_long}    => self.throttle(how_long),
            ConsumerMessage::ShutDown => {
                self.shut_down();
                return
            },
        };

        self.actor.consume();
    }
}

#[derive(Debug)]
pub enum ConsumerMessage
{
    Consume,
    Throttle {how_long: Duration},
    ShutDown
}

#[derive(Clone)]
pub struct ConsumerActor
{
    sender: Sender<ConsumerMessage>,
    receiver: Receiver<ConsumerMessage>,
    p_sender: Sender<ConsumerMessage>,
    p_receiver: Receiver<ConsumerMessage>,
    id: String
}

impl ConsumerActor
{
    #[cfg_attr(feature="flame_it", flame)]
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
            receiver: receiver.clone(),
            p_sender,
            p_receiver: p_receiver.clone(),
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

    #[cfg_attr(feature="flame_it", flame)]
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
            receiver: receiver.clone(),
            p_sender,
            p_receiver: p_receiver.clone(),
            id: id,
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
    #[cfg_attr(feature="flame_it", flame)]
    fn consume(&mut self) {
        self.sender.send(ConsumerMessage::Consume)
            .expect("Underlying consumer has died");
    }

    #[cfg_attr(feature="flame_it", flame)]
    fn throttle(&mut self, how_long: Duration) {
        self.p_sender.send(ConsumerMessage::Throttle {how_long})
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
    id: String
}

impl ConsumerBroker
{
    #[cfg_attr(feature="flame_it", flame)]
    pub fn new<T, C, F>(new: F,
                         worker_count: usize,
                         max_queue_depth: T)
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
            id
        }
    }

    pub fn add_consumer(&mut self) {
        if self.worker_count < 50 {
            self.workers.push(self.new.clone());
            self.worker_count += 1;
        }
        debug!(slog_scope::logger(), "Adding consumer: {}", self.worker_count);
    }

    pub fn drop_consumer(&mut self) {
        if self.worker_count > 1 {
            self.workers.pop();
            self.worker_count -= 1;
        }
        debug!(slog_scope::logger(), "Dropping consumer: {}", self.worker_count);
    }

    pub fn shut_down(&mut self) {
        self.workers.clear();
        self.worker_count = 0;
    }

    pub fn shut_down_blocking(&mut self) {
        self.workers.clear();
        self.worker_count = 0;
    }

    #[cfg_attr(feature="flame_it", flame)]
    pub fn consume(&self) {
        self.sender.send(
            ConsumerMessage::Consume
        ).unwrap();
    }

    #[cfg_attr(feature="flame_it", flame)]
    pub fn throttle(&self, how_long: Duration) {
        self.sender.send(
            ConsumerMessage::Throttle {how_long}
        ).unwrap();
    }

}

#[derive(Clone, Default)]
pub struct ConsumerThrottler {
    consumer_broker: Option<ConsumerBroker>,
}

impl ConsumerThrottler {
    pub fn new()
    -> ConsumerThrottler {
        ConsumerThrottler {
            consumer_broker: None,
        }
    }

    pub fn throttle(&self, how_long: Duration) {
        match self.consumer_broker {
            Some(ref consumer_broker) => {
                for _ in 0..consumer_broker.worker_count {
                    consumer_broker.throttle(how_long)
                }
            },
            None    => error!(slog_scope::logger(), "No consumer registered with ConsumerThrottler")
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
            ConsumerThrottlerMessage::Throttle {how_long} => self.throttle(how_long),
            ConsumerThrottlerMessage::RegisterconsumerBroker {consumer} => self.register_consumer(consumer),
            ConsumerThrottlerMessage::DropConsumer => self.drop_consumer(),
            ConsumerThrottlerMessage::AddConsumer => self.add_consumer(),
        }
    }
}

pub enum ConsumerThrottlerMessage
{
    Throttle {how_long: Duration},
    RegisterconsumerBroker {consumer: ConsumerBroker },
    DropConsumer,
    AddConsumer,
}

#[derive(Clone)]
pub struct ConsumerThrottlerActor
{
    sender: Sender<ConsumerThrottlerMessage>,
    receiver: Receiver<ConsumerThrottlerMessage>,
    p_sender: Sender<ConsumerThrottlerMessage>,
    p_receiver: Receiver<ConsumerThrottlerMessage>,
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
                    if let Ok(msg) = recvr.try_recv() {
                        actor.route_msg(msg);
                    }

                    match recvr.recv_timeout(Duration::from_secs(30)) {
                        Ok(msg) => {
                            actor.route_msg(msg);
                        }
                        Err(RecvTimeoutError::Disconnected) => {
                            break
                        }
                        Err(RecvTimeoutError::Timeout) => {
                        }
                    }
                }
            });

        ConsumerThrottlerActor {
            sender: sender.clone(),
            receiver: receiver.clone(),
            p_sender: p_sender.clone(),
            p_receiver: p_receiver.clone(),
            id: id,
        }
    }

    pub fn register_consumer(&self, consumer: ConsumerBroker) {
        self.sender.send(ConsumerThrottlerMessage::RegisterconsumerBroker {consumer})
            .expect("ConsumerThrottlerActor.register_consumer receivers have died");
    }

    pub fn throttle(&self, how_long: Duration) {
        self.p_sender.send(ConsumerThrottlerMessage::Throttle {how_long})
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


    #[test]
    pub fn test_consumer() {
        let mock_consumer = MockConsumer::new();
        let mock_state_manager = MockMessageStateManager::new();

        let mock_processor_broker = MessageHandlerBroker::new(
            move |_| {
                MockProcessor::new()
            },
            1,
            None,
            mock_state_manager.clone(),
            None
        );

        let throttler = MockThrottler::new();

        let mut consumer = DelayMessageConsumer::new(
            Arc::new(new_sqs_client()),
            "".to_owned(),
            mock_consumer.clone(),
            mock_state_manager.clone(),
            mock_processor_broker.clone(),
            throttler.clone()
        );

        consumer.consume();
        consumer.shut_down();

        thread::sleep(Duration::from_secs(2));

        let msg = mock_state_manager.receiver.try_recv().unwrap();
        let msg = match msg {
            m @ MessageStateManagerMessage::RegisterVariant {..} => m,
            m   => panic!("Expected registration of consumer message, got {:#?}",
                          m)
        };
    }
}