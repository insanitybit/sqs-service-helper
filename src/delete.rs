use two_lock_queue::{unbounded, Sender, Receiver, RecvTimeoutError, channel};
use uuid;

use util::*;
use std::time::{Instant, Duration};
use std::collections::HashMap;
use rusoto_sqs::{Sqs, DeleteMessageBatchRequest, DeleteMessageBatchRequestEntry};
use slog::Logger;
use std::sync::Arc;
use arrayvec::ArrayVec;
use std::iter::Iterator;
use std::thread;

#[derive(Clone)]
pub struct MessageDeleter<SQ>
    where SQ: Sqs + Send + Sync + 'static,
{
    sqs_client: Arc<SQ>,
    queue_url: String,
    logger: Logger
}

impl<SQ> MessageDeleter<SQ>
    where SQ: Sqs + Send + Sync + 'static,
{
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn new(sqs_client: Arc<SQ>, queue_url: String, logger: Logger) -> MessageDeleter<SQ> {
        MessageDeleter {
            sqs_client,
            queue_url,
            logger
        }
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn delete_messages(&self, receipts: Vec<(String, Instant)>) {
        let msg_count = receipts.len();

        if msg_count == 0 {
            return;
        }

        let mut receipt_init_map = HashMap::with_capacity(receipts.len());

        for (receipt, time) in receipts {
            receipt_init_map.insert(receipt, time);
        }

        let entries = receipt_init_map.keys().map(|r| {
            DeleteMessageBatchRequestEntry {
                id: format!("{}", uuid::Uuid::new_v4()),
                receipt_handle: r.to_owned()
            }
        }).collect();

        debug!(self.logger, "Deleting {} messages", msg_count);

        let req = DeleteMessageBatchRequest {
            entries,
            queue_url: self.queue_url.to_owned()
        };

        let mut backoff = 0;

        loop {
            match self.sqs_client.delete_message_batch(&req) {
                Ok(res) => {
                    let now = Instant::now();

                    for failed in res.failed {
                        println!("Failed to delete message {}", failed.message.unwrap_or(failed.code))
                    }

                    for init_time in receipt_init_map.values() {
                        let dur = now - *init_time;

                        let dur = millis(dur);
                        debug!(self.logger, "Took {}ms to process message to deletion", dur)
                    }
                    break
                }
                Err(e) => {
                    if backoff >= 5 {
                        warn!(self.logger, "Failed to deleted {} messages {}", msg_count, e);
                        break
                    }
                    backoff += 1;
                }
            }
        }

        //        for receipt in receipt_init_map.drain().map(|(k, _)| k) {
        //
        //        }
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn route_msg(&self, msg: MessageDeleterMessage) {
        match msg {
            MessageDeleterMessage::DeleteMessages { receipts } => self.delete_messages(receipts)
        }
    }
}

#[derive(Debug)]
pub enum MessageDeleterMessage {
    DeleteMessages {
        receipts: Vec<(String, Instant)>,
    },
}

#[derive(Clone)]
pub struct MessageDeleterActor {
    sender: Sender<MessageDeleterMessage>,
    id: String,
}

impl MessageDeleterActor {
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn new<SQ>(actor: MessageDeleter<SQ>) -> MessageDeleterActor
        where SQ: Sqs + Send + Sync + 'static,
    {
        let (sender, receiver) = unbounded();
        let id = uuid::Uuid::new_v4().to_string();

        thread::spawn(
            move || {
                loop {
                    if receiver.len() > 1000 {
                        warn!(actor.logger, "MessageDeleterActor queue depth {}", receiver.len());
                    }
                    match receiver.recv_timeout(Duration::from_secs(60)) {
                        Ok(msg) => {
                            actor.route_msg(msg);
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

        MessageDeleterActor {
            sender,
            id,
        }
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn from_queue<SQ, F>(
        new: &F,
        sender: Sender<MessageDeleterMessage>,
        receiver: Receiver<MessageDeleterMessage>,
        logger: Logger)
        -> MessageDeleterActor
        where SQ: Sqs + Send + Sync + 'static,
              F: Fn(MessageDeleterActor) -> MessageDeleter<SQ>,
    {
        let id = uuid::Uuid::new_v4().to_string();

        let actor = MessageDeleterActor {
            sender: sender.clone(),
            id,
        };

        let mut _actor = new(actor.clone());

        thread::spawn(
            move || {
                loop {
                    if receiver.len() > 1000 {
                        warn!(logger, "MessageDeleterActor queue depth {}", receiver.len());
                    }
                    match receiver.recv_timeout(Duration::from_secs(60)) {
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
    pub fn delete_messages(&self, receipts: Vec<(String, Instant)>) {
        self.sender.send(
            MessageDeleterMessage::DeleteMessages { receipts }
        ).unwrap();
    }
}

#[derive(Clone)]
pub struct MessageDeleterBroker
{
    workers: Vec<MessageDeleterActor>,
    sender: Sender<MessageDeleterMessage>,
    id: String
}

impl MessageDeleterBroker
{
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn new<T, F, SQ>(new: F,
                         worker_count: usize,
                         max_queue_depth: T,
                         logger: Logger)
                         -> MessageDeleterBroker
        where F: Fn(MessageDeleterActor) -> MessageDeleter<SQ>,
              T: Into<Option<usize>>,
              SQ: Sqs + Send + Sync + 'static,
    {
        let id = uuid::Uuid::new_v4().to_string();

        let (sender, receiver) = max_queue_depth.into().map_or(unbounded(), channel);

        let workers = (0..worker_count)
            .map(|_| MessageDeleterActor::from_queue(&new,
                                                     sender.clone(),
                                                     receiver.clone(),
                                                     logger.clone()))
            .collect();

        MessageDeleterBroker {
            workers,
            sender,
            id
        }
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn delete_messages(&self, receipts: Vec<(String, Instant)>) {
        self.sender.send(
            MessageDeleterMessage::DeleteMessages { receipts }
        ).unwrap();
    }
}

pub struct MessageDeleteBuffer {
    deleter_broker: MessageDeleterBroker,
    buffer: ArrayVec<[(String, Instant); 10]>,
    flush_period: Duration,
    logger: Logger,
}

impl MessageDeleteBuffer {
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn new(deleter_broker: MessageDeleterBroker,
               flush_period: Duration,
               logger: Logger) -> MessageDeleteBuffer
    {
        MessageDeleteBuffer {
            deleter_broker: deleter_broker,
            buffer: ArrayVec::new(),
            flush_period,
            logger
        }
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn delete_message(&mut self, receipt: String, init_time: Instant) {
        if self.buffer.is_full() {
            self.flush();
        }

        self.buffer.push((receipt, init_time));
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn flush(&mut self) {
        self.deleter_broker.delete_messages(Vec::from(self.buffer.as_ref()));
        self.buffer.clear();
    }

    fn route_msg(&mut self, msg: MessageDeleteBufferMessage) {
        match msg {
            MessageDeleteBufferMessage::Delete {receipt, init_time} => self.delete_message(receipt, init_time),
            MessageDeleteBufferMessage::Flush {} => self.flush(),
        }
    }
}

#[derive(Debug)]
pub enum MessageDeleteBufferMessage {
    Delete {
        receipt: String,
        init_time: Instant
    },
    Flush {},
}

#[derive(Clone)]
pub struct MessageDeleteBufferActor {
    sender: Sender<MessageDeleteBufferMessage>,
    id: String,
}

impl MessageDeleteBufferActor {
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn new
    (actor: MessageDeleteBuffer)
     -> MessageDeleteBufferActor
    {
        let mut actor = actor;
        let (sender, receiver) = unbounded();
        let id = uuid::Uuid::new_v4().to_string();

        thread::spawn(
            move || {
                // Avoid sending a flush message if we haven't received any other message type
                let mut flushable = false;

                let mut first_of_buffer = Instant::now();
                let period = millis(actor.flush_period) / 10;
                loop {
                    match receiver.recv_timeout(Duration::from_millis(period)) {
                        Ok(msg) => {

                            match msg {
                                MessageDeleteBufferMessage::Flush {} => {
                                    if flushable {
                                        actor.route_msg(msg)
                                    }
                                    flushable = false;
                                },
                                MessageDeleteBufferMessage::Delete {..} => {
                                    actor.route_msg(msg);
                                    if !flushable {
                                        first_of_buffer = Instant::now();
                                        flushable = true;
                                    } else {
                                        let now = Instant::now();

                                        if now - first_of_buffer >= actor.flush_period {
                                            actor.route_msg(MessageDeleteBufferMessage::Flush {});
                                            flushable = false;
                                        }
                                    }
                                }
                            }

                        }
                        Err(RecvTimeoutError::Disconnected) => {
                            break
                        }
                        Err(RecvTimeoutError::Timeout) => {
                            let now = Instant::now();

                            if now - first_of_buffer >= actor.flush_period {
                                actor.route_msg(MessageDeleteBufferMessage::Flush {});
                            }
                            continue
                        }
                    }
                }
            });

        MessageDeleteBufferActor {
            sender,
            id,
        }
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn delete_message(&self, receipt: String, init_time: Instant) {
        let msg = MessageDeleteBufferMessage::Delete {
            receipt,
            init_time
        };
        self.sender.send(msg).expect("MessageDeleteBufferActor :All receivers have died.");
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn flush(&self) {
        let msg = MessageDeleteBufferMessage::Flush {};
        self.sender.send(msg).expect("MessageDeleteBufferActor.flush :All receivers have died.");
    }
}