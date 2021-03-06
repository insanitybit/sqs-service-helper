#![allow(unreachable_code)]

use delete::*;
use autoscaling::*;
use util::millis;
use arrayvec::ArrayVec;
use uuid;
use lru_time_cache::LruCache;
use rusoto_sqs::{Sqs, ChangeMessageVisibilityBatchRequestEntry, ChangeMessageVisibilityBatchRequest};

use slog::Logger;
use two_lock_queue::{unbounded, Sender, Receiver, RecvTimeoutError, channel};

use std::sync::{Arc, Mutex};
use std::sync::mpsc::{Sender as MpscSender, channel as mpsc_channel};
use std::iter::{Iterator, FromIterator};
use std::thread;
use std::time::{Instant, Duration};
use std::collections::HashMap;


pub trait MessageStateManager {
    fn register(&mut self, String, Duration, Instant);
    fn deregister(&mut self, String, bool);
    fn route_msg(&mut self, MessageStateManagerMessage);
}

/// The `MessageStateManager` manages the local message's state in the SQS service. That is, it will
/// handle maintaining the messages visibility, and it will handle deleting the message
/// Anything actors that may impact this state should likely be invoked or managed by this actor
pub struct SqsMessageStateManager
{
    timers: LruCache<String, (VisibilityTimeoutActor, Instant)>,
    buffer: VisibilityTimeoutExtenderBufferActor,
    deleter: MessageDeleteBufferActor,
    logger: Logger
}

impl SqsMessageStateManager
{
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn new(buffer: VisibilityTimeoutExtenderBufferActor,
               deleter: MessageDeleteBufferActor,
               logger: Logger) -> SqsMessageStateManager
    {
        // Create the new MessageStateManager with a maximum cache  lifetime of 12 hours, which is
        // the maximum amount of time a message can be kept invisible
        SqsMessageStateManager {
            timers: LruCache::with_expiry_duration_and_capacity(Duration::from_secs(12 * 60 * 60), 120_000),
            buffer,
            deleter,
            logger
        }
    }
}

impl MessageStateManager for SqsMessageStateManager {
    #[cfg_attr(feature = "flame_it", flame)]
    fn register(&mut self, receipt: String, visibility_timeout: Duration, start_time: Instant) {
        let vis_timeout = VisibilityTimeout::new(self.buffer.clone(), self.deleter.clone(), receipt.clone());
        let vis_timeout = VisibilityTimeoutActor::new(vis_timeout, self.logger.clone());
        vis_timeout.start(visibility_timeout, start_time);

        self.timers.insert(receipt, (vis_timeout, start_time));
    }

    #[cfg_attr(feature = "flame_it", flame)]
    fn deregister(&mut self, receipt: String, should_delete: bool) {
        let vis_timeout = self.timers.remove(&receipt);

        match vis_timeout {
            Some((vis_timeout, _)) => {
                vis_timeout.end(should_delete);
            }
            None => {
                warn!(self.logger, "Attempting to deregister timeout that does not exist:\
                receipt: {} should_delete: {}", receipt, should_delete);
            }
        };
    }

    #[cfg_attr(feature = "flame_it", flame)]
    fn route_msg(&mut self, msg: MessageStateManagerMessage) {
        match msg {
            MessageStateManagerMessage::RegisterVariant {
                receipt, visibility_timeout, start_time
            } =>
                self.register(receipt, visibility_timeout, start_time),
            MessageStateManagerMessage::DeregisterVariant { receipt, should_delete } => {
                self.deregister(receipt, should_delete)
            }
        };
    }
}

#[derive(Debug)]
pub enum MessageStateManagerMessage {
    RegisterVariant {
        receipt: String,
        visibility_timeout: Duration,
        start_time: Instant
    },
    DeregisterVariant { receipt: String, should_delete: bool },
}

#[derive(Clone)]
pub struct MessageStateManagerActor {
    pub sender: MpscSender<MessageStateManagerMessage>,
    id: String,
}

impl MessageStateManagerActor {
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn new<M>(actor: M)
                  -> MessageStateManagerActor
        where M: MessageStateManager + Send + 'static
    {
        let mut actor = actor;
        let (sender, receiver) = mpsc_channel();
        let id = uuid::Uuid::new_v4().to_string();

        thread::spawn(
            move || {
                loop {
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

        MessageStateManagerActor {
            sender,
            id,
        }
    }
}

impl MessageStateManager for MessageStateManagerActor {
    #[cfg_attr(feature = "flame_it", flame)]
    fn register(&mut self, receipt: String, visibility_timeout: Duration, start_time: Instant) {
        let msg = MessageStateManagerMessage::RegisterVariant {
            receipt,
            visibility_timeout,
            start_time
        };
        self.sender.send(msg).expect("MessageStateManagerActor.register : All receivers have died.");
    }

    #[cfg_attr(feature = "flame_it", flame)]
    fn deregister(&mut self, receipt: String, should_delete: bool) {
        let msg = MessageStateManagerMessage::DeregisterVariant { receipt, should_delete };
        self.sender.send(msg).expect("MessageStateManagerActor.deregister : All receivers have died.");
    }

    fn route_msg(&mut self, msg: MessageStateManagerMessage) {
        self.sender.send(msg).unwrap();
    }
}

// VisibilityTimeout emits events to VisibilityTimeoutExtenders when a messagActore
// needs its visibility timeout extended. Upon receiving a kill message it will
// stop emitting these events.
#[derive(Clone)]
pub struct VisibilityTimeout
{
    pub buf: VisibilityTimeoutExtenderBufferActor,
    pub receipt: String,
    pub deleter: MessageDeleteBufferActor
}

impl VisibilityTimeout
{
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn new(buf: VisibilityTimeoutExtenderBufferActor,
               deleter: MessageDeleteBufferActor,
               receipt: String) -> VisibilityTimeout {
        VisibilityTimeout {
            buf,
            receipt,
            deleter
        }
    }
}

#[derive(Debug)]
pub enum VisibilityTimeoutMessage {
    StartVariant {
        init_timeout: Duration,
        start_time: Instant
    },
    EndVariant {
        should_delete: bool
    },
}

#[derive(Clone)]
pub struct VisibilityTimeoutActor {
    sender: MpscSender<VisibilityTimeoutMessage>,
    id: String,
}

impl VisibilityTimeoutActor {
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn new(actor: VisibilityTimeout, logger: Logger) -> VisibilityTimeoutActor
    {
        let (sender, receiver) = mpsc_channel();
        let id = uuid::Uuid::new_v4().to_string();

        thread::spawn(move || {
            let mut dur = Duration::from_secs(30); // Default, minimal timeout
            let mut _start_time = None;

            loop {
                let actor = actor.clone();
                let receipt = actor.receipt.clone();
                let res = receiver.recv_timeout(dur / 2);
                match res {
                    Ok(msg) => {
                        match msg {
                            VisibilityTimeoutMessage::StartVariant { init_timeout, start_time } => {
                                actor.buf.extend(receipt.clone(), init_timeout, start_time, false);

                                dur = init_timeout;
                                _start_time = Some(start_time);
                            }
                            VisibilityTimeoutMessage::EndVariant { should_delete }
                            => {
                                match _start_time {
                                    Some(st) => {
                                        actor.buf.extend(receipt.clone(), dur, st, should_delete);
                                    }
                                    None => {
                                        error!(logger, "Error, no start time provided")
                                    }
                                }
                                return;
                            }
                        }
                    }
                    Err(_) => {
                        let dur = match dur.checked_mul(2) {
                            Some(d) => d,
                            None => dur
                        };

                        match _start_time {
                            Some(st) => {
                                actor.buf.extend(receipt.clone(), dur, st, false);
                            }
                            None => {
                                error!(logger, "No start time provided")
                            }
                        }
                    }
                }
            }
        });

        VisibilityTimeoutActor {
            sender,
            id,
        }
    }

    // 'start' sets up the VisibilityTimeout with its initial timeout
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn start(&self, init_timeout: Duration, start_time: Instant) {
        self.sender.send(VisibilityTimeoutMessage::StartVariant {
            init_timeout,
            start_time
        }).expect("All receivers have died: VisibilityTimeoutMessage::StartVariant");
    }

    // 'end' stops the VisibilityTimeout from emitting any more events
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn end(&self, should_delete: bool) {
        self.sender.send(VisibilityTimeoutMessage::EndVariant { should_delete })
            .expect("All receivers have died: VisibilityTimeoutMessage::EndVariant");
    }
}


#[derive(Clone)]
pub struct VisibilityTimeoutExtenderBroker
{
    workers: Vec<VisibilityTimeoutExtenderActor>,
    sender: Sender<VisibilityTimeoutExtenderMessage>,
    id: String
}

impl VisibilityTimeoutExtenderBroker
{
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn new<T, SQ, F>(new: F,
                         worker_count: usize,
                         max_queue_depth: T,
                         logger: Logger)
                         -> VisibilityTimeoutExtenderBroker
        where SQ: Sqs + Send + Sync + 'static,
              F: Fn(VisibilityTimeoutExtenderActor) -> VisibilityTimeoutExtender<SQ>,
              T: Into<Option<usize>>,
    {
        let id = uuid::Uuid::new_v4().to_string();

        let (sender, receiver) = max_queue_depth.into().map_or(unbounded(), channel);

        let workers = (0..worker_count)
            .map(|_| VisibilityTimeoutExtenderActor::from_queue(&new,
                                                                sender.clone(),
                                                                receiver.clone(),
                                                                logger.clone()))
            .collect();

        VisibilityTimeoutExtenderBroker {
            workers,
            sender,
            id
        }
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn extend(&self, timeout_info: Vec<(String, Duration, Instant, bool)>) {
        self.sender.send(
            VisibilityTimeoutExtenderMessage::ExtendVariant {
                timeout_info
            }
        ).unwrap();
    }
}

// The VisibilityTimeoutExtenderBuffer is a 'broker' of VisibilityTimeoutExtenders. It will buffer
// messages into chunks, and send those chunks to the VisibilityTimeoutExtender
// It will buffer messages for some amount of time, or until it receives 10 messages
// in an effort to perform bulk API calls
#[derive(Clone)]
pub struct VisibilityTimeoutExtenderBuffer
{
    extender_broker: VisibilityTimeoutExtenderBroker,
    // Replace this with a Broker to do proper work stealing
    buffer: ArrayVec<[(String, Duration, Instant, bool); 10]>,
    last_flush: Instant,
    flush_period: Duration,
    short_circuit: Option<Arc<Mutex<LruCache<String, ()>>>>
}

impl VisibilityTimeoutExtenderBuffer

{
    // u8::MAX is just over 4 minutes
    // Highly suggest keep the number closer to 10s of seconds at most.
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn new(extender_broker: VisibilityTimeoutExtenderBroker,
               flush_period: Duration,
               short_circuit: Option<Arc<Mutex<LruCache<String, ()>>>>)
               -> VisibilityTimeoutExtenderBuffer

    {
        VisibilityTimeoutExtenderBuffer {
            extender_broker,
            buffer: ArrayVec::new(),
            last_flush: Instant::now(),
            flush_period,
            short_circuit
        }
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn extend(&mut self, receipt: String, timeout: Duration, start_time: Instant, should_delete: bool) {
        if self.buffer.is_full() {
            if let Some(ref sc) = self.short_circuit {
                let mut short_circuit = sc.lock().unwrap();
                let now = Instant::now();

                let shorted_buffer: Vec<_> = self.buffer.iter()
                    .cloned()
                    .filter_map(|(receipt, d, instant, should_delete)| {
                        if !should_delete && now - instant < Duration::from_secs(26) {
                            if short_circuit.get(&receipt).is_some() {
                                None
                            } else {
                                Some((receipt, d, instant, should_delete))
                            }
                        } else {
                            Some((receipt, d, instant, should_delete))
                        }
                    }).collect();

                self.buffer = ArrayVec::from_iter(shorted_buffer.into_iter());
            }

            if self.buffer.is_full() {
                self.flush();
            }
        }

        self.buffer.push((receipt, timeout, start_time, should_delete));
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn flush(&mut self) {
        if self.buffer.len() != 0 {
            self.extender_broker.extend(Vec::from(self.buffer.as_ref()));
            self.buffer.clear();
        }
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn on_timeout(&mut self) {
        if self.buffer.len() != 0 {
            self.flush();
        }
    }
}

pub fn get_short_circuit() -> Arc<Mutex<LruCache<String, ()>>> {
    Arc::new(
        Mutex::new(
            LruCache::with_expiry_duration_and_capacity(
                Duration::from_secs(12 * 60 * 60),
                120_000
            )
        )
    )
}

#[derive(Debug)]
pub enum VisibilityTimeoutExtenderBufferMessage {
    Extend { receipt: String, timeout: Duration, start_time: Instant, should_delete: bool },
    Flush {},
}

#[derive(Clone)]
pub struct VisibilityTimeoutExtenderBufferActor {
    sender: Sender<VisibilityTimeoutExtenderBufferMessage>,
    id: String,
}

impl VisibilityTimeoutExtenderBufferActor {
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn new(actor: VisibilityTimeoutExtenderBuffer, logger: Logger)
               -> VisibilityTimeoutExtenderBufferActor
    {
        let id = uuid::Uuid::new_v4().to_string();

        let (sender2, receiver2) = unbounded();
        let recvr2 = receiver2.clone();
        let mut actor = actor.clone();


        thread::spawn(
        move || {
            // Avoid sending a flush message if we haven't received any other message type
            let mut flushable = false;

            let mut first_of_buffer = Instant::now();
            let period = millis(actor.flush_period) / 10;
            loop {
                let logger = logger.clone();
                if recvr2.len() > 1000 {
                    warn!(logger, "VisibiltyTimeoutExtenderBufferActor broker queue depth {}", recvr2.len());
                }
                match recvr2.recv_timeout(Duration::from_millis(period)) {
                        Ok(msg) => {

                            match msg {
                                    VisibilityTimeoutExtenderBufferMessage::Flush {} => {
                                        if flushable {
                                            actor.route_msg(msg)
                                        }
                                        flushable = false;
                                    },
                                    VisibilityTimeoutExtenderBufferMessage::Extend {..} => {
                                        actor.route_msg(msg);
                                        if !flushable {
                                            first_of_buffer = Instant::now();
                                            flushable = true;
                                        } else {
                                            let now = Instant::now();

                                            if now - first_of_buffer >= actor.flush_period {
                                                actor.route_msg(VisibilityTimeoutExtenderBufferMessage::Flush {});
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
                                actor.route_msg(VisibilityTimeoutExtenderBufferMessage::Flush {});
                            }
                            continue
                        }
                }
            }
        });

        VisibilityTimeoutExtenderBufferActor {
            sender: sender2,
            id: id,
        }
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn extend(&self, receipt: String, timeout: Duration, start_time: Instant, should_delete: bool) {
        let msg = VisibilityTimeoutExtenderBufferMessage::Extend {
            receipt,
            timeout,
            should_delete,
            start_time,
        };
        self.sender.send(msg).expect("VisibilityTimeoutExtenderBufferActor.send: All receivers have died.");
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn flush(&self) {
        let msg = VisibilityTimeoutExtenderBufferMessage::Flush {};
        self.sender.send(msg).expect("VisibilityTimeoutExtenderBufferActor.flush All receivers have died.");
    }
}

impl VisibilityTimeoutExtenderBuffer
{
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn route_msg(&mut self, msg: VisibilityTimeoutExtenderBufferMessage) {
        match msg {
            VisibilityTimeoutExtenderBufferMessage::Extend {
                receipt,
                timeout,
                start_time,
                should_delete,
            } => {
                self.extend(receipt, timeout, start_time, should_delete)
            }
            VisibilityTimeoutExtenderBufferMessage::Flush {} => {
                self.flush()
            },
        };
    }
}

// VisibilityTimeoutExtender receives messages with receipts and timeout durations,
// and uses these to extend the timeout
// It will attempt to use bulk APIs where possible.
// It does not emit any events
#[derive(Clone)]
pub struct VisibilityTimeoutExtender<SQ>
    where SQ: Sqs + Send + Sync + 'static
{
    sqs_client: Arc<SQ>,
    queue_url: String,
    throttler: ThrottlerActor,
    deleter: MessageDeleteBufferActor,
    logger: Logger
}

impl<SQ> VisibilityTimeoutExtender<SQ>
    where SQ: Sqs + Send + Sync + 'static
{
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn new(sqs_client: Arc<SQ>,
               queue_url: String,
               deleter: MessageDeleteBufferActor,
               throttler: ThrottlerActor,
               logger: Logger)
               -> VisibilityTimeoutExtender<SQ>
    {
        VisibilityTimeoutExtender {
            sqs_client,
            queue_url,
            deleter,
            throttler,
            logger
        }
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn extend(&mut self, timeout_info: Vec<(String, Duration, Instant, bool)>) {
        let mut id_map = HashMap::with_capacity(timeout_info.len());

        let mut to_delete = vec![];
        let entries: Vec<_> = timeout_info.into_iter().filter_map(|(receipt, timeout, start_time, should_delete)| {
            let now = Instant::now();
            if start_time + timeout < now + Duration::from_millis(10) {
                error!(self.logger, "Message timeout expired before extend: start_time {:#?} timeout {:#?} now {:#?}", start_time, timeout, now);
                if should_delete {
                    to_delete.push((receipt.clone(), start_time));
                }
                return None;
            }

            if should_delete {
                to_delete.push((receipt.clone(), start_time));
                None
            } else {
                let id = format!("{}", uuid::Uuid::new_v4());

                id_map.insert(id.clone(), (receipt.clone(), timeout, start_time));

                Some(ChangeMessageVisibilityBatchRequestEntry {
                    id,
                    receipt_handle: receipt,
                    visibility_timeout: Some(timeout.as_secs() as i64)
                })
            }
        }).collect();

        if entries.is_empty() {
            for (receipt, init_time) in to_delete {
                self.deleter.delete_message(receipt, init_time);
            }
            return;
        }

        let req = ChangeMessageVisibilityBatchRequest {
            entries,
            queue_url: self.queue_url.clone()
        };

        let mut backoff = 0;
        loop {
            match self.sqs_client.change_message_visibility_batch(&req) {
                Ok(t) => {
                    if !t.failed.is_empty() {
                        let mut to_retry = Vec::with_capacity(t.failed.len());
                        for failed in t.failed.clone() {
                            to_retry.push(id_map[&failed.id].clone());
                        }
                        self.retry_extend(to_retry, 0);
                    }
                    trace!(self.logger, "Successfully updated visibilities for {} messages", t.successful.len());

                    for successful in t.successful {
                        let now = Instant::now();
                        let (ref receipt, _, _) = id_map[&successful.id];
                        self.throttler.message_stop(receipt.to_owned(), now);
                    }
                    break
                }
                Err(e) => {
                    backoff += 1;
                    thread::sleep(Duration::from_secs(20 * backoff));
                    if backoff > 5 {
                        warn!(self.logger, "Failed to change message visibility {}", e);
                        break
                    } else {
                        continue
                    }
                }
            };
        }

        for (receipt, init_time) in to_delete {
            self.deleter.delete_message(receipt, init_time);
        }
    }

    fn retry_extend(&mut self, timeout_info: Vec<(String, Duration, Instant)>, attempts: usize) {
        if attempts > 10 {
            warn!(self.logger, "Failed to retry_extend {} messages", timeout_info.len());
            return;
        }

        let mut id_map = HashMap::with_capacity(timeout_info.len());

        let entries: Vec<_> = timeout_info.into_iter().flat_map(|(receipt, timeout, start_time)| {
            let now = Instant::now();
            if start_time + timeout < now + Duration::from_millis(10) {
                error!(self.logger, "Message timeout expired before extend: start_time {:#?} timeout {:#?} now {:#?}", start_time, timeout, now);
                None
            } else {
                let id = format!("{}", uuid::Uuid::new_v4());
                id_map.insert(id.clone(), (receipt.clone(), timeout, start_time));

                Some(ChangeMessageVisibilityBatchRequestEntry {
                    id,
                    receipt_handle: receipt,
                    visibility_timeout: Some(timeout.as_secs() as i64)
                })
            }
        }).collect();

        if entries.is_empty() {
            return;
        }

        let req = ChangeMessageVisibilityBatchRequest {
            entries,
            queue_url: self.queue_url.clone()
        };

        let mut backoff = 0;
        loop {
            match self.sqs_client.change_message_visibility_batch(&req) {
                Ok(t) => {
                    if !t.failed.is_empty() {
                        let mut to_retry = Vec::with_capacity(t.failed.len());
                        for failed in t.failed.clone() {
                            to_retry.push(id_map[&failed.id].clone());
                        }
                        for successful in t.successful {
                            let now = Instant::now();
                            let (ref receipt, _, _) = id_map[&successful.id];
                            self.throttler.message_stop(receipt.to_owned(), now);
                        }
                        thread::sleep(Duration::from_millis(5 * attempts as u64 + 1));
                        self.retry_extend(to_retry, attempts + 1);
                    }
                    break
                }
                Err(e) => {
                    backoff += 1;
                    thread::sleep(Duration::from_millis(5 * backoff));
                    if backoff > 5 {
                        warn!(self.logger, "Failed to change message visibility {}", e);
                        break
                    } else {
                        continue
                    }
                }
            };
        }
    }
}

#[derive(Debug)]
pub enum VisibilityTimeoutExtenderMessage {
    ExtendVariant {
        timeout_info: Vec<(String, Duration, Instant, bool)>,
    },
}

#[derive(Clone)]
pub struct VisibilityTimeoutExtenderActor {
    sender: Sender<VisibilityTimeoutExtenderMessage>,
    id: String,
}

impl VisibilityTimeoutExtenderActor {
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn from_queue<SQ, F>(new: &F,
                             sender: Sender<VisibilityTimeoutExtenderMessage>,
                             receiver: Receiver<VisibilityTimeoutExtenderMessage>,
                             logger: Logger)
                             -> VisibilityTimeoutExtenderActor
        where SQ: Sqs + Send + Sync + 'static,
              F: Fn(VisibilityTimeoutExtenderActor) -> VisibilityTimeoutExtender<SQ>,
    {
        let id = uuid::Uuid::new_v4().to_string();

        let actor = VisibilityTimeoutExtenderActor {
            sender: sender.clone(),
            id,
        };

        let mut _actor = new(actor.clone());

        thread::spawn(
            move || {
                loop {
                    if receiver.len() > 1000 {
                        warn!(logger, "VisibilityTimeoutExtenderActor queue depth {}", receiver.len());
                    }
                    match receiver.recv_timeout(Duration::from_secs(60)) {
                        Ok(msg) => {
                            _actor.route_msg(msg);
                        }
                        Err(RecvTimeoutError::Disconnected) => {
                            break
                        }
                        Err(RecvTimeoutError::Timeout) => {}
                    }
                }
            });

        actor
    }


    #[cfg_attr(feature = "flame_it", flame)]
    pub fn new<SQ>(actor: VisibilityTimeoutExtender<SQ>) -> VisibilityTimeoutExtenderActor
        where SQ: Sqs + Send + Sync + 'static {
        let mut actor = actor;
        let (sender, receiver) = unbounded();
        let id = uuid::Uuid::new_v4().to_string();

        thread::spawn(
            move || {
                loop {
                    match receiver.recv_timeout(Duration::from_secs(60)) {
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

        VisibilityTimeoutExtenderActor {
            sender,
            id,
        }
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn extend(&self, timeout_info: Vec<(String, Duration, Instant, bool)>) {
        let msg = VisibilityTimeoutExtenderMessage::ExtendVariant { timeout_info };
        self.sender.send(msg).expect("VisibilityTimeoutExtenderActor.extend: All receivers have died.");
    }
}

impl<SQ> VisibilityTimeoutExtender<SQ>
    where SQ: Sqs + Send + Sync + 'static
{
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn route_msg(&mut self, msg: VisibilityTimeoutExtenderMessage) {
        match msg {
            VisibilityTimeoutExtenderMessage::ExtendVariant { timeout_info } => {
                self.extend(timeout_info)
            }
        };
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use mocks::*;
    #[cfg(feature = "flame_it")]
    use flame;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use processor::*;
    use consumer::*;
    use util;

    // Test is disabled while mocking is built out
    //    #[test]
    fn test_hundred() {
        let logger = util::init_logger("test_hundred.log");

        util::set_timer();

        let queue_url = "some queue url".to_owned();

        let sqs_client = Arc::new(new_sqs_client());

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

        let sns_client = Arc::new(new_sns_client());

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
        let buffer = VisibilityTimeoutExtenderBuffer::new(broker,
                                                          Duration::from_secs(2),
                                                          sc.clone());
        let buffer = VisibilityTimeoutExtenderBufferActor::new(buffer);

        let consumer_throttler = ConsumerThrottler::new(logger.clone());
        let consumer_throttler = ConsumerThrottlerActor::new(consumer_throttler);

        let state_manager = SqsMessageStateManager::new(buffer,
                                                        deleter.clone(),
                                                        logger.clone());

        let state_manager = MessageStateManagerActor::new(state_manager);

        let (processor_sender, processor_receiver) = unbounded();
        let sndr = processor_sender.clone();
        let rcvr = processor_receiver.clone();
        let processor = MessageHandlerBroker::new(
            move |_| {
                MockProcessor::from_queue(sndr.clone(),
                                          rcvr.clone())
            },
            1,
            None,
            state_manager.clone(),
            None,
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
        {
            // Remove all consumer threads
            sqs_broker.shut_down();

            loop {
                let count = sns_client.publishes.load(Ordering::Relaxed);

                if count >= 10 {
                    break
                }
            }
        }

        thread::sleep(Duration::from_secs(5));

        // Unfortunately things are a little racy so we can't assert 100 exactly
        assert!(sns_client.publishes.load(Ordering::Relaxed) >= 100);
    }

    #[test]
    fn test_deleter() {
        let logger = util::init_logger("test_deleter.log");

        util::set_timer();

        let queue_url = "some queue url".to_owned();

        let sqs_client = Arc::new(new_sqs_client());

        let deleter = MessageDeleter::new(sqs_client.clone(), queue_url.clone(), logger.clone());

        deleter.delete_messages(vec![("receipt1".to_owned(), Instant::now())]);

        thread::sleep(Duration::from_millis(5));

        assert_eq!(sqs_client.deletes.load(Ordering::Relaxed), 1);
    }
}

#[cfg(not(test))]
mod bench {
    #![allow(unused_imports)]

    use super::*;
    use test::Bencher;
    use xorshift::{Xoroshiro128, Rng, SeedableRng};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[bench]
    fn bench_time_to_register(b: &mut Bencher) {
        let mut median_tracker = StreamingMedian::new();

        b.iter(|| {
            median_tracker.insert_and_calculate(100);
        });
    }
}
