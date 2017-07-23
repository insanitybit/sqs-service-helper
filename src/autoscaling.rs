use arraydeque::ArrayDeque;
use lru_time_cache::LruCache;
use two_lock_queue::{Sender, Receiver, RecvTimeoutError, unbounded};
use uuid;

use std::cmp::{min, max, Ordering};
use std::iter::{self, FromIterator};
use std::time::{Instant, Duration};
use std::thread;
use std::mem::uninitialized;

use consumer::*;
use util::*;
use slog_scope;

type CurrentActors = usize;

pub enum ScaleMetric {
    QueueDepth {
        depth: usize,
        current_actor_count: usize
    },
    ProcessingTime {
        process_time: Duration,
        current_actor_count: usize
    },
    EmptyReceives {
        current_actor_count: usize
    }
}

pub enum ScaleMessage {
    Up(usize),
    Down(usize)
}

pub trait Scalable {
    fn scale(&mut self, scale: ScaleMessage);
}

pub struct ScalingPolicy
{
    /// Minimum number of actors to scale down to
    min_actors: usize,
    /// Maximum number of actors to scale up to
    max_actors: usize,
    /// The minimum duration between emitting scaling commands
    wait_time: Duration,
    /// Function to determine how to scale
    should_scale: Box<Fn(ScaleMetric) -> Option<ScaleMessage>>
}

impl ScalingPolicy
{
    pub fn new(min_actors: usize, max_actors: usize, wait_time: Duration, should_scale: Box<Fn(ScaleMetric) -> Option<ScaleMessage>>)
               -> ScalingPolicy {
        ScalingPolicy {
            min_actors,
            max_actors,
            wait_time,
            should_scale
        }
    }

    pub fn default_algorithm(min_actors: usize, max_actors: usize, wait_time: Duration)
                             -> ScalingPolicy {
        ScalingPolicy {
            min_actors,
            max_actors,
            wait_time,
            should_scale: Box::new(move |metric| {
                match metric {
                    ScaleMetric::EmptyReceives { current_actor_count } => {
                        if current_actor_count > min_actors {
                            Some(ScaleMessage::Down(current_actor_count - 1))
                        } else {
                            None
                        }
                    }
                    ScaleMetric::ProcessingTime { process_time, current_actor_count } => {
                        if current_actor_count < max_actors && process_time > Duration::from_secs(2) {
                            Some(ScaleMessage::Up(current_actor_count + 1))
                        } else {
                            None
                        }
                    }
                    ScaleMetric::QueueDepth { depth, current_actor_count } => {
                        if current_actor_count < max_actors {
                            if depth > 1000 {
                                Some(ScaleMessage::Up(max_actors))
                            } else if depth > 100 {
                                let new_count = min(current_actor_count + 2, max_actors);
                                Some(ScaleMessage::Up(new_count))
                            } else if depth > 10 {
                                Some(ScaleMessage::Up(current_actor_count + 1))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                }
            })
        }
    }
}

pub struct AutoScaler<A>
    where A: Scalable,
{
    /// The actor to communicate scaling commands to
    scalable: A,
    /// The policy to determine how to scale
    policy: ScalingPolicy
}

impl<A> AutoScaler<A>
    where A: Scalable,
{
    pub fn new(scalable: A, policy: ScalingPolicy) -> AutoScaler<A> {
        AutoScaler {
            scalable,
            policy
        }
    }
}

enum ThrottlerMessage {
    MessageStart {
        receipt: String,
        time_started: Instant
    },
    MessageStop {
        receipt: String,
        time_stopped: Instant,
        success: bool
    },
    RegisterConsumerThrottler {
        throttler: ConsumerThrottlerActor
    }
}

#[derive(Clone)]
pub struct ThrottlerActor
{
    sender: Sender<ThrottlerMessage>,
    receiver: Receiver<ThrottlerMessage>,
    id: String
}

/// Throttler tracks the processing time for messages from the consumer to the
/// deleter. It then uses this information to calculate how many messages we can
/// process in 30 seconds, the time it takes for a message to time out. Ideally
/// we process as many messages as we can while ensuring that we don't back our
/// message visibility handlers up, which could lead to more frequent double
/// processing.
pub struct Throttler {
    throttler: Option<ConsumerThrottlerActor>,
    inflight_timings: LruCache<String, Instant>,
    proc_times: StreamingMedian,
    inflight_limit: usize,
}

impl Throttler {
    pub fn new() -> Throttler {
        let inflight_timings =
            LruCache::with_expiry_duration(Duration::from_secs(12 * 60 * 60));
        let proc_times = StreamingMedian::new();
        Throttler {
            throttler: None,
            inflight_timings,
            proc_times,
            inflight_limit: 10,
        }
    }

    pub fn message_start(&mut self, receipt: String, time_started: Instant) {
        if self.inflight_timings.insert(receipt, time_started).is_some() {
            error!(slog_scope::logger(), "Message starting twice");
        }

        let processing_time = self.proc_times.last() as u64;

        // If we have more inflight messages than our limit...
        if self.inflight_timings.len() > self.inflight_limit {
            if self.inflight_timings.len() > self.inflight_limit * 10 {
                for _ in 0..50 {
                    self.throttler.as_ref().map(|t| t.drop_consumer());
                }
            } else if self.inflight_timings.len() > self.inflight_limit * 2 {
                self.throttler.as_ref().map(|t| t.drop_consumer());
                self.throttler.as_ref().map(|t| t.throttle(
                    Duration::from_millis(self.inflight_timings.len() as u64 * processing_time)
                ));
            } else {
                self.throttler.as_ref().map(|t| t.throttle(
                    Duration::from_millis(self.inflight_timings.len() as u64 * processing_time)
                ));
            }
        } else if self.inflight_timings.len() as f64 > self.inflight_limit as f64 * 0.95 {
            self.throttler.as_ref().map(|t| t.drop_consumer());
            self.throttler.as_ref().map(|t| t.throttle(
                Duration::from_millis(processing_time)
            ));
        } else if self.inflight_timings.len() as f64 > self.inflight_limit as f64 * 0.85 {
            self.throttler.as_ref().map(|t| t.throttle(
                Duration::from_millis(processing_time)
            ));
        } else if (self.inflight_timings.len() as f64) > self.inflight_limit as f64 * 0.55 {
            self.throttler.as_ref().map(|t| t.add_consumer());
            self.throttler.as_ref().map(|t| t.throttle(
                Duration::from_millis(processing_time)
            ));
        } else {
            self.throttler.as_ref().map(|t| t.add_consumer());
            self.throttler.as_ref().map(|t| t.add_consumer());
            self.throttler.as_ref().map(|t| t.throttle(
                Duration::from_millis(processing_time / 2)
            ));
        }
    }

    pub fn message_stop(&mut self, receipt: String, time_stopped: Instant, success: bool) {
        // Get the time that the messagae was started
        let start_time = self.inflight_timings.remove(&receipt);

        match start_time {
            // We are only interested in timings for successfully processed messages, at least
            // in part because this is likely the slowest path and we want to throttle accordingly
            Some(start_time) if success => {
                // Calculate the time it took from message consumption to delete
                let proc_time = millis(time_stopped - start_time) as u32;

                let median = self.proc_times.insert_and_calculate(proc_time);
                let last_limit = self.inflight_limit;
                // Recalculate the maximum backlog based on our most recent processing times
                let new_max = self.get_max_backlog(Duration::from_secs(28), median);
                self.inflight_limit = if new_max == 0 {
                    self.inflight_limit + 1
                } else {
                    new_max as usize
                };
                
                if last_limit != self.inflight_limit {
                    debug!(slog_scope::logger(),
                           "Setting new inflight limit to : {} from {}",
                           self.inflight_limit, last_limit);
                }

                self.inflight_limit = min(self.inflight_limit, 120_000);
            }
            _ => {
                warn!(slog_scope::logger(),
                      "Attempting to deregister timeout that does not exist:\
                receipt: {} success: {}", receipt, success);
            }
        };
    }

    pub fn register_consumer_throttler(&mut self,
                                       consumer_throttler: ConsumerThrottlerActor)
    {
        self.throttler = Some(consumer_throttler);
    }

    // Given a timeout of n seconds, and our current processing times,
    // what is the number of messages we can process within that timeout
    fn get_max_backlog(&self, dur: Duration, proc_time: u32) -> u64 {
        let max_ms = millis(dur) as u32;

        if proc_time == 0 {
            return 0;
        }

        let max_msgs = max_ms / proc_time;

        max(max_msgs, 1) as u64
    }

    fn route_msg(&mut self, msg: ThrottlerMessage) {
        match msg {
            ThrottlerMessage::MessageStart { receipt, time_started } =>
                self.message_start(receipt, time_started),
            ThrottlerMessage::MessageStop { receipt, time_stopped, success } =>
                self.message_stop(receipt, time_stopped, success),
            ThrottlerMessage::RegisterConsumerThrottler { throttler } =>
                self.register_consumer_throttler(throttler)
        }
    }

    pub fn get_inflight_limit(&self) -> usize {
        self.inflight_limit
    }
}

impl Default for Throttler {
    fn default() -> Self {
        Self::new()
    }
}

impl ThrottlerActor
{
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn new(actor: Throttler)
               -> ThrottlerActor
    {
        let (sender, receiver) = unbounded();
        let id = uuid::Uuid::new_v4().to_string();

        let mut actor = actor;

        let recvr = receiver.clone();
        thread::spawn(
            move || {
                loop {
                    match recvr.recv_timeout(Duration::from_secs(30)) {
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

        ThrottlerActor {
            sender: sender.clone(),
            receiver: receiver.clone(),
            id: id,
        }
    }

    pub fn message_start(&self, receipt: String, time_started: Instant) {
        self.sender.send(ThrottlerMessage::MessageStart {
            receipt,
            time_started
        }).expect("ThrottlerActor.message_start: receivers have died.");
    }

    pub fn message_stop(&self, receipt: String, time_stopped: Instant, success: bool) {
        self.sender.send(ThrottlerMessage::MessageStop {
            receipt,
            time_stopped,
            success
        }).expect("ThrottlerActor.message_stop: receivers have died.");
    }

    pub fn register_consumer_throttler(&self, consumer_throttler: ConsumerThrottlerActor) {
        self.sender
            .send(ThrottlerMessage::RegisterConsumerThrottler { throttler: consumer_throttler })
            .expect("ThrottlerActor.register_consumer_throttler: receivers have died.");
    }
}

/// `StreamingMedian` provides a simple interface for inserting values
/// and calculating medians. By default we start with a repeated median of 31,000.
/// 31,000 is chosen because it is a "worst case" - we assume, at first, that the
/// time to process is longer than the time it takes for a message visibility to
/// time out.
pub struct StreamingMedian {
    data: ArrayDeque<[u32; 64]>,
    sorted: [u32; 63],
    last_median: u32,
}

impl StreamingMedian {
    pub fn new() -> StreamingMedian {
        let data = ArrayDeque::from_iter(iter::repeat(31_000).take(64));

        // We use unsafe here and then immediately assign values to the
        // unused space
        let mut sorted: [u32; 63] = unsafe { uninitialized() };

        for (i, t) in data.iter().enumerate() {
            sorted[i] = *t;
        }

        StreamingMedian {
            data,
            sorted,
            last_median: 31_000,
        }
    }

    /// Returns the last median value without performing any recalculation
    ///
    /// # Example
    /// ```norun
    /// use sqs_service_handler::autoscaling::median;
    ///
    /// let stream = StreamingMedian::new();
    /// assert_eq!(stream.last(), 31_000);
    /// ```
    pub fn last(&self) -> u32 {
        self.last_median
    }

    /// Calculates and returns the median
    ///
    /// # Arguments
    ///
    /// * `value` - The value to be inserted into the stream
    /// # Example
    /// ```norun
    /// use sqs_service_handler::autoscaling::median;
    ///
    /// let stream = StreamingMedian::new();
    /// assert_eq!(stream.insert_and_calculate(31_000), 31_000);
    /// ```
    /// The algorithm used to efficiently insert and calculate relies
    /// on the fact that the data is always left in a sorted state.
    ///
    /// First we pop off the oldest value, 'removed', from our internal
    /// ring buffer. Then we add our new value 'value' to the buffer at
    /// the back. We use this buffer to maintain a temporal relationship
    /// between our values.
    ///
    /// A separate stack array 'self.sorted' is used to maintain a sorted
    /// representation of the data.
    ///
    /// We binary search for 'removed' in our 'sorted' array and store the
    /// index as 'remove_index'.
    ///
    /// We then calculate where to insert the new 'value' by binary searching
    /// for it, either finding it already or where to insert it.
    ///
    /// If the 'insert_index' for our 'value' is less than the 'remove_index'
    /// we shift the data between the 'remove_index' and the 'insert_index' over
    /// one space. This overwrites the old value we want to remove while maintaining
    /// order. We can then insert our value into the array.
    ///
    /// Example:
    /// Starting with a self.sorted of
    /// [2, 3, 4, 5, 7, 8]
    /// We then call insert_and_calculate(6)
    /// Let's assume that '3' is the oldest value. This makes 'remove_index' = 1
    /// We search for where to insert our value '6' and its' index 3.
    /// [2, 3, 4, 5, 7, 8] <- remove_index = 1, insert_index = 3
    /// Shift the data between 1 and 3 over by one.
    /// [2, 4, 5, 5, 7, 8]
    /// Insert our value into index 3.
    /// [2, 4, 5, 6, 7, 8]
    ///
    /// A similar approach is performed in the case of the insert_index being before
    /// the remove index.
    ///
    /// Unsafe is used here to dramatically improve performance - a full 3-5x
    pub fn insert_and_calculate(&mut self, value: u32) -> u32 {
        let mut scratch_space: [u32; 63] = unsafe { uninitialized() };

        let removed = match self.data.pop_front() {
            Some(t) => t,
            None => unsafe { uninitialized() }
        };
        self.data.push_back(value);

        if removed == value {
            return unsafe { *self.sorted.get_unchecked(31) };
        }

        let remove_index = binary_search(&self.sorted, &removed);

        // If removed is larger than value than the remove_index must be
        // after the insert_index, allowing us to cut our search down
        let insert_index = {
            if removed > value {
                let sorted_slice = unsafe { self.sorted.get_unchecked(..remove_index) };
                binary_search(sorted_slice, &value)
            } else {
                let sorted_slice = unsafe { self.sorted.get_unchecked(remove_index..) };
                remove_index + binary_search(sorted_slice, &value)
            }
        };

        // shift the data between remove_index and insert_index so that the
        // value of remove_index is overwritten and the 'value' can be placed
        // in the gap between them

        if remove_index < insert_index {
            // Starting with a self.sorted of
            // [2, 3, 4, 5, 7, 8]
            // insert_and_calculate(6)
            // [2, 3, 4, 5, 7, 8] <- remove_index = 1, insert_index = 3
            // [2, 4, 5, 5, 7, 8]
            // [2, 4, 5, 6, 7, 8]

            unsafe {
                scratch_space.get_unchecked_mut(remove_index + 1..insert_index)
                    .copy_from_slice(self.sorted.get_unchecked(remove_index + 1..insert_index));

                self.sorted.get_unchecked_mut(remove_index..insert_index - 1)
                    .copy_from_slice(scratch_space.get_unchecked(remove_index + 1..insert_index));

                *self.sorted.get_unchecked_mut(insert_index - 1) = value;
            }
        } else {
            // Starting with a self.sorted of
            // [2, 3, 4, 5, 7, 8, 9]
            // insert_and_calculate(6)
            // [2, 3, 4, 5, 7, 8, 9] <- remove_index = 5, insert_index = 3
            // [2, 3, 4, 5, 5, 7, 9] Shift values
            // [2, 3, 4, 6, 7, 8, 9] Insert value
            unsafe {
                scratch_space.get_unchecked_mut(insert_index..remove_index)
                    .copy_from_slice(self.sorted.get_unchecked(insert_index..remove_index));

                self.sorted.get_unchecked_mut(insert_index + 1..remove_index + 1)
                    .copy_from_slice(scratch_space.get_unchecked(insert_index..remove_index));

                *self.sorted.get_unchecked_mut(insert_index) = value;
            }
        }

        let median = unsafe { *self.sorted.get_unchecked(31) };
        self.last_median = median;
        median
    }
}

impl Default for StreamingMedian {
    fn default() -> Self {
        Self::new()
    }
}


fn binary_search<T>(t: &[T], x: &T) -> usize where T: Ord {
    binary_search_by(t, |p| p.cmp(x))
}

// A custom binary search that always returns a usize, showing where an item is or
// where an item can be inserted to preserve sorted order
// Since we have no use for differentiating between the two cases, a single usize
// is sufficient.
fn binary_search_by<T, F>(t: &[T], mut f: F) -> usize
    where F: FnMut(&T) -> Ordering
{
    let mut base = 0usize;
    let mut s = t;

    loop {
        let (head, tail) = s.split_at(s.len() >> 1);
        if tail.is_empty() {
            return base;
        }
        match f(&tail[0]) {
            Ordering::Less => {
                base += head.len() + 1;
                s = &tail[1..];
            }
            Ordering::Greater => s = head,
            Ordering::Equal => return base + head.len(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use xorshift::{Xoroshiro128, Rng, SeedableRng};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn test_median_random() {
        let t = millis(SystemTime::now().duration_since(UNIX_EPOCH).unwrap());
        let mut rng = Xoroshiro128::from_seed(&[t, 71, 1223]);

        let mut median_tracker = StreamingMedian::new();
        for _ in 0..100_000 {
            median_tracker.insert_and_calculate(rng.gen());
        }

        for i in median_tracker.sorted.windows(2) {
            assert!(i[0] < i[1]);
        }
    }

    #[test]
    fn test_median_ascending() {
        let t = millis(SystemTime::now().duration_since(UNIX_EPOCH).unwrap());
        let mut rng = Xoroshiro128::from_seed(&[t, 71, 1223]);

        let mut median_tracker = StreamingMedian::new();

        let mut ascending_iter = 0..;
        for _ in 0..100_000 {
            median_tracker.insert_and_calculate(ascending_iter.next().unwrap());
        }

        for i in median_tracker.sorted.windows(2) {
            assert!(i[0] < i[1]);
        }
    }

    #[test]
    fn test_median_descending() {
        let t = millis(SystemTime::now().duration_since(UNIX_EPOCH).unwrap());
        let mut rng = Xoroshiro128::from_seed(&[t, 71, 1223]);

        let mut median_tracker = StreamingMedian::new();

        let mut ascending_iter = 200_000..;
        for _ in 0..100_000 {
            median_tracker.insert_and_calculate(ascending_iter.next().unwrap());
        }

        for i in median_tracker.sorted.windows(2) {
            assert!(i[0] < i[1]);
        }
    }
}

#[cfg(not(test))]
mod bench {
    use super::*;
    use test::Bencher;
    use xorshift::{Xoroshiro128, Rng, SeedableRng};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[bench]
    fn bench_insert_and_calculate(b: &mut Bencher) {
        let mut median_tracker = StreamingMedian::new();

        b.iter(|| {
            median_tracker.insert_and_calculate(100);
        });
    }

    #[bench]
    fn bench_insert_and_calculate_rand(b: &mut Bencher) {
        let t = millis(SystemTime::now().duration_since(UNIX_EPOCH).unwrap());
        let mut rng = Xoroshiro128::from_seed(&[t, 71, 1223]);

        let mut median_tracker = StreamingMedian::new();

        b.iter(|| {
            median_tracker.insert_and_calculate(rng.gen());
        });
    }
}
