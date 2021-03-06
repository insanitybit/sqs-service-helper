use arraydeque::ArrayDeque;
use lru_time_cache::LruCache;
use two_lock_queue::{Sender, RecvTimeoutError, unbounded};
use uuid;
use std::cmp::{min, max, Ordering};
use std::iter::{self, FromIterator};
use std::time::{Instant, Duration};
use std::thread;
use std::mem::uninitialized;

use consumer::*;
use util::*;
use slog::Logger;

type CurrentActors = usize;

pub trait Throttler {
    fn message_start(&mut self, identifier: String, time_started: Instant);
    fn message_stop(&mut self, identifier: String, time_stopped: Instant);
}

pub enum ThrottlerMessage {
    Start {
        receipt: String,
        time_started: Instant
    },
    Stop {
        receipt: String,
        time_stopped: Instant,
    },
    RegisterThrottled {
        throttler: ConsumerThrottlerActor
    }
}

#[derive(Clone)]
pub struct ThrottlerActor
{
    sender: Sender<ThrottlerMessage>,
    id: String
}

/// `MedianThrottler` tracks the processing time for messages from the consumer to the
/// deleter. It then uses this information to calculate how many messages we can
/// process in 30 seconds, the time it takes for a message to time out. Ideally
/// we process as many messages as we can while ensuring that we don't back our
/// message visibility handlers up, which could lead to more frequent double
/// processing.
pub struct MedianThrottler {
    throttler: Option<ConsumerThrottlerActor>,
    inflight_timings: LruCache<String, Instant>,
    previously_seen: LruCache<String, ()>,
    proc_times: StreamingMedian,
    inflight_limit: usize,
    logger: Logger
}

impl MedianThrottler {
    pub fn new(logger: Logger) -> MedianThrottler {
        let inflight_timings =
            LruCache::with_expiry_duration_and_capacity(Duration::from_secs(12 * 60 * 60),
                                                        120_000);
        let previously_seen =
            LruCache::with_expiry_duration_and_capacity(Duration::from_secs(12 * 60 * 60),
            120_000);

        let proc_times = StreamingMedian::new();

        MedianThrottler {
            throttler: None,
            inflight_timings,
            previously_seen,
            proc_times,
            inflight_limit: 10,
            logger
        }
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
            ThrottlerMessage::Start { receipt, time_started } =>
                self.message_start(receipt, time_started),
            ThrottlerMessage::Stop { receipt, time_stopped } =>
                self.message_stop(receipt, time_stopped),
            ThrottlerMessage::RegisterThrottled { throttler } =>
                self.register_consumer_throttler(throttler)
        }
    }

    pub fn get_inflight_limit(&self) -> usize {
        self.inflight_limit
    }
}

impl Throttler for MedianThrottler {
    fn message_start(&mut self, receipt: String, time_started: Instant) {
        if self.inflight_timings.insert(receipt.clone(), time_started).is_some() {
            error!(self.logger, "Message starting twice");
        }

        self.previously_seen.insert(receipt, ());

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

    fn message_stop(&mut self, receipt: String, time_stopped: Instant) {

        // We're only interested in the initial time it took to get to the visibility extender
        if self.previously_seen.get(&receipt).is_some() {
            return
        }

        // Get the time that the messagae was started
        let start_time = self.inflight_timings.remove(&receipt);

        match start_time {
            // We are only interested in timings for successfully processed messages, at least
            // in part because this is likely the slowest path and we want to throttle accordingly
            Some(start_time) => {
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
                    debug!(self.logger,
                           "Setting new inflight limit to : {} from {}",
                           self.inflight_limit, last_limit);
                }

                self.inflight_limit = min(self.inflight_limit, 120_000);
            }
            _ => {
                warn!(self.logger,
                      "Attempting to deregister timeout that does not exist:\
                receipt: {}", receipt);
            }
        };
    }

}

impl ThrottlerActor
{
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn new(actor: MedianThrottler)
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
            id: id,
        }
    }

    pub fn register_consumer_throttler(&self, consumer_throttler: ConsumerThrottlerActor) {
        self.sender
            .send(ThrottlerMessage::RegisterThrottled { throttler: consumer_throttler })
            .expect("ThrottlerActor.register_consumer_throttler: receivers have died.");
    }
}

impl Throttler for ThrottlerActor {
    fn message_start(&mut self, receipt: String, time_started: Instant) {
        self.sender.send(ThrottlerMessage::Start {
            receipt,
            time_started
        }).expect("ThrottlerActor.message_start: receivers have died.");
    }

    fn message_stop(&mut self, receipt: String, time_stopped: Instant) {
        self.sender.send(ThrottlerMessage::Stop {
            receipt,
            time_stopped,
        }).expect("ThrottlerActor.message_stop: receivers have died.");
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
            assert!(i[0] <= i[1]);
        }
    }

    #[test]
    fn test_median_ascending() {
        let mut median_tracker = StreamingMedian::new();

        let mut ascending_iter = 0..;
        for _ in 0..100_000 {
            median_tracker.insert_and_calculate(ascending_iter.next().unwrap());
        }

        for i in median_tracker.sorted.windows(2) {
            assert!(i[0] <= i[1]);
        }
    }

    #[test]
    fn test_median_descending() {
        let mut median_tracker = StreamingMedian::new();

        let mut ascending_iter = 200_000..;
        for _ in 0..100_000 {
            median_tracker.insert_and_calculate(ascending_iter.next().unwrap());
        }

        for i in median_tracker.sorted.windows(2) {
            assert!(i[0] <= i[1]);
        }
    }


    #[quickcheck]
    fn maintains_sorted(input: u32) -> bool {
        let mut median_tracker = StreamingMedian::new();
        median_tracker.insert_and_calculate(input);

        for i in median_tracker.sorted.windows(2) {
            if i[0] > i[1] {
                return false
            }
        }
        true
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
    fn bench_insert_and_calculate(b: &mut Bencher) {
        let mut median_tracker = StreamingMedian::new();

        b.iter(|| {
            median_tracker.insert_and_calculate(100);
        });
    }

    #[bench]
    fn bench_insert_and_calculate_rand(b: &mut Bencher) {
        let t = millis(SystemTime::now().duration_since(UNIX_EPOCH).unwrap());
        let rng = Xoroshiro128::from_seed(&[t, 71, 1223]);

        let mut median_tracker = StreamingMedian::new();

        b.iter(|| {
            median_tracker.insert_and_calculate(rng.gen());
        });
    }
}
