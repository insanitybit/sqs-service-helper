#![allow(dead_code)]
//#![deny(warnings)]
#![feature(conservative_impl_trait, drop_types_in_const, test)]
#![cfg_attr(feature = "flame_it", feature(plugin, custom_attribute))]
#![cfg_attr(feature = "flame_it", plugin(flamer))]

extern crate test;

#[cfg(feature = "flame_it")]
extern crate flame;

#[macro_use]
extern crate slog;
extern crate slog_scope;
extern crate slog_stdlog;
extern crate slog_term;

#[macro_use]
extern crate serde_derive;

extern crate base64;
extern crate coco;
extern crate xorshift;
extern crate dogstatsd;
extern crate arrayvec;
extern crate arraydeque;
extern crate rusoto_sqs;
extern crate rusoto_sns;
extern crate rusoto_core;
extern crate hyper;
extern crate futures;
extern crate tokio_core;
extern crate tokio_timer;
extern crate threadpool;
extern crate futures_cpupool;
extern crate rusoto_credential;
extern crate two_lock_queue;
extern crate itertools;
extern crate serde;
extern crate serde_json;
extern crate lru_time_cache;
extern crate parking_lot;
extern crate stopwatch;
extern crate slog_json;
extern crate uuid;
extern crate slog_stream;
extern crate fibers;

macro_rules! time {
    ($expression:expr) => (
        {
            let mut sw = $crate::stopwatch::Stopwatch::start_new();
            let exp = $expression;
            sw.stop();
            println!("{} took {}ms",stringify!($expression) , sw.elapsed_ms());
            exp
//              $expression
        }
    );
    ($expression:expr, $s:expr) => (
        {
            let mut sw = $crate::stopwatch::Stopwatch::start_new();
            let exp = $expression;
            sw.stop();
            println!("{} took {}ms", stringify!($s), sw.elapsed_ms());
            exp
//              $expression
        }
    );
}

macro_rules! timeout_ms {
    ($pool:expr, $closure:expr, $dur:expr) => {
        {
            let timeout = Timer::default().sleep(Duration::from_millis($dur))
                .then(|_| Err(()));
            let value = $pool.spawn_fn($closure);
            let value_or_timeout = timeout.select(value).map(|(win, _)| win);
            value_or_timeout.wait()
        }
    };
    ($pool:expr, $closure:expr, $dur:expr, $timer:expr) => {
        {
            let timeout = $timer.sleep(Duration::from_millis($dur))
                .then(|_| Err(()));
            let value = $pool.spawn_fn($closure);
            let value_or_timeout = timeout.select(value).map(|(win, _)| win);
            value_or_timeout.wait()
        }
    };
}

pub mod actor;
pub mod autoscaling;
pub mod consumer;
pub mod delay;
pub mod delete;
pub mod metrics;
pub mod processor;
pub mod publish;
pub mod util;
pub mod visibility;

mod mocks;
mod queue;


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
    }
}
