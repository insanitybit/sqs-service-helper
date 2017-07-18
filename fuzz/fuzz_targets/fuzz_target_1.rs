#![no_main]
#[macro_use] extern crate libfuzzer_sys;
extern crate sqs_service_helper;

use sqs_service_helper::autoscaling::*;

fuzz_target!(|data: &[u8]| {
    let mut median_tracker = StreamingMedian::new();

    for value in data {
        median_tracker.insert_and_calculate(*value as u32);
    }
});
