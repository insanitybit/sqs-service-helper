#!/usr/bin/env python

import subprocess
import os

print("Running tests: Debug + Release * Sanitizers. This will take a while.")
print("Note that 'memory' sanitizer is not yet supported")
print("cargo test")
subprocess.check_call(['cargo', 'test'])

print("cargo test --release")
subprocess.check_call(['cargo', 'test', '--release'])

for sanitizer in ['address', 'thread']:
    print("cargo test sanitizer = {}".format(sanitizer))

    e = dict(os.environ, RUSTFLAGS='-Z sanitizer={}'.format(sanitizer))

    if sanitizer == 'address':
        e['ASAN_OPTIONS'] = 'detect_odr_violation=0',
    if sanitizer == 'thread':
        e['RUST_TEST_THREADS'] = '1'

    subprocess.check_call(['cargo', 'test','--message-format', 'json', '--target', 'x86_64-unknown-linux-gnu'], env=e)

for sanitizer in ['address', 'thread']:
    print("cargo test sanitizer = {}".format(sanitizer))

    e = dict(os.environ, RUSTFLAGS='-Z sanitizer={}'.format(sanitizer))

    if sanitizer == 'address':
        e['ASAN_OPTIONS'] = 'detect_odr_violation=0',
    if sanitizer == 'thread':
        e['RUST_TEST_THREADS'] = '1'

    subprocess.check_call(['cargo', 'test','--message-format', 'json', '--release', '--target', 'x86_64-unknown-linux-gnu'], env=e)
