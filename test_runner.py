#!/usr/bin/env python
import os

print("Running tests: Debug + Release * Sanitizers. This will take a while.")

print("cargo test")
os.system(r"cargo test")

print("cargo test --release")
os.system(r"cargo test --release")

print("cargo test sanitizer = address")
os.system(r"ASAN_OPTIONS=detect_odr_violation=0 RUSTFLAGS=\"-Z sanitizer=address\" cargo test --target x86_64-unknown-linux-gnu")

print("cargo test sanitizer = leak")
os.system(r"RUSTFLAGS=\"-Z sanitizer=leak\" cargo test --target x86_64-unknown-linux-gnu")

print("cargo test sanitizer = thread")
os.system(r"RUSTFLAGS=\"-Z sanitizer=thread\" cargo test --target x86_64-unknown-linux-gnu")

print("cargo test --release sanitizer = address")
os.system(r"ASAN_OPTIONS=detect_odr_violation=0 RUSTFLAGS=\"-Z sanitizer=address\" cargo test --release --target x86_64-unknown-linux-gnu")
print("cargo test --release sanitizer = leak")
os.system(r"RUSTFLAGS=\"-Z sanitizer=leak\" cargo test --release --target x86_64-unknown-linux-gnu")
print("cargo test --release sanitizer = thread")
os.system(r"RUSTFLAGS=\"-Z sanitizer=thread\" cargo test --release --target x86_64-unknown-linux-gnu")
