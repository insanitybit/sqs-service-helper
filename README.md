# sqs-service-handler
A library for making SQS consumer based services trivial to write.

# Features

* Automatic visibility timeout handling
* Automatic consumer throttling/ scaling
* Automatic use of AWS Bulk APIs
* Parallel message processing


## Visibility Timeout Handling

When an SQS Message is received from its queue it remains
invisible for a set amount of time - either defined by
the SQS Queue or the message. The default is 30 seconds.

When that timeout expires the message reappears on the queue
with a new receipt.

If a message takes longer to process than its visibility timeout
then, without extra measures, the message will likely be processed
many times.

In order to avoid this issue the framework will automatically
start background services to update the timeout. The goal is
to keep the timeout as low as possible so that if there is an
error during process it will appear quickly onto the queue, but
also to ensure that regardless of the time necessary to process
the message it will remain invisible.

The current algorithm goes something like this:

* A consumer receives a message
* The message is registered with the MessageStateManager
* The message receipt is sent to a VisibilityExtenderBuffer, which will
flush if it reaches ten receipts or a timeout occurs.
* The receipts, now grouped together, reach the actual
VisibilityTimeoutExtender, which uses a bulk API call to update the
messages visibilities.

Throughout this process the time it takes for a message to reach the
extender from the consumer is tracked and sent to the Throttler. The
throttler uses this time to determine the maximum number of inflight
messages the service can handle while processing messages quickly
enough to maintain their invisibility state.

It is notable that in the event of network failures where message
visibility updates or deletes fail for the duration of a timeout
that multiple message processing will occur. This is simply a limitation
of the at-least-once model of processing that SQS has chosen.

## Consumer Throttling/ Scaling

As previously mentioned the service tracks how long it takes for a
message's receipt to go from the consumer to the Visibility Extender.

This is the service's bottleneck.

Based on the median time this takes the service will scale consumers
up or down, as well as implement a throttle - a set amount of time each
consumer needs to wait between API calls (this takes the call's latency
into account, so as to keep this wait period to a minimum).

A specialied StreamingMedian structure is used to efficiently calculate
the median for the last 63 messages.

This throttling/ scaling should mean that you're always processing as
many messages as the framework can handle, but not more.

## Automatic Bulk APIs

SQS supports using bulk APIs to reduce IO and costs. This can allow
for a 10x cost reduction for each bulk API utilized.

Consumers will, by default, long poll for 20 seconds and grab at most
10 messages.

This library will automatically buffer delete and visibility update
requests, increasing the frequency of bulk API usage.

## Parallel Message Processing

Once you have provided your MessageHandler to the framework it will
spread the processor across threads (currently defined by the user).

The processors use a work stealing algorithm over an MPMC queue.