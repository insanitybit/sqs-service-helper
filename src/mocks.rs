#![allow(warnings)]

use autoscaling::*;
use visibility::*;
use consumer::*;
use processor::*;
use rusoto_sqs;
use rusoto_sqs::{Sqs, Message as SqsMessage};
use std::thread;
use std::time::{Duration, Instant};
use rusoto_credential::*;
use rusoto_sns::*;
use std::sync::*;
use std::collections::VecDeque;
use std;
use std::mem::forget;
use std::fs::OpenOptions;
use slog;
use slog_json;
use slog_scope;
use slog_stdlog;
use slog::{Drain, FnValue};
use base64::encode;
use serde_json;
use delay::DelayMessage;
#[cfg(feature = "flame_it")]
use flame;
use std::fs::File;
use uuid::Uuid;
use std::sync::{Arc, Mutex};
use two_lock_queue::{Sender, Receiver, RecvTimeoutError, unbounded, channel};

use std::sync::atomic::{AtomicUsize, Ordering};

pub fn new_sqs_client() -> MockSqs
{
    MockSqs {
        receives: Arc::new(AtomicUsize::new(0)),
        deletes: Arc::new(AtomicUsize::new(0))
    }
}

pub fn new_sns_client() -> MockSns
{
    MockSns {
        publishes: Arc::new(AtomicUsize::new(0))
    }
}

#[derive(Clone)]
pub struct MockThrottler {
    pub sender: Sender<ThrottlerMessage>,
    pub receiver: Receiver<ThrottlerMessage>,
}

impl MockThrottler {
    pub fn new() -> MockThrottler {
        let (sender, receiver) = unbounded();

        MockThrottler {
            sender,
            receiver
        }
    }
}

impl Throttler for MockThrottler {
    fn message_start(&mut self, receipt: String, time_started: Instant) {
        self.sender.send(ThrottlerMessage::Start {
            receipt,
            time_started
        });
    }

    fn message_stop(&mut self, receipt: String, time_stopped: Instant) {
        self.sender.send(ThrottlerMessage::Stop {
            receipt,
            time_stopped,
        });
    }
}

#[derive(Clone)]
pub struct MockConsumer {
    pub sender: Sender<ConsumerMessage>,
    pub receiver: Receiver<ConsumerMessage>,
}

impl MockConsumer {
    pub fn new() -> MockConsumer {
        let (sender, receiver) = unbounded();

        MockConsumer {
            sender,
            receiver
        }
    }
}

impl Consumer for MockConsumer {
    fn consume(&mut self) {
        self.sender.send(ConsumerMessage::Consume)
            .expect("MockConsumer panicked at consume");
    }

    fn throttle(&mut self, how_long: Duration) {
        self.sender.send(ConsumerMessage::Throttle { how_long })
            .expect("MockConsumer panicked at throttle");
    }

    fn shut_down(&mut self) {
        self.sender.send(ConsumerMessage::ShutDown)
            .expect("MockConsumer panicked at shut_down");
    }

    fn route_msg(&mut self, msg: ConsumerMessage) {
        self.sender.send(msg).unwrap();
    }
}

#[derive(Clone)]
pub struct MockProcessor {
    pub sender: Sender<SqsMessage>,
    pub receiver: Receiver<SqsMessage>,
}

impl MockProcessor {
    pub fn new() -> MockProcessor {
        let (sender, receiver) = unbounded();

        MockProcessor {
            sender,
            receiver
        }
    }
}

impl MessageHandler for MockProcessor {
    fn process_message(&mut self, msg: SqsMessage) -> Result<(), String> {
        self.sender.send(msg).unwrap();
        Ok(())
    }
}

#[derive(Clone)]
pub struct MockMessageStateManager {
    pub sender: Sender<MessageStateManagerMessage>,
    pub receiver: Receiver<MessageStateManagerMessage>,
}

impl MockMessageStateManager {
    pub fn new() -> MockMessageStateManager {
        let (sender, receiver) = unbounded();

        MockMessageStateManager {
            sender,
            receiver
        }
    }
}

impl MessageStateManager for MockMessageStateManager {
    fn register(&mut self, receipt: String, visibility_timeout: Duration, start_time: Instant) {
        self.sender.send(
            MessageStateManagerMessage::RegisterVariant {
                receipt, visibility_timeout, start_time
            }
        ).unwrap();
    }

    fn deregister(&mut self, receipt: String, should_delete: bool) {
        self.sender.send(
            MessageStateManagerMessage::DeregisterVariant {
                receipt, should_delete
            }
        ).unwrap();
    }

    fn route_msg(&mut self, msg: MessageStateManagerMessage) {
        self.sender.send(msg).unwrap();
    }
}

pub struct MockSqs {
    pub receives: Arc<AtomicUsize>,
    pub deletes: Arc<AtomicUsize>
}

pub struct MockSns {
    pub publishes: Arc<AtomicUsize>
}

impl Sqs for MockSqs {
    fn receive_message(&self, input: &rusoto_sqs::ReceiveMessageRequest) -> Result<rusoto_sqs::ReceiveMessageResult, rusoto_sqs::ReceiveMessageError> {
        //            thread::sleep(Duration::from_millis(25));

        let mut rc = self.receives.clone();

        if rc.load(Ordering::Relaxed) >= 1_000 {
            thread::sleep(Duration::from_secs(20));
            return Ok(rusoto_sqs::ReceiveMessageResult {
                messages: None
            });
        }

        rc.fetch_add(10, Ordering::Relaxed);

        let mut messages = vec![];
        for _ in 0..10 {
            let delay_message = serde_json::to_string(
                &DelayMessage {
                    message: Uuid::new_v4().to_string(),
                    topic_name: "topic".to_owned(),
                    correlation_id: Some("foo".to_owned()),
                }).unwrap();

            messages.push(
                rusoto_sqs::Message {
                    attributes: None,
                    body: Some(encode(&delay_message)),
                    md5_of_body: Some("md5body".to_owned()),
                    md5_of_message_attributes: Some("md5attrs".to_owned()),
                    message_attributes: None,
                    message_id: Some(Uuid::new_v4().to_string()),
                    receipt_handle: Some(Uuid::new_v4().to_string()),
                }
            )
        }

        Ok(rusoto_sqs::ReceiveMessageResult {
            messages: Some(messages)
        }
        )
    }

    fn add_permission(&self, input: &rusoto_sqs::AddPermissionRequest) -> Result<(), rusoto_sqs::AddPermissionError> {
        unimplemented!()
    }


    fn change_message_visibility(&self,
                                 input: &rusoto_sqs::ChangeMessageVisibilityRequest)
                                 -> Result<(), rusoto_sqs::ChangeMessageVisibilityError> {
        //            thread::sleep(Duration::from_millis(10));
        Ok(())
    }


    fn change_message_visibility_batch
    (&self,
     input: &rusoto_sqs::ChangeMessageVisibilityBatchRequest)
     -> Result<rusoto_sqs::ChangeMessageVisibilityBatchResult, rusoto_sqs::ChangeMessageVisibilityBatchError> {
        Ok(
            rusoto_sqs::ChangeMessageVisibilityBatchResult {
                failed: vec![],
                successful: vec![]
            }
        )
    }


    fn create_queue(&self,
                    input: &rusoto_sqs::CreateQueueRequest)
                    -> Result<rusoto_sqs::CreateQueueResult, rusoto_sqs::CreateQueueError> {
        Ok(
            rusoto_sqs::CreateQueueResult {
                queue_url: Some("queueurl".to_owned())
            }
        )
    }


    fn delete_message(&self, input: &rusoto_sqs::DeleteMessageRequest) -> Result<(), rusoto_sqs::DeleteMessageError> {
        unimplemented!()
    }


    fn delete_message_batch(&self,
                            input: &rusoto_sqs::DeleteMessageBatchRequest)
                            -> Result<rusoto_sqs::DeleteMessageBatchResult, rusoto_sqs::DeleteMessageBatchError> {
        let msg_count = input.entries.len();
        self.deletes.fetch_add(msg_count, Ordering::Relaxed);
        Ok(
            rusoto_sqs::DeleteMessageBatchResult {
                failed: vec![],
                successful: vec![]
            }
        )
    }


    fn delete_queue(&self, input: &rusoto_sqs::DeleteQueueRequest) -> Result<(), rusoto_sqs::DeleteQueueError> {
        unimplemented!()
    }


    fn get_queue_attributes(&self,
                            input: &rusoto_sqs::GetQueueAttributesRequest)
                            -> Result<rusoto_sqs::GetQueueAttributesResult, rusoto_sqs::GetQueueAttributesError> {
        unimplemented!()
    }


    fn get_queue_url(&self,
                     input: &rusoto_sqs::GetQueueUrlRequest)
                     -> Result<rusoto_sqs::GetQueueUrlResult, rusoto_sqs::GetQueueUrlError> {
        unimplemented!()
    }


    fn list_dead_letter_source_queues
    (&self,
     input: &rusoto_sqs::ListDeadLetterSourceQueuesRequest)
     -> Result<rusoto_sqs::ListDeadLetterSourceQueuesResult, rusoto_sqs::ListDeadLetterSourceQueuesError> {
        unimplemented!()
    }


    fn list_queues(&self, input: &rusoto_sqs::ListQueuesRequest) -> Result<rusoto_sqs::ListQueuesResult, rusoto_sqs::ListQueuesError> {
        unimplemented!()
    }


    fn purge_queue(&self, input: &rusoto_sqs::PurgeQueueRequest) -> Result<(), rusoto_sqs::PurgeQueueError> {
        unimplemented!()
    }


    fn remove_permission(&self,
                         input: &rusoto_sqs::RemovePermissionRequest)
                         -> Result<(), rusoto_sqs::RemovePermissionError> {
        unimplemented!()
    }


    fn send_message(&self,
                    input: &rusoto_sqs::SendMessageRequest)
                    -> Result<rusoto_sqs::SendMessageResult, rusoto_sqs::SendMessageError> {
        unimplemented!()
    }


    fn send_message_batch(&self,
                          input: &rusoto_sqs::SendMessageBatchRequest)
                          -> Result<rusoto_sqs::SendMessageBatchResult, rusoto_sqs::SendMessageBatchError> {
        unimplemented!()
    }


    fn set_queue_attributes(&self,
                            input: &rusoto_sqs::SetQueueAttributesRequest)
                            -> Result<(), rusoto_sqs::SetQueueAttributesError> {
        unimplemented!()
    }
}

impl Sns for MockSns {
    fn add_permission(&self, input: &AddPermissionInput) -> Result<(), AddPermissionError> {
        unimplemented!()
    }


    fn check_if_phone_number_is_opted_out
    (&self,
     input: &CheckIfPhoneNumberIsOptedOutInput)
     -> Result<CheckIfPhoneNumberIsOptedOutResponse, CheckIfPhoneNumberIsOptedOutError> {
        unimplemented!()
    }


    fn confirm_subscription(&self,
                            input: &ConfirmSubscriptionInput)
                            -> Result<ConfirmSubscriptionResponse, ConfirmSubscriptionError> {
        unimplemented!()
    }


    fn create_platform_application
    (&self,
     input: &CreatePlatformApplicationInput)
     -> Result<CreatePlatformApplicationResponse, CreatePlatformApplicationError> {
        unimplemented!()
    }


    fn create_platform_endpoint(&self,
                                input: &CreatePlatformEndpointInput)
                                -> Result<CreateEndpointResponse, CreatePlatformEndpointError> {
        unimplemented!()
    }


    fn create_topic(&self,
                    input: &CreateTopicInput)
                    -> Result<CreateTopicResponse, CreateTopicError> {
        Ok(
            CreateTopicResponse {
                topic_arn: Some("Arn".to_owned())
            }
        )
    }


    fn delete_endpoint(&self, input: &DeleteEndpointInput) -> Result<(), DeleteEndpointError> {
        unimplemented!()
    }


    fn delete_platform_application(&self,
                                   input: &DeletePlatformApplicationInput)
                                   -> Result<(), DeletePlatformApplicationError> {
        unimplemented!()
    }


    fn delete_topic(&self, input: &DeleteTopicInput) -> Result<(), DeleteTopicError> {
        unimplemented!()
    }


    fn get_endpoint_attributes
    (&self,
     input: &GetEndpointAttributesInput)
     -> Result<GetEndpointAttributesResponse, GetEndpointAttributesError> {
        unimplemented!()
    }


    fn get_platform_application_attributes
    (&self,
     input: &GetPlatformApplicationAttributesInput)
     -> Result<GetPlatformApplicationAttributesResponse, GetPlatformApplicationAttributesError> {
        unimplemented!()
    }


    fn get_sms_attributes(&self,
                          input: &GetSMSAttributesInput)
                          -> Result<GetSMSAttributesResponse, GetSMSAttributesError> {
        unimplemented!()
    }


    fn get_subscription_attributes
    (&self,
     input: &GetSubscriptionAttributesInput)
     -> Result<GetSubscriptionAttributesResponse, GetSubscriptionAttributesError> {
        unimplemented!()
    }


    fn get_topic_attributes(&self,
                            input: &GetTopicAttributesInput)
                            -> Result<GetTopicAttributesResponse, GetTopicAttributesError> {
        unimplemented!()
    }


    fn list_endpoints_by_platform_application
    (&self,
     input: &ListEndpointsByPlatformApplicationInput)
     -> Result<ListEndpointsByPlatformApplicationResponse,
         ListEndpointsByPlatformApplicationError> {
        unimplemented!()
    }


    fn list_phone_numbers_opted_out
    (&self,
     input: &ListPhoneNumbersOptedOutInput)
     -> Result<ListPhoneNumbersOptedOutResponse, ListPhoneNumbersOptedOutError> {
        unimplemented!()
    }


    fn list_platform_applications
    (&self,
     input: &ListPlatformApplicationsInput)
     -> Result<ListPlatformApplicationsResponse, ListPlatformApplicationsError> {
        unimplemented!()
    }


    fn list_subscriptions(&self,
                          input: &ListSubscriptionsInput)
                          -> Result<ListSubscriptionsResponse, ListSubscriptionsError> {
        unimplemented!()
    }


    fn list_subscriptions_by_topic
    (&self,
     input: &ListSubscriptionsByTopicInput)
     -> Result<ListSubscriptionsByTopicResponse, ListSubscriptionsByTopicError> {
        unimplemented!()
    }


    fn list_topics(&self, input: &ListTopicsInput) -> Result<ListTopicsResponse, ListTopicsError> {
        unimplemented!()
    }


    fn opt_in_phone_number(&self,
                           input: &OptInPhoneNumberInput)
                           -> Result<OptInPhoneNumberResponse, OptInPhoneNumberError> {
        unimplemented!()
    }


    fn publish(&self, input: &PublishInput) -> Result<PublishResponse, PublishError> {
        let mut rc = self.publishes.clone();

        rc.fetch_add(1, Ordering::Relaxed);

        Ok(PublishResponse {
            message_id: Some("id".to_owned())
        })
    }


    fn remove_permission(&self,
                         input: &RemovePermissionInput)
                         -> Result<(), RemovePermissionError> {
        unimplemented!()
    }


    fn set_endpoint_attributes(&self,
                               input: &SetEndpointAttributesInput)
                               -> Result<(), SetEndpointAttributesError> {
        unimplemented!()
    }


    fn set_platform_application_attributes(&self,
                                           input: &SetPlatformApplicationAttributesInput)
                                           -> Result<(), SetPlatformApplicationAttributesError> {
        unimplemented!()
    }


    fn set_sms_attributes(&self,
                          input: &SetSMSAttributesInput)
                          -> Result<SetSMSAttributesResponse, SetSMSAttributesError> {
        unimplemented!()
    }


    fn set_subscription_attributes(&self,
                                   input: &SetSubscriptionAttributesInput)
                                   -> Result<(), SetSubscriptionAttributesError> {
        unimplemented!()
    }


    fn set_topic_attributes(&self,
                            input: &SetTopicAttributesInput)
                            -> Result<(), SetTopicAttributesError> {
        unimplemented!()
    }


    fn subscribe(&self, input: &SubscribeInput) -> Result<SubscribeResponse, SubscribeError> {
        unimplemented!()
    }


    fn unsubscribe(&self, input: &UnsubscribeInput) -> Result<(), UnsubscribeError> {
        unimplemented!()
    }
}

