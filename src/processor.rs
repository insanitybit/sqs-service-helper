use visibility::*;

use rusoto_sqs::Message as SqsMessage;
use two_lock_queue::{Sender, Receiver, RecvTimeoutError, unbounded, channel};
use std::time::Duration;

use slog::Logger;
use uuid::Uuid;
use std::thread;
use std::sync::{Arc, Mutex};

use lru_time_cache::LruCache;

pub trait MessageHandler {
    fn process_message(&mut self, msg: SqsMessage) -> Result<(), String>;
}

#[derive(Clone)]
pub struct MessageHandlerActor {
    sender: Sender<SqsMessage>,
    id: String
}

impl MessageHandlerActor {
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn from_queue<M, P, F>(new: &F,
                               sender: Sender<SqsMessage>,
                               receiver: Receiver<SqsMessage>,
                               state_manager: M,
                               short_circuit: Option<Arc<Mutex<LruCache<String, ()>>>>,
                               logger: Logger)
                               -> MessageHandlerActor
        where M: MessageStateManager + Clone + Send + 'static,
              P: MessageHandler + Send + 'static,
              F: Fn(MessageHandlerActor) -> P
    {
        let id = Uuid::new_v4().to_string();

        let actor = MessageHandlerActor {
            sender: sender.clone(),
            id,
        };

        let mut _actor = new(actor.clone());

        thread::spawn(
            move || {
                MessageHandlerActor::actor_loop(
                    logger,
                    receiver,
                    short_circuit,
                    _actor,
                    state_manager
                )
            });

        actor
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn new<M, P, F>(new: F,
                        state_manager: M,
                        short_circuit: Option<Arc<Mutex<LruCache<String, ()>>>>,
                        logger: Logger)
                        -> MessageHandlerActor
        where M: MessageStateManager + Clone + Send + 'static,
              P: MessageHandler + Send + 'static,
              F: FnOnce(MessageHandlerActor) -> P
    {
        let (sender, receiver) = channel(100);
        let id = Uuid::new_v4().to_string();

        let actor = MessageHandlerActor {
            sender: sender.clone(),
            id,
        };

        let mut _actor = new(actor.clone());

        thread::spawn(
            move || {
                MessageHandlerActor::actor_loop(
                    logger,
                    receiver,
                    short_circuit,
                    _actor,
                    state_manager
                )
            });

        actor
    }

    fn actor_loop<M, P>(logger: Logger,
                        recvr: Receiver<SqsMessage>,
                        short_circuit: Option<Arc<Mutex<LruCache<String, ()>>>>,
                        mut actor: P,
                        mut state_manager: M)
        where M: MessageStateManager + Clone + Send + 'static,
              P: MessageHandler + Send + 'static,
    {
        loop {
            match recvr.recv_timeout(Duration::from_secs(60)) {
                Ok(msg) => {
                    let receipt = match msg.receipt_handle.clone() {
                        Some(r) => r,
                        None => {
                            error!(logger, "Missing receipt handle");
                            continue
                        }
                    };

                    match actor.process_message(msg) {
                        Ok(_) => {
                            state_manager.deregister(receipt.clone(), true);
                        }
                        Err(e) => {
                            error!(
                                logger,
                                "Actor failed to process message: {}",
                                e
                            );
                            state_manager.deregister(receipt.clone(), false);
                        }
                    }

                    if let Some(ref sc) = short_circuit {
                        let mut short_circuit = sc.lock().unwrap();
                        short_circuit.insert(receipt.clone(), ());
                    }
                }
                Err(RecvTimeoutError::Disconnected) => {
                    break
                }
                Err(RecvTimeoutError::Timeout) => {}
            }
        }
    }
}

#[derive(Clone)]
pub struct MessageHandlerBroker
{
    workers: Vec<MessageHandlerActor>,
    sender: Sender<SqsMessage>,
    id: String
}

impl MessageHandlerBroker
{
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn new<M, P, T, F>(new: F,
                           worker_count: usize,
                           max_queue_depth: T,
                           state_manager: M,
                           short_circuit: Option<Arc<Mutex<LruCache<String, ()>>>>,
                           logger: Logger)
                           -> MessageHandlerBroker
        where P: MessageHandler + Send + 'static,
              M: MessageStateManager + Clone + Send + 'static,
              F: Fn(MessageHandlerActor) -> P,
              T: Into<Option<usize>>,
    {
        let id = Uuid::new_v4().to_string();

        let (sender, receiver) = max_queue_depth.into().map_or(unbounded(), channel);

        let workers = (0..worker_count)
            .map(|_|
                MessageHandlerActor::from_queue(&new,
                                                sender.clone(),
                                                receiver.clone(),
                                                state_manager.clone(),
                                                short_circuit.clone(),
                                                logger.clone())
            )
            .collect();

        MessageHandlerBroker {
            workers,
            sender,
            id
        }
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn process(&self, message: SqsMessage) {
        self.sender.send(
            message
        ).unwrap();
    }
}