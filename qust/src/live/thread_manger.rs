use std::sync::{mpsc, Arc, Mutex};
use std::thread;

use crate::prelude::TickData;

use super::prelude::HoldLocal;

pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: mpsc::Sender<Job>,
}

type Job = Box<dyn FnOnce() + Send>;

impl ThreadPool {
    pub fn new(size: usize) -> ThreadPool {
        assert!(size > 0);

        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));

        let mut workers = Vec::with_capacity(size);

        for id in 0..size {
            workers.push(Worker::new(id, Arc::clone(&receiver)));
        }

        ThreadPool { workers, sender }
    }

    pub fn execute<F>(&self, f: F, tick_data: Arc<TickData>, hold_local: Arc<HoldLocal>)
    where
        F: FnOnce(&TickData, &HoldLocal) + Send + 'static,
    {
        let job = Box::new(move || f(&tick_data, &hold_local));
        self.sender.send(job).unwrap();
    }
}

pub struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    pub fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Job>>>) -> Worker {
        let thread = thread::spawn(move || loop {
            let job = receiver.lock().unwrap().recv().unwrap();
            println!("Worker {} got a job; executing.", id);
            job();
        });

        Worker {
            id,
            thread: Some(thread),
        }
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        for worker in &mut self.workers {
            println!("Shutting down worker {}", worker.id);

            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        }
    }
}

pub fn main() {
    let pool = ThreadPool::new(4);
    let tick_data = Arc::new(TickData::default());
    let hold_local = Arc::new(HoldLocal::default());
    for i in 0..8 {
        pool.execute(move |tick_data, _hold_local| {
            println!("Executing task {}, tick_data: {:?}", i, tick_data);
        }, tick_data.clone(), hold_local.clone());
    }
}