use std::{
    sync::atomic::{AtomicBool, Ordering},
    thread,
};

fn main() {
    let threads = num_cpus::get() - 2;
    let messages = threads * 200_000;

    for _ in 0..100_000 {
        let queue = Chan::new(jiffy::bounded::Queue::new(messages));

        crossbeam::scope(|scope| {
            for _ in 0..threads {
                scope.spawn(|_| {
                    for i in 0..messages / threads {
                        queue.send(|c| c.push(i)).unwrap();
                    }
                });
            }

            for _ in 0..messages {
                queue.recv(|c| c.pop()).unwrap();
            }
        })
        .unwrap();
    }
}

struct Chan<T> {
    thread: thread::Thread,
    unparked: AtomicBool,
    inner: T,
}

impl<T> Chan<T> {
    fn new(inner: T) -> Self {
        Self {
            thread: thread::current(),
            unparked: AtomicBool::new(false),
            inner,
        }
    }

    fn send<V>(&self, f: impl Fn(&T) -> Result<(), V>) -> Result<(), V> {
        f(&self.inner).map(|_| self.unpark())
    }

    fn recv<V>(&self, f: impl Fn(&T) -> Option<V>) -> Option<V> {
        loop {
            match f(&self.inner) {
                Some(x) => break Some(x),
                None => {
                    while !self.try_unpark() {
                        thread::park();
                    }
                }
            }
        }
    }

    fn try_unpark(&self) -> bool {
        self.unparked.swap(false, Ordering::Acquire)
    }

    fn unpark(&self) {
        self.unparked
            .fetch_update(Ordering::Release, Ordering::Relaxed, |unparked| {
                if unparked {
                    None
                } else {
                    Some(true)
                }
            })
            .map(|_| self.thread.unpark())
            .unwrap_or(());
    }
}
