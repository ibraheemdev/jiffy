use std::{
    sync::atomic::{AtomicBool, Ordering},
    thread,
};

use criterion::{criterion_group, criterion_main, Criterion};

const THREADS: usize = 15;
const MESSAGES: usize = 15 * 100_000;

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
                    while !self.unparked.swap(false, Ordering::Acquire) {
                        thread::park();
                    }
                }
            }
        }
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

fn mpsc(c: &mut Criterion) {
    let mut group = c.benchmark_group("mpsc");
    group.sample_size(10);

    group.bench_function("jiffy", |b| {
        b.iter(|| {
            let queue = Chan::new(jiffy::Queue::new());

            crossbeam::scope(|scope| {
                for _ in 0..THREADS {
                    scope.spawn(|_| {
                        for i in 0..MESSAGES / THREADS {
                            queue.send(|c| Ok::<_, ()>(c.push(i))).unwrap();
                        }
                    });
                }

                for _ in 0..MESSAGES {
                    queue.recv(|c| unsafe { c.pop() }).unwrap();
                }
            })
            .unwrap();
        })
    });

    group.bench_function("riffy", |b| {
        b.iter(|| {
            let queue = Chan::new(riffy::MpscQueue::new());

            crossbeam::scope(|scope| {
                for _ in 0..THREADS {
                    scope.spawn(|_| {
                        for i in 0..MESSAGES / THREADS {
                            queue.send(|c| c.enqueue(i)).unwrap();
                        }
                    });
                }

                for _ in 0..MESSAGES {
                    queue.recv(|c| c.dequeue()).unwrap();
                }
            })
            .unwrap();
        })
    });

    group.bench_function("crossbeam-queue", |b| {
        b.iter(|| {
            let queue = Chan::new(crossbeam::queue::SegQueue::new());

            crossbeam::scope(|scope| {
                for _ in 0..THREADS {
                    scope.spawn(|_| {
                        for i in 0..MESSAGES / THREADS {
                            queue.send(|c| Ok::<_, ()>(c.push(i))).unwrap();
                        }
                    });
                }

                for _ in 0..MESSAGES {
                    queue.recv(|c| c.pop()).unwrap();
                }
            })
            .unwrap();
        })
    });

    group.bench_function("crossbeam-channel", |b| {
        b.iter(|| {
            let (tx, rx) = crossbeam::channel::unbounded();

            crossbeam::scope(|scope| {
                for _ in 0..THREADS {
                    scope.spawn(|_| {
                        for i in 0..MESSAGES / THREADS {
                            tx.send(i).unwrap();
                        }
                    });
                }

                for _ in 0..MESSAGES {
                    rx.recv().unwrap();
                }
            })
            .unwrap();
        })
    });

    group.bench_function("std", |b| {
        b.iter(|| {
            let (tx, rx) = std::sync::mpsc::channel();

            crossbeam::scope(|scope| {
                for _ in 0..THREADS {
                    let tx = tx.clone();
                    scope.spawn(move |_| {
                        for i in 0..MESSAGES / THREADS {
                            tx.send(i).unwrap();
                        }
                    });
                }

                for _ in 0..MESSAGES {
                    rx.recv().unwrap();
                }
            })
            .unwrap();
        })
    });

    group.bench_function("flume", |b| {
        b.iter(|| {
            let (tx, rx) = flume::unbounded();

            crossbeam::scope(|scope| {
                for _ in 0..THREADS {
                    scope.spawn(|_| {
                        for i in 0..MESSAGES / THREADS {
                            tx.send(i).unwrap();
                        }
                    });
                }

                for _ in 0..MESSAGES {
                    rx.recv().unwrap();
                }
            })
            .unwrap();
        })
    });

    group.finish();
}

criterion_group!(benches, mpsc);
criterion_main!(benches);
