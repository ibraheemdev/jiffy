use std::{
    sync::atomic::{AtomicBool, Ordering},
    thread,
};

use criterion::{criterion_group, criterion_main, Criterion};

struct Chan {
    thread: thread::Thread,
    unparked: AtomicBool,
}

impl Chan {
    fn new() -> Self {
        Self {
            thread: thread::current(),
            unparked: AtomicBool::new(false),
        }
    }

    fn send<V, T>(&self, x: &T, f: impl Fn(&T) -> Result<(), V>) -> Result<(), V> {
        f(&x).map(|_| self.unpark())
    }

    fn recv<V, T>(&self, x: &T, f: impl Fn(&T) -> Option<V>) -> Option<V> {
        loop {
            match f(x) {
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

fn mpsc_bounded(c: &mut Criterion) {
    let mut group = c.benchmark_group("mpsc");
    group.sample_size(20);

    const THREADS: usize = 14;
    const MESSAGES: usize = 14 * 200_000;

    group.bench_function("jiffy", |b| {
        b.iter(|| {
            let q = jiffy::unbounded::Queue::new();
            let c = Chan::new();

            crossbeam::scope(|scope| {
                for _ in 0..THREADS {
                    scope.spawn({
                        |_| {
                            for i in 0..MESSAGES / THREADS {
                                c.send(&q, |c| Ok::<_, ()>(c.push(i))).unwrap();
                            }
                        }
                    });
                }

                for _ in 0..MESSAGES {
                    c.recv(&q, |c| c.pop()).unwrap();
                }
            })
            .unwrap();
        })
    });

    group.bench_function("crossbeam", |b| {
        b.iter(|| {
            let t = crossbeam::queue::SegQueue::new();
            let c = Chan::new();

            crossbeam::scope(|scope| {
                for _ in 0..THREADS {
                    scope.spawn({
                        |_| {
                            for i in 0..MESSAGES / THREADS {
                                c.send(&t, |c| Ok::<_, ()>(c.push(i))).unwrap();
                            }
                        }
                    });
                }

                for _ in 0..MESSAGES {
                    c.recv(&t, |c| c.pop()).unwrap();
                }
            })
            .unwrap();
        })
    });

    group.bench_function("riffy (unsound)", |b| {
        b.iter(|| {
            let t = riffy::MpscQueue::new();
            let c = Chan::new();

            crossbeam::scope(|scope| {
                for _ in 0..THREADS {
                    scope.spawn({
                        |_| {
                            for i in 0..MESSAGES / THREADS {
                                c.send(&t, |c| c.enqueue(i)).unwrap();
                            }
                        }
                    });
                }

                for _ in 0..MESSAGES {
                    c.recv(&t, |c| c.dequeue()).unwrap();
                }
            })
            .unwrap();
        })
    });

    group.bench_function("protty-looish", |b| {
        b.iter(|| {
            let t = jiffy::protty_looish::Queue::EMPTY;
            let c = Chan::new();

            crossbeam::scope(|scope| {
                for _ in 0..THREADS {
                    scope.spawn({
                        |_| {
                            for i in 0..MESSAGES / THREADS {
                                c.send(&t, |c| Ok::<_, ()>(c.send(i))).unwrap();
                            }
                        }
                    });
                }

                for _ in 0..MESSAGES {
                    unsafe {
                        c.recv(&t, |c| c.try_recv()).unwrap();
                    }
                }
            })
            .unwrap();
        })
    });

    group.finish();
}

criterion_group!(benches, mpsc_bounded);
criterion_main!(benches);
