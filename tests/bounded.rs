use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam::thread::scope;
use jiffy::bounded::Queue;
use rand::{thread_rng, Rng};

#[test]
fn foo() {
    let x = Queue::<()>::new(16);
    for _ in 0..100 {
        x.pop();
    }
}

#[test]
fn smoke() {
    let q = Queue::new(1);

    q.push(7).unwrap();
    assert_eq!(q.pop(), Some(7));

    q.push(8).unwrap();
    assert_eq!(q.pop(), Some(8));
    assert!(q.pop().is_none());
}

#[test]
fn capacity() {
    for i in 1..10 {
        let q = Queue::<i32>::new(i);
        assert_eq!(q.capacity(), i.next_power_of_two());
    }
}

#[test]
#[should_panic(expected = "capacity must be non-zero")]
fn zero_capacity() {
    let _ = Queue::<i32>::new(0);
}

#[test]
fn len_empty_full() {
    let q = Queue::new(2);

    assert_eq!(q.len(), 0);
    assert!(q.is_empty());
    assert!(!q.is_full());

    q.push(()).unwrap();

    assert_eq!(q.len(), 1);
    assert!(!q.is_empty());
    assert!(!q.is_full());

    q.push(()).unwrap();

    assert_eq!(q.len(), 2);
    assert!(!q.is_empty());
    assert!(q.is_full());

    q.pop().unwrap();

    assert_eq!(q.len(), 1);
    assert!(!q.is_empty());
    assert!(!q.is_full());
}

#[test]
fn len() {
    #[cfg(miri)]
    const COUNT: usize = 500;
    #[cfg(not(miri))]
    const COUNT: usize = 25_000;
    #[cfg(miri)]
    const CAP: usize = 1 << 7;
    #[cfg(not(miri))]
    const CAP: usize = 1 << 14;
    #[cfg(miri)]
    const RUNS: usize = 1;
    #[cfg(not(miri))]
    const RUNS: usize = 20;

    for _ in 0..RUNS {
        let q = Queue::new(CAP);
        assert_eq!(q.len(), 0);

        for _ in 0..CAP / 10 {
            for i in 0..50 {
                q.push(i).unwrap();
                assert_eq!(q.len(), i + 1);
            }

            for i in 0..50 {
                q.pop().unwrap();
                assert_eq!(q.len(), 50 - i - 1);
            }
        }
        assert_eq!(q.len(), 0);

        for i in 0..CAP {
            q.push(i).unwrap();
            assert_eq!(q.len(), i + 1);
        }

        for _ in 0..CAP {
            q.pop().unwrap();
        }
        assert_eq!(q.len(), 0);

        scope(|scope| {
            scope.spawn(|_| {
                for i in 0..COUNT {
                    loop {
                        if let Some(x) = q.pop() {
                            assert_eq!(x, i);
                            break;
                        }

                        #[cfg(miri)]
                        std::thread::yield_now(); // https://github.com/rust-lang/miri/issues/1388
                    }
                    let len = q.len();
                    assert!(len <= CAP, "{} > {} ???", len, CAP);
                }
            });

            scope.spawn(|_| {
                for i in 0..COUNT {
                    while q.push(i).is_err() {
                        #[cfg(miri)]
                        std::thread::yield_now(); // https://github.com/rust-lang/miri/issues/1388
                    }
                    let len = q.len();
                    assert!(len <= CAP, "{} > {} ???", len, CAP);
                }
            });
        })
        .unwrap();
        assert_eq!(q.len(), 0);
    }
}

#[test]
fn spsc() {
    #[cfg(miri)]
    const COUNT: usize = 500;
    #[cfg(not(miri))]
    const COUNT: usize = 100_000;

    let q = Queue::new(3);

    scope(|scope| {
        scope.spawn(|_| {
            for i in 0..COUNT {
                loop {
                    if let Some(x) = q.pop() {
                        assert_eq!(x, i);
                        break;
                    }
                    #[cfg(miri)]
                    std::thread::yield_now(); // https://github.com/rust-lang/miri/issues/1388
                }
            }
            assert!(q.pop().is_none());
        });

        scope.spawn(|_| {
            for i in 0..COUNT {
                while q.push(i).is_err() {
                    #[cfg(miri)]
                    std::thread::yield_now(); // https://github.com/rust-lang/miri/issues/1388
                }
            }
        });
    })
    .unwrap();
}

#[cfg_attr(miri, ignore)] // Miri is too slow
#[test]
fn mpmc() {
    const COUNT: usize = 25_000;
    const THREADS: usize = 4;

    let q = Queue::<usize>::new(3);
    let v = (0..COUNT).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>();

    scope(|scope| {
        for _ in 0..THREADS {
            scope.spawn(|_| {
                for _ in 0..COUNT {
                    let n = loop {
                        if let Some(x) = q.pop() {
                            break x;
                        }
                    };
                    v[n].fetch_add(1, Ordering::SeqCst);
                }
            });
        }
        for _ in 0..THREADS {
            scope.spawn(|_| {
                for i in 0..COUNT {
                    while q.push(i).is_err() {}
                }
            });
        }
    })
    .unwrap();

    for c in v {
        assert_eq!(c.load(Ordering::SeqCst), THREADS);
    }
}

#[test]
fn drops() {
    #[cfg(miri)]
    const RUNS: usize = 50;
    #[cfg(not(miri))]
    const RUNS: usize = 100;
    #[cfg(miri)]
    const STEPS: usize = 500;
    #[cfg(not(miri))]
    const STEPS: usize = 10_000;

    static DROPS: AtomicUsize = AtomicUsize::new(0);

    #[derive(Debug, PartialEq)]
    struct DropCounter;

    impl Drop for DropCounter {
        fn drop(&mut self) {
            DROPS.fetch_add(1, Ordering::SeqCst);
        }
    }

    let mut rng = thread_rng();

    for _ in 0..RUNS {
        let steps = rng.gen_range(0..STEPS);
        let additional = rng.gen_range(0..50);

        DROPS.store(0, Ordering::SeqCst);
        let q = Queue::new(50);

        scope(|scope| {
            scope.spawn(|_| {
                for _ in 0..steps {
                    while q.pop().is_none() {
                        #[cfg(miri)]
                        std::thread::yield_now(); // https://github.com/rust-lang/miri/issues/1388
                    }
                }
            });

            scope.spawn(|_| {
                for _ in 0..steps {
                    while q.push(DropCounter).is_err() {
                        DROPS.fetch_sub(1, Ordering::SeqCst);
                        #[cfg(miri)]
                        std::thread::yield_now(); // https://github.com/rust-lang/miri/issues/1388
                    }
                }
            });
        })
        .unwrap();

        for _ in 0..additional {
            q.push(DropCounter).unwrap();
        }

        assert_eq!(DROPS.load(Ordering::SeqCst), steps);
        drop(q);
        assert_eq!(DROPS.load(Ordering::SeqCst), steps + additional);
    }
}
