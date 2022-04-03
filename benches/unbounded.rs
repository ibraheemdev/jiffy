// use std::{
//     sync::atomic::{AtomicBool, Ordering},
//     thread,
// };
// 
// use criterion::{criterion_group, criterion_main, Criterion};
// 
// const THREADS: usize = 12;
// const MESSAGES: usize = 12 * 200_000;
// 
// struct Chan {
//     thread: thread::Thread,
//     unparked: AtomicBool,
// }
// 
// impl Chan {
//     fn new() -> Self {
//         Self {
//             thread: thread::current(),
//             unparked: AtomicBool::new(false),
//         }
//     }
// 
//     fn send<V, T>(&self, x: &T, f: impl Fn(&T) -> Result<(), V>) -> Result<(), V> {
//         f(&x).map(|_| self.unpark())
//     }
// 
//     fn recv<V, T>(&self, x: &T, f: impl Fn(&T) -> Option<V>) -> Option<V> {
//         loop {
//             match f(x) {
//                 Some(x) => break Some(x),
//                 None => {
//                     while !self.try_unpark() {
//                         thread::park();
//                     }
//                 }
//             }
//         }
//     }
// 
//     fn try_unpark(&self) -> bool {
//         self.unparked.swap(false, Ordering::Acquire)
//     }
// 
//     fn unpark(&self) {
//         self.unparked
//             .fetch_update(Ordering::Release, Ordering::Relaxed, |unparked| {
//                 if unparked {
//                     None
//                 } else {
//                     Some(true)
//                 }
//             })
//             .map(|_| self.thread.unpark())
//             .unwrap_or(());
//     }
// }
// 
// fn mpsc_bounded(c: &mut Criterion) {
//     let mut group = c.benchmark_group("mpsc");
//     group.sample_size(20);
// 
//     group.bench_function("loo", |b| {
//         b.iter(|| {
//             let (t, r) = loo::mpsc::queue();
//             let c = Chan::new();
// 
//             crossbeam::scope(|scope| {
//                 for _ in 0..THREADS {
//                     scope.spawn({
//                         let t = t.clone();
//                         |_| {
//                             let t = t;
//                             for i in 0..MESSAGES / THREADS {
//                                 c.send(&t, |c| Ok::<_, ()>(c.push_back(i))).unwrap();
//                             }
//                         }
//                     });
//                 }
// 
//                 for _ in 0..MESSAGES {
//                     c.recv(&r, |c| c.pop_front()).unwrap();
//                 }
//             })
//             .unwrap();
//         })
//     });
// 
//     group.finish();
// }
// 
// criterion_group!(benches, mpsc_bounded);
// criterion_main!(benches);
