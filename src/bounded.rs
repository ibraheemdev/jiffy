use crate::utils::{CachePadded, DoubleCachePadded};

use std::cell::UnsafeCell;
use std::mem::{self, MaybeUninit};
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};
use std::{iter, ptr};

pub type Queue<T> = DoubleCachePadded<Inner<T>>;

pub struct Inner<T> {
    free: IndexQueue,
    elements: IndexQueue,
    slots: Box<[UnsafeCell<MaybeUninit<T>>]>,
    order: usize,
}

unsafe impl<T: Send> Send for Queue<T> {}
unsafe impl<T: Send> Sync for Queue<T> {}

impl<T> Queue<T> {
    pub fn new(capacity: usize) -> Self {
        if capacity == 0 {
            panic!("capacity must be non-zero");
        }

        let capacity = capacity.next_power_of_two();
        let order = (usize::BITS - capacity.leading_zeros() - 1) as _;

        if order > 31 {
            panic!("exceeded maximum queue capacity of {}", MAX_CAPACITY);
        }

        DoubleCachePadded(Inner {
            slots: iter::repeat_with(|| UnsafeCell::new(MaybeUninit::uninit()))
                .take(capacity)
                .collect(),
            free: IndexQueue::full(order),
            elements: IndexQueue::empty(order),
            order,
        })
    }

    pub fn push(&self, value: T) -> Result<(), T> {
        match self.free.pop(self.order) {
            Some(elem) => unsafe {
                (*self.slots.get_unchecked(elem).get()).write(value);
                self.elements.push(elem, self.order);
                Ok(())
            },
            None => Err(value),
        }
    }

    pub fn pop(&self) -> Option<T> {
        match self.elements.pop(self.order) {
            Some(elem) => unsafe {
                let value = mem::replace(
                    &mut *self.slots.get_unchecked(elem).get(),
                    MaybeUninit::uninit(),
                )
                .assume_init();
                self.free.push(elem, self.order);
                Some(value)
            },
            None => None,
        }
    }

    pub fn len(&self) -> usize {
        self.elements.len()
    }

    pub fn is_empty(&self) -> bool {
        self.elements.len() == 0
    }

    pub fn is_full(&self) -> bool {
        self.free.len() == 0
    }

    pub fn capacity(&self) -> usize {
        1 << self.order
    }
}

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        while let Some(elem) = self.elements.pop(self.order) {
            unsafe { ptr::drop_in_place(self.slots[elem].get()) }
        }
    }
}

/// Note: If set by a user, this will round up to 1 << 32.
pub const MAX_CAPACITY: usize = (1 << 31) + 1;

pub struct IndexQueue {
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
    threshold: CachePadded<AtomicIsize>,
    entries: CachePadded<Box<[AtomicUsize]>>,
}

impl IndexQueue {
    pub fn empty(order: usize) -> IndexQueue {
        IndexQueue {
            head: CachePadded(AtomicUsize::new(0)),
            tail: CachePadded(AtomicUsize::new(0)),
            threshold: CachePadded(AtomicIsize::new(-1)),
            entries: CachePadded(
                iter::repeat(-1_isize as usize)
                    .map(AtomicUsize::new)
                    // double capacity
                    .take(1 << (order + 1))
                    .collect(),
            ),
        }
    }

    pub fn full(order: usize) -> IndexQueue {
        let half = 1 << order;
        let cap = half * 2;

        let mut entries = iter::repeat(-1_isize as _)
            .map(AtomicUsize::new)
            .take(cap)
            .collect::<Box<[_]>>();

        for i in 0..half {
            entries[i] = AtomicUsize::new(i);
        }

        IndexQueue {
            head: CachePadded(AtomicUsize::new(0)),
            tail: CachePadded(AtomicUsize::new(half)),
            threshold: CachePadded(AtomicIsize::new(IndexQueue::threshold(half, cap))),
            entries: CachePadded(entries),
        }
    }

    pub fn push(&self, index: usize, order: usize) {
        let half = 1 << order;
        let cap = half * 2;
        let index = index ^ (cap - 1);

        'next: loop {
            // acquire an entry
            let tail = self.tail.fetch_add(1, Ordering::AcqRel);
            let tail_cycle = (tail << 1) | (2 * cap - 1);
            let tail_index = tail & (cap - 1);

            let mut entry = unsafe {
                self.entries
                    .get_unchecked(tail_index)
                    .load(Ordering::Acquire)
            };

            'retry: loop {
                let entry_cycle = entry | (2 * cap - 1);

                // the entry is from a newer cycle
                if wrapping_cmp!(entry_cycle, >=, tail_cycle) {
                    continue 'next;
                }

                if entry == entry_cycle
                    || (entry == (entry_cycle ^ cap)
                        && wrapping_cmp!(self.head.load(Ordering::Acquire), <=, tail))
                {
                    let new_entry = tail_cycle ^ index;

                    unsafe {
                        if let Err(e) = self
                            .entries
                            .get_unchecked(tail_index)
                            .compare_exchange_weak(
                                entry,
                                new_entry,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                        {
                            entry = e;
                            continue 'retry;
                        }
                    }

                    let threshold = IndexQueue::threshold(half, cap);
                    if self.threshold.load(Ordering::Acquire) != threshold {
                        // reset the threshold
                        self.threshold.store(threshold, Ordering::Release);
                    }

                    return;
                }

                continue 'next;
            }
        }
    }

    pub fn pop(&self, order: usize) -> Option<usize> {
        // the queue is empty
        if self.threshold.load(Ordering::Acquire) < 0 {
            return None;
        }

        let cap = 1 << (order + 1);

        loop {
            // release an entry
            let head = self.head.fetch_add(1, Ordering::AcqRel);
            let head_cycle = (head << 1) | (2 * cap - 1);
            let head_index = head & (cap - 1);

            let mut entry = unsafe {
                self.entries
                    .get_unchecked(head_index)
                    .load(Ordering::Acquire)
            };

            loop {
                let entry_cycle = entry | (2 * cap - 1);

                if entry_cycle == head_cycle {
                    // the cycles match, consume this entry
                    // but preserve the safety bit and cycle
                    self.entries[head_index].fetch_or(cap - 1, Ordering::AcqRel);
                    return Some(entry & (cap - 1));
                }

                let new_entry = if (entry | cap) == entry_cycle {
                    head_cycle ^ (!entry & cap)
                } else {
                    let new_entry = entry & !cap;
                    if entry == new_entry {
                        break;
                    }
                    new_entry
                };

                // the entry is from a newer cycle
                if wrapping_cmp!(entry_cycle, >=, head_cycle) {
                    break;
                }

                unsafe {
                    match self
                        .entries
                        .get_unchecked(head_index)
                        .compare_exchange_weak(
                            entry,
                            new_entry,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        ) {
                        Ok(_) => break,
                        Err(e) => {
                            entry = e;
                        }
                    }
                }
            }

            let tail = self.tail.load(Ordering::Acquire);

            if tail <= head + 1 {
                // the head overtook the tail, push the tail forward
                self.catchup(tail, head + 1);
                self.threshold.fetch_sub(1, Ordering::AcqRel);
                return None;
            }

            if self.threshold.fetch_sub(1, Ordering::AcqRel) <= 0 {
                return None;
            }
        }
    }

    fn catchup(&self, mut tail: usize, mut head: usize) {
        while self
            .tail
            .compare_exchange_weak(tail, head, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            head = self.head.load(Ordering::Acquire);
            tail = self.tail.load(Ordering::Acquire);

            if wrapping_cmp!(tail, >=, head) {
                break;
            }
        }
    }

    pub fn len(&self) -> usize {
        loop {
            let tail = self.tail.load(Ordering::Acquire);
            let head = self.head.load(Ordering::Acquire);

            if self.tail.load(Ordering::Relaxed) == tail {
                break dbg!(tail).wrapping_sub(dbg!(head));
            }
        }
    }

    #[inline(always)]
    fn threshold(half: usize, cap: usize) -> isize {
        ((half + cap) - 1) as isize
    }
}

macro_rules! wrapping_cmp {
    ($x:expr, $op:tt, $y:expr) => {
        ((($x).wrapping_sub($y)) as isize) $op 0
    }
}

pub(self) use wrapping_cmp;
