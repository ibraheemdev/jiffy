use crate::utils::{CachePadded, DoubleCachePadded};

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
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
                self.slots
                    .get_unchecked(elem)
                    .get()
                    .write(MaybeUninit::new(value));
                self.elements.push(elem, self.order);
                Ok(())
            },
            None => Err(value),
        }
    }

    pub fn pop(&self) -> Option<T> {
        match self.elements.pop(self.order) {
            Some(elem) => unsafe {
                let value = self.slots.get_unchecked(elem).get().read().assume_init();
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

const SPIN_LIMIT: usize = 40;

pub struct IndexQueue {
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
    threshold: CachePadded<AtomicIsize>,
    slots: CachePadded<Box<[AtomicUsize]>>,
}

impl IndexQueue {
    pub fn empty(order: usize) -> IndexQueue {
        let capacity = 1 << order;

        // the number of slots is double the capacity
        // such that the last dequeuer can always locate
        // an unused slot no farther than 2*capacity slots
        // away from the last enqueuer
        let slots = capacity * 2;

        IndexQueue {
            head: CachePadded(AtomicUsize::new(0)),
            tail: CachePadded(AtomicUsize::new(0)),
            threshold: CachePadded(AtomicIsize::new(-1)),
            slots: CachePadded(
                iter::repeat(-1_isize as usize)
                    .map(AtomicUsize::new)
                    .take(slots)
                    .collect(),
            ),
        }
    }

    pub fn full(order: usize) -> IndexQueue {
        let capacity = 1 << order;
        let slots = capacity * 2;

        let mut entries = iter::repeat(-1_isize as _)
            .map(AtomicUsize::new)
            .take(slots)
            .collect::<Box<[_]>>();

        // initialize the first slots
        for i in 0..capacity {
            entries[i] = AtomicUsize::new(i);
        }

        IndexQueue {
            head: CachePadded(AtomicUsize::new(0)),
            tail: CachePadded(AtomicUsize::new(capacity)),
            threshold: CachePadded(AtomicIsize::new(IndexQueue::threshold(capacity, slots))),
            slots: CachePadded(entries),
        }
    }

    pub fn push(&self, index: usize, order: usize) {
        let capacity = 1 << order;
        let slots = capacity * 2;

        'next: loop {
            // acquire a slot
            let tail = self.tail.fetch_add(1, Ordering::AcqRel);
            let tail_index = tail & (slots - 1);
            let cycle = (tail << 1) | (2 * slots - 1);

            let mut slot = unsafe { self.slots.get_unchecked(tail_index).load(Ordering::Acquire) };

            'retry: loop {
                let slot_cycle = slot | (2 * slots - 1);

                // the slot is from a newer cycle, move to
                // the next one
                if wrapping_cmp!(slot_cycle, >=, cycle) {
                    continue 'next;
                }

                // we can safely read the entry if either:
                // - the entry is unused and safe
                // - the entry is unused and _unsafe_ but
                //   the head is behind the tail
                if slot == slot_cycle
                    || (slot == slot_cycle ^ slots
                        && wrapping_cmp!(self.head.load(Ordering::Acquire), <=, tail))
                {
                    // set the safe bit
                    let new_slot = index ^ (slots - 1);
                    // store the cycle
                    let new_slot = cycle ^ new_slot;

                    unsafe {
                        if let Err(e) = self.slots.get_unchecked(tail_index).compare_exchange_weak(
                            slot,
                            new_slot,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        ) {
                            slot = e;
                            continue 'retry;
                        }
                    }

                    let threshold = IndexQueue::threshold(capacity, slots);

                    // reset the threshold
                    if self.threshold.load(Ordering::Acquire) != threshold {
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

        let slots = 1 << (order + 1);

        'next: loop {
            // acquire a slot
            let head = self.head.fetch_add(1, Ordering::AcqRel);
            let head_index = head & (slots - 1);
            let cycle = (head << 1) | (2 * slots - 1);

            let mut spun = 0;

            'spin: loop {
                let mut slot =
                    unsafe { self.slots.get_unchecked(head_index).load(Ordering::Acquire) };

                'retry: loop {
                    let slot_cycle = slot | (2 * slots - 1);

                    // if the cycles match, we can read this slot
                    if slot_cycle == cycle {
                        unsafe {
                            // mark as unused, but preserve the safety bit
                            self.slots
                                .get_unchecked(head_index)
                                .fetch_or(slots - 1, Ordering::AcqRel);
                        }

                        // extract the index (ignore cycle and safety bit)
                        return Some(slot & (slots - 1));
                    }

                    // if the slot is from an older cycle,
                    // we have to update it
                    if wrapping_cmp!(slot_cycle, <, cycle) {
                        let new_slot = if (slot | slots) == slot_cycle {
                            // spin for a bit before invalidating
                            // the slot for writers from a previous
                            // cycles in case they arrive soon,
                            // alleviating unnecessary contention
                            if spun <= SPIN_LIMIT {
                                spun += 1;

                                std::hint::spin_loop();
                                continue 'spin;
                            }

                            // the slot is unused, preserve the safety bit
                            cycle ^ (!slot & slots)
                        } else {
                            // mark the slot as unsafe
                            let new_entry = slot & !slots;

                            if slot == new_entry {
                                continue 'next;
                            }

                            new_entry
                        };

                        unsafe {
                            if let Err(e) =
                                self.slots.get_unchecked(head_index).compare_exchange_weak(
                                    slot,
                                    new_slot,
                                    Ordering::AcqRel,
                                    Ordering::Acquire,
                                )
                            {
                                slot = e;
                                continue 'retry;
                            }
                        }

                        let tail = self.tail.load(Ordering::Acquire);

                        // if the head overtook the tail, push the tail forward
                        if tail <= head + 1 {
                            self.catchup(tail, head + 1);
                            self.threshold.fetch_sub(1, Ordering::AcqRel);
                            return None;
                        }

                        if self.threshold.fetch_sub(1, Ordering::AcqRel) <= 0 {
                            return None;
                        }

                        continue 'next;
                    }
                }
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
                break tail.wrapping_sub(head);
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
