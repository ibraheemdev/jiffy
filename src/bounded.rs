use crate::utils::{CachePadded, DoubleCachePadded};

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};
use std::{hint, iter, ptr};

pub struct Queue<T> {
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

        Queue {
            slots: iter::repeat_with(|| UnsafeCell::new(MaybeUninit::uninit()))
                .take(capacity)
                .collect(),
            free: IndexQueue::full(order),
            elements: IndexQueue::empty(order),
            order,
        }
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
        self.elements.len(self.order)
    }

    pub fn is_empty(&self) -> bool {
        self.elements.len(self.order) == 0
    }

    pub fn is_full(&self) -> bool {
        self.free.len(self.order) == 0
    }

    pub fn capacity(&self) -> usize {
        1 << self.order
    }
}

impl<T> Drop for Queue<T> {
    fn drop(&mut self) {
        while let Some(elem) = self.elements.pop(self.order) {
            unsafe { ptr::drop_in_place((*self.slots[elem].get()).as_mut_ptr()) }
        }
    }
}

/// Note: If set by a user, this will round up to 1 << 32.
pub const MAX_CAPACITY: usize = (1 << 31) + 1;

const SPIN_LIMIT: usize = 6;

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
        // such that the last reader can always locate
        // an unused slot no farther than capacity*2 slots
        // away from the last writer
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

        let mut queue = IndexQueue::empty(order);

        for i in 0..capacity {
            queue.slots[cache_remap!(i, order, slots)] =
                AtomicUsize::new(raw_cache_remap!(slots + i, order, capacity));
        }

        *queue.tail.get_mut() = capacity;
        *queue.threshold.get_mut() = IndexQueue::threshold(capacity, slots);

        queue
    }

    pub fn push(&self, index: usize, order: usize) {
        let capacity = 1 << order;
        let slots = capacity * 2;

        'next: loop {
            // acquire a slot
            let tail = self.tail.fetch_add(1, Ordering::Relaxed);
            // let tail_index = tail & (slots - 1);
            let tail_index = cache_remap!(tail, order, slots);
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
                //   the head is behind the tail, meaning
                //   all active readers are still behind
                if slot == slot_cycle
                    || (slot == slot_cycle ^ slots
                        && wrapping_cmp!(self.head.load(Ordering::Acquire), <=, tail))
                {
                    // set the safety bit and cycle
                    let new_slot = index ^ (slots - 1);
                    let new_slot = cycle ^ new_slot;

                    unsafe {
                        if let Err(new) =
                            self.slots.get_unchecked(tail_index).compare_exchange_weak(
                                slot,
                                new_slot,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                        {
                            slot = new;
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
            let head = self.head.fetch_add(1, Ordering::Relaxed);
            // let head_index = head & (slots - 1);
            let head_index = cache_remap!(head, order, slots);
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

                    if wrapping_cmp!(slot_cycle, <, cycle) {
                        // otherwise, we have to update the cycle
                        let new_slot = if (slot | slots) == slot_cycle {
                            // spin for a bit before invalidating
                            // the slot for writers from a previous
                            // cycle in case they arrive soon
                            if spun <= SPIN_LIMIT {
                                spun += 1;
                                (0..(1 << spun)).for_each(|_| hint::spin_loop());
                                continue 'spin;
                            }

                            // the slot is unused, preserve the safety bit
                            cycle ^ (!slot & slots)
                        } else {
                            // mark the slot as unsafe
                            let new_entry = slot & !slots;

                            if slot == new_entry {
                                break 'retry;
                            }

                            new_entry
                        };

                        unsafe {
                            if let Err(new) =
                                self.slots.get_unchecked(head_index).compare_exchange_weak(
                                    slot,
                                    new_slot,
                                    Ordering::AcqRel,
                                    Ordering::Acquire,
                                )
                            {
                                slot = new;
                                continue 'retry;
                            }
                        }
                    }

                    break 'retry;
                }

                // check if the queue is empty
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

    fn catchup(&self, mut tail: usize, mut head: usize) {
        while let Err(t) =
            self.tail
                .compare_exchange_weak(tail, head, Ordering::AcqRel, Ordering::Acquire)
        {
            tail = t;
            head = self.head.load(Ordering::Acquire);

            if wrapping_cmp!(tail, >=, head) {
                break;
            }
        }
    }

    pub fn len(&self, order: usize) -> usize {
        let capacity = 1 << order;

        loop {
            let tail = self.tail.load(Ordering::Acquire);
            let head = self.head.load(Ordering::Acquire);

            // make sure we have consistent values to work with
            if self.tail.load(Ordering::Acquire) == tail {
                let hix = head & (capacity - 1);
                let tix = tail & (capacity - 1);

                break if hix < tix {
                    tix - hix
                } else if hix > tix {
                    capacity - (hix - tix)
                } else if tail == head {
                    0
                } else {
                    capacity
                };
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

macro_rules! cache_remap {
    ($i:expr, $order:expr, $n:expr) => {
        raw_cache_remap!($i, ($order) + 1, $n)
    };
}

macro_rules! raw_cache_remap {
    ($i:expr, $order:expr, $n:expr) => {
        ((($i) & (($n) - 1)) >> (($order) - MIN)) | ((($i) << MIN) & (($n) - 1))
    };
}

pub(self) use {cache_remap, raw_cache_remap, wrapping_cmp};

// TODO: not x86
const CACHE_SHIFT: usize = 7;

#[cfg(target_pointer_width = "32")]
const MIN: usize = CACHE_SHIFT - 2;

#[cfg(target_pointer_width = "64")]
const MIN: usize = CACHE_SHIFT - 3;

#[cfg(target_pointer_width = "128")]
const MIN: usize = CACHE_SHIFT - 4;

pub mod idea {
    use crate::utils::{CachePadded, StrictProvenance};
    use std::{
        alloc::{alloc, dealloc, Layout},
        cell::{Cell, UnsafeCell},
        hint::spin_loop,
        marker::PhantomPinned,
        mem::{drop, MaybeUninit},
        pin::Pin,
        ptr::{self, NonNull},
        sync::atomic::{fence, AtomicBool, AtomicPtr, AtomicUsize, Ordering},
        thread,
    };

    #[derive(Default)]
    struct Backoff(usize);

    impl Backoff {
        fn is_completed(&self) -> bool {
            self.0 > 6
        }

        fn yield_now(&mut self) {
            if self.0 < 10 {
                self.0 += 1;
            }

            if !self.is_completed() || cfg!(windows) {
                (0..(1 << self.0)).for_each(|_| spin_loop());
                return;
            }

            thread::yield_now();
        }
    }

    fn pinned<P: Default, T, F: FnOnce(Pin<&P>) -> T>(f: F) -> T {
        let pinnable = P::default();
        f(unsafe { Pin::new_unchecked(&pinnable) })
    }

    #[derive(Default)]
    struct Event {
        thread: Cell<Option<thread::Thread>>,
        is_set: AtomicBool,
        _pinned: PhantomPinned,
    }

    impl Event {
        fn with<F>(f: impl FnOnce(Pin<&Self>) -> F) -> F {
            pinned::<Self, _, _>(|event| {
                event.thread.set(Some(thread::current()));
                f(event)
            })
        }

        fn wait(&self) {
            while !self.is_set.load(Ordering::Acquire) {
                thread::park();
            }
        }

        fn set(&self) {
            let thread = self.thread.take().unwrap();
            self.is_set.store(true, Ordering::Release);
            thread.unpark();
        }
    }

    #[derive(Default)]
    struct Parker {
        event: AtomicPtr<Event>,
    }

    impl Parker {
        const fn new() -> Self {
            Self {
                event: AtomicPtr::new(ptr::null_mut()),
            }
        }

        #[cold]
        fn park(&self) {
            let mut ev = self.event.load(Ordering::Acquire);
            let notified = NonNull::dangling().as_ptr();

            let mut spin = Backoff::default();
            while !ptr::eq(ev, notified) && !spin.is_completed() {
                spin.yield_now();
                ev = self.event.load(Ordering::Acquire);
            }

            if !ptr::eq(ev, notified) {
                Event::with(|event| {
                    let event_ptr = NonNull::from(&*event).as_ptr();
                    assert!(!ptr::eq(event_ptr, notified));

                    if let Ok(_) = self.event.compare_exchange(
                        ptr::null_mut(),
                        event_ptr,
                        Ordering::Release,
                        Ordering::Relaxed,
                    ) {
                        event.wait();
                    }

                    ev = self.event.load(Ordering::Acquire);
                });
            }

            assert!(ptr::eq(ev, notified));
            self.event.store(ptr::null_mut(), Ordering::Relaxed);
        }

        #[cold]
        fn unpark(&self) {
            self.event
                .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |ev| {
                    let notified = NonNull::dangling().as_ptr();
                    if ptr::eq(ev, notified) {
                        None
                    } else {
                        Some(notified)
                    }
                })
                .map(|ev| unsafe {
                    if !ev.is_null() {
                        (*ev).set();
                    }
                })
                .unwrap_or(())
        }
    }

    #[repr(align(2))]
    #[derive(Default)]
    struct Waiter {
        next: Cell<Option<NonNull<Self>>>,
        parker: Parker,
    }

    struct Lock<T> {
        state: AtomicPtr<Waiter>,
        value: UnsafeCell<T>,
    }

    unsafe impl<T: Send> Send for Lock<T> {}
    unsafe impl<T: Send> Sync for Lock<T> {}

    impl<T: Default> Default for Lock<T> {
        fn default() -> Self {
            Self::new(T::default())
        }
    }

    impl<T> Lock<T> {
        const LOCKED: usize = 1;

        const fn new(value: T) -> Self {
            Self {
                state: AtomicPtr::new(ptr::null_mut()),
                value: UnsafeCell::new(value),
            }
        }

        #[inline]
        fn with<F>(&self, f: impl FnOnce(&mut T) -> F) -> F {
            if let Err(_) = self.state.compare_exchange_weak(
                ptr::null_mut::<Waiter>(),
                ptr::null_mut::<Waiter>().with_addr(Self::LOCKED),
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                self.lock_slow();
            }

            let result = f(unsafe { &mut *self.value.get() });

            if let Err(_) = self.state.compare_exchange(
                ptr::null_mut::<Waiter>().with_addr(Self::LOCKED),
                ptr::null_mut::<Waiter>(),
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                unsafe { self.unlock_slow() };
            }

            result
        }

        #[cold]
        fn lock_slow(&self) {
            pinned::<Waiter, _, _>(|waiter| {
                let mut spin = Backoff::default();
                let mut state = self.state.load(Ordering::Relaxed);

                loop {
                    let mut backoff = Backoff::default();
                    while state.addr() & Self::LOCKED == 0 {
                        if let Ok(_) = self.state.compare_exchange(
                            state,
                            state.map_addr(|addr| addr | Self::LOCKED),
                            Ordering::Acquire,
                            Ordering::Relaxed,
                        ) {
                            return;
                        }

                        backoff.yield_now();
                        state = self.state.load(Ordering::Relaxed);
                    }

                    let head = NonNull::new(state.map_addr(|addr| addr & !Self::LOCKED));
                    if head.is_none() && !spin.is_completed() {
                        spin.yield_now();
                        state = self.state.load(Ordering::Relaxed);
                        continue;
                    }

                    let waiter_ptr = NonNull::from(&*waiter).as_ptr();
                    waiter.next.set(head);

                    if let Err(e) = self.state.compare_exchange_weak(
                        state,
                        waiter_ptr.map_addr(|addr| addr | Self::LOCKED),
                        Ordering::Release,
                        Ordering::Relaxed,
                    ) {
                        state = e;
                        continue;
                    }

                    waiter.parker.park();
                    state = self.state.load(Ordering::Relaxed);
                }
            })
        }

        #[cold]
        unsafe fn unlock_slow(&self) {
            loop {
                let state = self.state.load(Ordering::Acquire);
                assert!(state.addr() & Self::LOCKED != 0);

                let head = state.map_addr(|addr| addr & !Self::LOCKED);
                assert!(!head.is_null());

                let next = (*head).next.get().map(|p| p.as_ptr());
                let new_state = next.unwrap_or(ptr::null_mut());

                match self.state.compare_exchange(
                    state,
                    new_state,
                    Ordering::Release,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return (*head).parker.unpark(),
                    Err(_) => spin_loop(),
                }
            }
        }
    }

    #[derive(Default)]
    struct WaitQueue {
        queue: Lock<Option<NonNull<Waiter>>>,
        pending: AtomicBool,
    }

    impl WaitQueue {
        const fn new() -> Self {
            Self {
                queue: Lock::new(None),
                pending: AtomicBool::new(false),
            }
        }

        fn park_if(&self, condition: impl FnOnce() -> bool) {
            pinned::<Waiter, _, _>(|waiter| {
                if self.queue.with(|queue| {
                    fence(Ordering::SeqCst);

                    if !self.pending.load(Ordering::Relaxed) {
                        self.pending.store(true, Ordering::Relaxed);
                    }

                    if !condition() {
                        if queue.is_none() {
                            self.pending.store(false, Ordering::Relaxed);
                        }
                        return false;
                    }

                    waiter.next.set(*queue);
                    *queue = Some(NonNull::from(&*waiter));
                    true
                }) {
                    waiter.parker.park();
                }
            });
        }

        fn unpark_one(&self) {
            fence(Ordering::SeqCst);

            if !self.pending.load(Ordering::Relaxed) {
                return;
            }

            unsafe {
                if let Some(waiter) = self.queue.with(|queue| {
                    let waiter = (*queue)?;
                    *queue = waiter.as_ref().next.get();

                    if queue.is_none() {
                        self.pending.store(false, Ordering::Relaxed);
                    }

                    Some(waiter)
                }) {
                    waiter.as_ref().parker.unpark();
                }
            }
        }
    }

    #[derive(Default)]
    struct Producer {
        position: AtomicUsize,
        wait_queue: WaitQueue,
    }

    #[derive(Default)]
    struct Consumer {
        position: Cell<usize>,
        parker: Parker,
    }

    struct Slot<T> {
        value: UnsafeCell<MaybeUninit<T>>,
        stored: AtomicBool,
    }

    pub struct Queue<T> {
        producer: CachePadded<Producer>,
        consumer: CachePadded<Consumer>,
        slots: CachePadded<Box<[Slot<T>]>>,
    }

    unsafe impl<T: Send> Send for Queue<T> {}
    unsafe impl<T: Send> Sync for Queue<T> {}

    impl<T> Queue<T> {
        pub fn new(capacity: usize) -> Self {
            Self {
                producer: CachePadded(Producer {
                    position: AtomicUsize::new(0),
                    wait_queue: WaitQueue::new(),
                }),
                consumer: CachePadded(Consumer {
                    position: Cell::new(0),
                    parker: Parker::new(),
                }),
                slots: CachePadded({
                    (0..capacity.max(1).next_power_of_two())
                        .map(|_| Slot {
                            value: UnsafeCell::new(MaybeUninit::uninit()),
                            stored: AtomicBool::new(false),
                        })
                        .collect()
                }),
            }
        }

        pub fn try_send(&self, item: T) -> Result<(), T> {
            let mut backoff = Backoff::default();
            let mask = self.slots.len() - 1;

            loop {
                let pos = self.producer.position.load(Ordering::Acquire);
                let slot = unsafe { self.slots.get_unchecked(pos & mask) };

                if slot.stored.load(Ordering::Acquire) {
                    let current_pos = self.producer.position.load(Ordering::Relaxed);
                    if pos == current_pos {
                        return Err(item);
                    }

                    spin_loop();
                    continue;
                }

                if let Err(_) = self.producer.position.compare_exchange(
                    pos,
                    pos.wrapping_add(1),
                    Ordering::Acquire,
                    Ordering::Relaxed,
                ) {
                    backoff.yield_now();
                    continue;
                }

                return Ok(unsafe {
                    slot.value.get().write(MaybeUninit::new(item));
                    slot.stored.store(true, Ordering::Release);
                    self.consumer.parker.unpark();
                });
            }
        }

        pub fn send(&self, mut item: T) {
            loop {
                match self.try_send(item) {
                    Ok(_) => return,
                    Err(e) => item = e,
                }

                self.producer.wait_queue.park_if(|| {
                    let mask = self.slots.len() - 1;
                    let pos = self.producer.position.load(Ordering::Acquire);
                    let slot = unsafe { self.slots.get_unchecked(pos & mask) };

                    if slot.stored.load(Ordering::Acquire) {
                        let current_pos = self.producer.position.load(Ordering::Relaxed);
                        return pos == current_pos;
                    }

                    false
                });
            }
        }

        pub unsafe fn try_recv(&self) -> Option<T> {
            let mask = self.slots.len() - 1;
            let pos = self.consumer.position.get();
            let slot = self.slots.get_unchecked(pos & mask);

            if slot.stored.load(Ordering::Acquire) {
                let value = slot.value.get().read().assume_init();
                slot.stored.store(false, Ordering::Release);

                self.producer.wait_queue.unpark_one();

                self.consumer.position.set(pos.wrapping_add(1));
                return Some(value);
            }

            None
        }

        pub unsafe fn recv(&self) -> T {
            let mut spin = Backoff::default();
            while !spin.is_completed() {
                match self.try_recv() {
                    Some(value) => return value,
                    None => spin.yield_now(),
                }
            }

            loop {
                match self.try_recv() {
                    Some(value) => return value,
                    None => self.consumer.parker.park(),
                }
            }
        }
    }

    impl<T> Drop for Queue<T> {
        fn drop(&mut self) {
            while let Some(value) = unsafe { self.try_recv() } {
                drop(value);
            }
        }
    }
}

pub mod sema {
    use crate::utils::CachePadded;
    use std::{
        cell::{Cell, UnsafeCell},
        mem::{drop, MaybeUninit},
        sync::atomic::{AtomicBool, Ordering},
    };

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    mod atomic {
        use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};

        pub struct Guard(AtomicIsize);

        impl Guard {
            pub fn new(permits: usize) -> Self {
                Self(AtomicIsize::new(permits.try_into().unwrap()))
            }

            pub fn try_acquire(&self) -> bool {
                loop {
                    if self.0.fetch_sub(1, Ordering::Acquire) > 0 {
                        return true;
                    }

                    (0..32).for_each(|_| std::hint::spin_loop());

                    if self.0.fetch_add(1, Ordering::Relaxed) < 0 {
                        return false;
                    }

                    std::thread::yield_now();
                }
            }

            pub fn release(&self) {
                self.0.fetch_add(1, Ordering::Release);
            }
        }

        #[derive(Default)]
        pub struct Counter(AtomicUsize);

        impl Counter {
            pub fn inc_gen(&self) -> usize {
                self.0.fetch_add(1, Ordering::Relaxed)
            }
        }
    }

    #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
    mod atomic {
        use std::sync::atomic::{AtomicUsize, Ordering};

        pub struct Guard(AtomicUsize);

        impl Guard {
            pub fn new(permits: usize) -> Self {
                Self(AtomicUsize::new(permits))
            }

            pub fn try_acquire(&self) -> bool {
                fetch_update(&self.0, Ordering::Acquire, |v| v.checked_sub(1)).is_ok()
            }

            pub fn release(&self) {
                let _ = fetch_update(&self.0, Ordering::Release, |v| Some(v + 1)).unwrap();
            }
        }

        #[derive(Default)]
        pub struct Counter(AtomicUsize);

        impl Counter {
            pub fn inc_gen(&self) -> usize {
                fetch_update(&self.0, Ordering::Relaxed, |v| Some(v + 1)).unwrap()
            }
        }

        #[inline(always)]
        fn fetch_update(
            value: &AtomicUsize,
            success: Ordering,
            mut update: impl FnMut(usize) -> Option<usize>,
        ) -> Result<usize, usize> {
            loop {
                let v = value.load(Ordering::Relaxed);
                let new_v = update(v).ok_or(v)?;
                match value.compare_exchange(v, new_v, success, Ordering::Relaxed) {
                    Ok(_) => return Ok(v),
                    Err(_) => std::thread::yield_now(),
                }
            }
        }
    }

    struct Slot<T> {
        stored: AtomicBool,
        value: UnsafeCell<MaybeUninit<T>>,
    }

    pub struct Queue<T> {
        sema: CachePadded<atomic::Guard>,
        tail: CachePadded<atomic::Counter>,
        head: CachePadded<Cell<usize>>,
        slots: Box<[Slot<T>]>,
    }

    unsafe impl<T: Send> Send for Queue<T> {}
    unsafe impl<T: Send> Sync for Queue<T> {}

    impl<T> Queue<T> {
        pub fn new(capacity: usize) -> Self {
            let capacity = capacity.next_power_of_two();
            Self {
                sema: CachePadded(atomic::Guard::new(capacity)),
                tail: CachePadded(atomic::Counter::default()),
                head: CachePadded(Cell::new(0)),
                slots: (0..capacity)
                    .map(|_| Slot {
                        stored: AtomicBool::new(false),
                        value: UnsafeCell::new(MaybeUninit::uninit()),
                    })
                    .collect(),
            }
        }

        pub fn try_send(&self, value: T) -> Result<(), T> {
            if !self.sema.try_acquire() {
                return Err(value);
            }

            let pos = self.tail.inc_gen();
            let index = pos & (self.slots.len() - 1);

            Ok(unsafe {
                let slot = self.slots.get_unchecked(index);
                slot.value.get().write(MaybeUninit::new(value));
                slot.stored.store(true, Ordering::Release);
            })
        }

        pub unsafe fn try_recv(&self) -> Option<T> {
            let pos = self.head.get();
            let index = pos & (self.slots.len() - 1);

            let slot = self.slots.get_unchecked(index);
            if !slot.stored.load(Ordering::Acquire) {
                return None;
            }

            let value = slot.value.get().read().assume_init();
            slot.stored.store(false, Ordering::Release);

            self.sema.release();

            self.head.set(pos.wrapping_add(1));
            Some(value)
        }
    }

    impl<T> Drop for Queue<T> {
        fn drop(&mut self) {
            while let Some(value) = unsafe { self.try_recv() } {
                drop(value);
            }
        }
    }
}
