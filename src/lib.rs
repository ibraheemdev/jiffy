mod utils;
use utils::{CachePadded, UnsafeDeref};

pub mod bounded;

use std::cell::UnsafeCell;
use std::mem::{self, MaybeUninit};
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU8, AtomicUsize, Ordering};

const BLOCK: usize = 1620;

pub struct Queue<T> {
    head_block: UnsafeCell<*mut Block<T>>,
    tail_block: CachePadded<AtomicPtr<Block<T>>>,
    tail: CachePadded<AtomicUsize>,
}

unsafe impl<T: Send> Send for Queue<T> {}
unsafe impl<T: Send> Sync for Queue<T> {}

impl<T> Queue<T> {
    pub fn new() -> Queue<T> {
        let head = Block::alloc(1, ptr::null_mut());

        Queue {
            head_block: UnsafeCell::new(head),
            tail_block: CachePadded(AtomicPtr::new(head)),
            tail: CachePadded(AtomicUsize::new(0)),
        }
    }

    pub fn push(&self, value: T) {
        // acquire an index into the queue
        let index = self.tail.fetch_add(1, Ordering::Relaxed);

        // whether we had to backtrack to a previous block
        let mut backtracked = false;

        loop {
            let mut block_ptr = self.tail_block.load(Ordering::Acquire);
            let mut block = unsafe { block_ptr.deref() };

            // the number of slots in the queue, excluding
            // the current block
            let mut prev_slots = BLOCK * (block.position - 1);

            // the index is in a previous block, backtrack until we find it
            while index < prev_slots {
                backtracked = true;
                block_ptr = block.prev;
                block = unsafe { block_ptr.deref() };
                prev_slots -= BLOCK;
            }

            // the number of slots in the queue
            let slots = BLOCK + prev_slots;

            // we're in the correct block
            if prev_slots <= index && index < slots {
                let index = index - prev_slots;

                // write the value
                unsafe {
                    let slot = block.slots.get_unchecked(index);
                    slot.value.get().write(MaybeUninit::new(value));
                    slot.state.store(Slot::ACTIVE, Ordering::Release);
                }

                // pre-allocate the next block
                if index == 1 && !backtracked {
                    let next = Block::alloc(block.position + 1, block_ptr);

                    if block
                        .next
                        .compare_exchange(
                            ptr::null_mut(),
                            next,
                            Ordering::Release,
                            Ordering::Relaxed,
                        )
                        .is_err()
                    {
                        // lost the race, free our block
                        unsafe { Block::dealloc(next) };
                    }
                }

                return;
            }

            // the index is in the next block
            if index >= slots {
                let next = block.next.load(Ordering::Acquire);

                // we have to allocate the next block
                if next.is_null() {
                    let next = Block::alloc(block.position + 1, block_ptr);

                    match block.next.compare_exchange(
                        ptr::null_mut(),
                        next,
                        Ordering::Release,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => {
                            // update the tail
                            self.tail_block.store(next, Ordering::Release);
                        }
                        Err(_) => {
                            // lost the race, free our block
                            unsafe { Block::dealloc(next) };
                        }
                    }
                } else {
                    // someone else already allocated, help
                    // move to the next buffer
                    let _ = self.tail_block.compare_exchange(
                        block_ptr,
                        next,
                        Ordering::Release,
                        Ordering::Relaxed,
                    );
                }

                continue;
            }
        }
    }

    pub unsafe fn pop(&self) -> Option<T> {
        loop {
            let tail_block = self.tail_block.load(Ordering::Acquire);

            // the number of slots in the queue, excluding
            // the current block
            let prev_slots = BLOCK * (tail_block.deref().position - 1);

            // the queue is empty
            if ptr::eq(self.head_block(), tail_block)
                && self.head_block().deref().head()
                    == (self.tail.load(Ordering::Acquire) - prev_slots)
            {
                return None;
            }

            // there are elements in this block
            if self.head_block().deref().head() < BLOCK {
                let head_slot = self
                    .head_block()
                    .deref()
                    .slots
                    .get_unchecked(self.head_block().deref().head());

                // this slot has already been taken
                if head_slot.state.load(Ordering::Acquire) == Slot::TAKEN {
                    self.head_block().deref().advance_head();
                    continue;
                }

                let mut block = self.head_block();
                let mut head = block.deref().head();

                // whether we moved to the next block
                let mut went_to_next = false;
                // whether we found an entire block of taken slots
                let mut block_taken = true;

                // search for a set element
                while head_slot.state.load(Ordering::Acquire) == Slot::INACTIVE {
                    // there are slots in this block
                    if head < BLOCK {
                        let mut candidate = block.deref().slots.get_unchecked(head);
                        head += 1;

                        // found an active slot, and the original is still inactive
                        if candidate.state.load(Ordering::Acquire) == Slot::ACTIVE
                            && head_slot.state.load(Ordering::Acquire) == Slot::INACTIVE
                        {
                            let mut scan_block = self.head_block();
                            let mut scan_head = scan_block.deref().head();

                            // scan all slots from the head of the queue
                            // in case we find one that was activated
                            // before this one
                            while !ptr::eq(scan_block, block)
                                || scan_head < (head - 1)
                                    && head_slot.state.load(Ordering::Acquire) == Slot::INACTIVE
                            {
                                // reached the end of the block
                                //
                                // start scanning the next one
                                if scan_head >= BLOCK {
                                    scan_block = scan_block.deref().next.load(Ordering::Acquire);
                                    scan_head = scan_block.deref().head();
                                    continue;
                                }

                                let scan_slot = scan_block.deref().slots.get_unchecked(scan_head);

                                // we found a different slot
                                //
                                // rescan until this slot to in case
                                // we find one that was activated
                                // before this one
                                if scan_slot.state.load(Ordering::Acquire) == Slot::ACTIVE {
                                    head = scan_head;
                                    block = scan_block;
                                    candidate = scan_slot;
                                    scan_block = self.head_block();
                                    scan_head = scan_block.deref().head();
                                }

                                scan_head += 1;
                            }

                            // the original slot was activated
                            if head_slot.state.load(Ordering::Acquire) == Slot::ACTIVE {
                                break;
                            }

                            // we scanned and nothing changed,
                            // read the slot we found
                            let value = candidate.take();

                            // set `TAKEN` to avoid taking it
                            // this slot again in the future
                            candidate.state.store(Slot::TAKEN, Ordering::Release);

                            // if we moved to a new block, move the head forward
                            if went_to_next && (head - 1) == block.deref().head() {
                                block.deref().advance_head();
                            }

                            return Some(value);
                        }

                        // this slot is inactive, continue searching
                        if candidate.state.load(Ordering::Acquire) == Slot::INACTIVE {
                            block_taken = false;
                        }
                    }

                    // we reached the end of the block
                    if head >= BLOCK {
                        // we scanned a block and found that
                        // all slots were taken, free it
                        if block_taken && went_to_next {
                            // reached the end of the queue
                            if ptr::eq(block, self.tail_block.load(Ordering::Acquire)) {
                                return None;
                            }

                            let next = block.deref().next.load(Ordering::Acquire);

                            // reached the end of the queue
                            if next.is_null() {
                                return None;
                            }

                            // unlink the block
                            let prev = block.deref().prev;
                            next.deref_mut().prev = prev;
                            prev.deref().next.store(next, Ordering::Release);

                            // free the block
                            Block::dealloc(block);

                            // move to the next block
                            block = next;
                            head = block.deref().head();

                            block_taken = true;
                            went_to_next = true;
                        } else {
                            // otherwise, move to the next block
                            let next = block.deref().next.load(Ordering::Acquire);

                            // reached the end of the queue
                            if next.is_null() {
                                return None;
                            }

                            block = next;
                            head = block.deref().head();

                            block_taken = true;
                            went_to_next = true;
                        }
                    }
                }

                // the head slot is active
                if head_slot.state.load(Ordering::Acquire) == Slot::ACTIVE {
                    self.head_block().deref().advance_head();
                    // note that we don't have to mark this slot
                    // as `TAKEN` because we advanced `head`
                    return Some(head_slot.take());
                }
            }

            // reached the end of the head block
            if self.head_block().deref().head() >= BLOCK {
                // there is only one buffer
                if ptr::eq(self.head_block(), self.tail_block.load(Ordering::Acquire)) {
                    return None;
                }

                let next = self.head_block().deref().next.load(Ordering::Acquire);

                // reached the end of the queue
                if next.is_null() {
                    return None;
                }

                // deallocate this block and move to the next
                Block::dealloc(self.head_block());
                *self.head_block.get() = next;
            }
        }
    }

    unsafe fn head_block(&self) -> *mut Block<T> {
        *self.head_block.get()
    }
}

struct Block<T> {
    slots: CachePadded<[Slot<T>; BLOCK]>,
    next: CachePadded<AtomicPtr<Block<T>>>,
    prev: *mut Block<T>,
    head: UnsafeCell<usize>,
    position: usize,
}

impl<T> Block<T> {
    fn alloc(position: usize, prev: *mut Block<T>) -> *mut Block<T> {
        let mut block = unsafe { utils::box_zeroed::<Block<T>>() };
        block.position = position;
        block.prev = prev;
        Box::into_raw(block)
    }

    unsafe fn dealloc(block: *mut Block<T>) {
        let _ = Box::from_raw(block);
    }

    unsafe fn head(&self) -> usize {
        *self.head.get()
    }

    unsafe fn advance_head(&self) {
        *self.head.get() += 1
    }
}

struct Slot<T> {
    value: UnsafeCell<MaybeUninit<T>>,
    state: AtomicU8,
}

impl<T> Slot<T> {
    unsafe fn take(&self) -> T {
        mem::replace(&mut *self.value.get(), MaybeUninit::uninit()).assume_init()
    }
}

impl Slot<()> {
    const INACTIVE: u8 = 0;
    const ACTIVE: u8 = 1;
    const TAKEN: u8 = 2;
}

pub mod protty {
    use crate::utils::CachePadded;
    use std::{
        cell::{Cell, UnsafeCell},
        hint::spin_loop,
        marker::{PhantomData, PhantomPinned},
        mem::{drop, MaybeUninit},
        num::NonZeroUsize,
        pin::Pin,
        ptr::{self, NonNull},
        sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering},
        thread,
    };

    #[derive(Default)]
    struct SpinWait {
        counter: u32,
    }

    impl SpinWait {
        fn try_yield_now(&mut self) -> bool {
            if !Self::should_spin() {
                return false;
            }

            if self.counter >= 100 {
                return false;
            }

            self.counter += 1;
            spin_loop();
            true
        }

        fn yield_now(&mut self) {
            if !Self::should_spin() {
                return;
            }

            self.counter = (self.counter + 1).min(5);
            for _ in 0..(1 << self.counter) {
                spin_loop();
            }
        }

        fn should_spin() -> bool {
            static NUM_CPUS: AtomicUsize = AtomicUsize::new(0);

            let num_cpus =
                NonZeroUsize::new(NUM_CPUS.load(Ordering::Relaxed)).unwrap_or_else(|| {
                    let num_cpus = thread::available_parallelism()
                        .ok()
                        .or(NonZeroUsize::new(1))
                        .unwrap();

                    NUM_CPUS.store(num_cpus.get(), Ordering::Relaxed);
                    num_cpus
                });

            num_cpus.get() > 1
        }
    }

    #[derive(Default)]
    struct Event {
        thread: Cell<Option<thread::Thread>>,
        is_set: AtomicBool,
        _pinned: PhantomPinned,
    }

    impl Event {
        fn with<F>(f: impl FnOnce(Pin<&Self>) -> F) -> F {
            let event = Self::default();
            event.thread.set(Some(thread::current()));
            f(unsafe { Pin::new_unchecked(&event) })
        }

        fn wait(&self) {
            while !self.is_set.load(Ordering::Acquire) {
                thread::park();
            }
        }

        fn set(&self) {
            let is_set_ptr = NonNull::from(&self.is_set).as_ptr();
            let thread = self.thread.take();
            drop(self);

            unsafe { (*is_set_ptr).store(true, Ordering::Release) };
            let thread = thread.expect("Event without a thread");
            thread.unpark()
        }
    }

    #[derive(Default)]
    struct Parker {
        event: AtomicPtr<Event>,
    }

    impl Parker {
        #[inline]
        fn park(&self) {
            let mut ev = self.event.load(Ordering::Acquire);
            let notified = NonNull::dangling().as_ptr();

            // let mut spin = SpinWait::default();
            // while !ptr::eq(ev, notified) && spin.try_yield_now() {
            //     ev = self.event.load(Ordering::Acquire);
            // }

            if !ptr::eq(ev, notified) {
                ev = self.park_slow();
            }

            assert!(ptr::eq(ev, notified));
            self.event.store(ptr::null_mut(), Ordering::Relaxed);
        }

        #[cold]
        fn park_slow(&self) -> *mut Event {
            Event::with(|event| {
                let event_ptr = NonNull::from(&*event).as_ptr();
                let notified = NonNull::dangling().as_ptr();
                assert!(!ptr::eq(event_ptr, notified));

                match self.event.compare_exchange(
                    ptr::null_mut(),
                    event_ptr,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Err(ev) => ev,
                    Ok(_) => {
                        event.wait();
                        self.event.load(Ordering::Acquire)
                    }
                }
            })
        }

        unsafe fn unpark(&self) {
            let event_ptr = NonNull::from(&self.event).as_ptr();
            let notified = NonNull::dangling().as_ptr();
            drop(self);

            let ev = (*event_ptr).swap(notified, Ordering::AcqRel);
            assert!(!ptr::eq(ev, notified), "multiple threads unparked Parker");

            if !ev.is_null() {
                (*ev).set();
            }
        }
    }

    #[derive(Default)]
    struct Waiter {
        next: Cell<Option<NonNull<Self>>>,
        parker: Parker,
        _pinned: PhantomPinned,
    }

    #[derive(Default)]
    struct WaitList {
        top: AtomicPtr<Waiter>,
    }

    impl WaitList {
        fn wait_while(&self, mut should_wait: impl FnMut() -> bool) {
            let waiter = Waiter::default();
            let waiter = unsafe { Pin::new_unchecked(&waiter) };

            let mut spin = SpinWait::default();
            let mut top = self.top.load(Ordering::Relaxed);

            while should_wait() {
                if top.is_null() && spin.try_yield_now() {
                    top = self.top.load(Ordering::Relaxed);
                    continue;
                }

                waiter.next.set(NonNull::new(top));
                if let Err(e) = self.top.compare_exchange_weak(
                    top,
                    NonNull::from(&*waiter).as_ptr(),
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    top = e;
                    continue;
                }

                if !should_wait() {
                    self.wake_all();
                }

                waiter.parker.park();
                top = self.top.load(Ordering::Relaxed);
            }
        }

        fn wake_all(&self) {
            let mut top = self.top.load(Ordering::Relaxed);
            while !top.is_null() {
                match self.top.compare_exchange_weak(
                    top,
                    ptr::null_mut(),
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(e) => top = e,
                }
            }

            while !top.is_null() {
                unsafe {
                    let parker = NonNull::from(&(*top).parker).as_ptr();
                    let next = (*top).next.get().map(|ptr| ptr.as_ptr());

                    (*parker).unpark();
                    top = next.unwrap_or(ptr::null_mut());
                }
            }
        }
    }

    struct Value<T>(UnsafeCell<MaybeUninit<T>>);

    impl<T> Value<T> {
        const INIT: Self = Self(UnsafeCell::new(MaybeUninit::uninit()));

        unsafe fn write(&self, value: T) {
            self.0.get().write(MaybeUninit::new(value))
        }

        unsafe fn read(&self) -> T {
            self.0.get().read().assume_init()
        }
    }

    const LAP: usize = 1024;
    const BLOCK_CAP: usize = LAP - 1;

    struct Block<T> {
        values: [Value<T>; BLOCK_CAP],
        stored: [AtomicBool; BLOCK_CAP],
        next: AtomicPtr<Self>,
    }

    impl<T> Block<T> {
        const fn new() -> Self {
            const UNSTORED: AtomicBool = AtomicBool::new(false);
            Self {
                values: [Value::<T>::INIT; BLOCK_CAP],
                stored: [UNSTORED; BLOCK_CAP],
                next: AtomicPtr::new(ptr::null_mut()),
            }
        }
    }

    #[derive(Default)]
    struct Producer<T> {
        position: AtomicUsize,
        block: AtomicPtr<Block<T>>,
        waiters: WaitList,
    }

    #[derive(Default)]
    struct Consumer<T> {
        block: AtomicPtr<Block<T>>,
        index: Cell<usize>,
    }

    #[derive(Default)]
    pub struct MpscQueue<T> {
        producer: CachePadded<Producer<T>>,
        consumer: CachePadded<Consumer<T>>,
        _marker: PhantomData<T>,
    }

    unsafe impl<T: Send> Send for MpscQueue<T> {}
    unsafe impl<T: Send> Sync for MpscQueue<T> {}

    impl<T> Drop for MpscQueue<T> {
        fn drop(&mut self) {
            unsafe {
                while let Some(value) = self.pop() {
                    drop(value);
                }

                let block = self.consumer.block.load(Ordering::Relaxed);
                if !block.is_null() {
                    drop(Box::from_raw(block));
                }
            }
        }
    }

    impl<T> MpscQueue<T> {
        pub fn push(&self, item: T) {
            let mut next_block = None;
            let mut spin = SpinWait::default();

            loop {
                let position = self.producer.position.load(Ordering::Acquire);
                let index = position % LAP;

                if index == BLOCK_CAP {
                    let should_wait = || {
                        let current_pos = self.producer.position.load(Ordering::Relaxed);
                        current_pos % LAP == BLOCK_CAP
                    };

                    self.producer.waiters.wait_while(should_wait);
                    continue;
                }

                if index + 1 == BLOCK_CAP && next_block.is_none() {
                    next_block = Some(Box::new(Block::new()));
                }

                let mut block = self.producer.block.load(Ordering::Acquire);
                if block.is_null() {
                    let new_block = Box::into_raw(Box::new(Block::new()));

                    if let Err(_) = self.producer.block.compare_exchange(
                        ptr::null_mut(),
                        new_block,
                        Ordering::Release,
                        Ordering::Relaxed,
                    ) {
                        drop(unsafe { Box::from_raw(new_block) });
                        continue;
                    }

                    block = new_block;
                    self.consumer.block.store(new_block, Ordering::Release);
                }

                let new_position = position.wrapping_add(1);
                if let Err(_) = self.producer.position.compare_exchange_weak(
                    position,
                    new_position,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    spin.yield_now();
                    continue;
                }

                unsafe {
                    if index + 1 == BLOCK_CAP {
                        let next_block = Box::into_raw(next_block.unwrap());
                        self.producer.block.store(next_block, Ordering::Release);
                        (*block).next.store(next_block, Ordering::Release);

                        let next_position = new_position.wrapping_add(1);
                        self.producer
                            .position
                            .store(next_position, Ordering::Release);
                        self.producer.waiters.wake_all();
                    }

                    let value = NonNull::from((*block).values.get_unchecked(index)).as_ptr();
                    let stored = NonNull::from((*block).stored.get_unchecked(index)).as_ptr();

                    (*value).write(item);
                    (*stored).store(true, Ordering::Release);
                    return;
                }
            }
        }

        pub unsafe fn pop(&self) -> Option<T> {
            let mut block = self.consumer.block.load(Ordering::Acquire);
            if block.is_null() {
                return None;
            }

            let mut index = self.consumer.index.get();
            if index == BLOCK_CAP {
                let next = (*block).next.load(Ordering::Acquire);
                if next.is_null() {
                    return None;
                }

                drop(Box::from_raw(block));
                block = next;
                index = 0;

                self.consumer.index.set(index);
                self.consumer.block.store(block, Ordering::Relaxed);
            }

            let value = (*block).values.get_unchecked(index);
            let stored = (*block).stored.get_unchecked(index);

            if stored.load(Ordering::Acquire) {
                self.consumer.index.set(index + 1);
                return Some(value.read());
            }

            None
        }
    }
}

pub mod thing {

    use crate::utils::CachePadded;
    use std::{
        cell::{Cell, UnsafeCell},
        collections::VecDeque,
        hint::spin_loop,
        marker::PhantomPinned,
        mem::{drop, swap},
        num::NonZeroUsize,
        pin::Pin,
        ptr::{self, NonNull},
        sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering},
        thread,
    };

    unsafe trait StrictProvenance: Sized {
        fn addr(self) -> usize;
        fn with_addr(self, addr: usize) -> Self;
        fn map_addr(self, f: impl FnOnce(usize) -> usize) -> Self;
    }

    unsafe impl<T> StrictProvenance for *mut T {
        fn addr(self) -> usize {
            self as usize
        }

        fn with_addr(self, addr: usize) -> Self {
            addr as Self
        }

        fn map_addr(self, f: impl FnOnce(usize) -> usize) -> Self {
            self.with_addr(f(self.addr()))
        }
    }

    fn pinned<P: Default, T, F: FnOnce(Pin<&P>) -> T>(f: F) -> T {
        let pinnable = P::default();
        f(unsafe { Pin::new_unchecked(&pinnable) })
    }

    fn num_cpus() -> NonZeroUsize {
        static CPUS: AtomicUsize = AtomicUsize::new(0);

        NonZeroUsize::new(CPUS.load(Ordering::Relaxed)).unwrap_or_else(|| {
            let cpus = thread::available_parallelism()
                .ok()
                .or(NonZeroUsize::new(1))
                .unwrap();

            CPUS.store(cpus.get(), Ordering::Relaxed);
            cpus
        })
    }

    #[derive(Default)]
    struct Backoff {
        counter: usize,
    }

    impl Backoff {
        fn try_yield_now(&mut self) -> bool {
            self.counter < 32 && {
                self.counter += 1;
                spin_loop();
                true
            }
        }

        fn yield_now(&mut self) {
            self.counter = self.counter.wrapping_add(1);
            if self.counter <= 3 {
                (0..(1 << self.counter)).for_each(|_| spin_loop());
            } else if cfg!(windows) {
                (0..(1 << self.counter.min(5))).for_each(|_| spin_loop());
            } else {
                thread::yield_now();
            }
        }
    }

    struct Parker {
        thread: Cell<Option<thread::Thread>>,
        is_unparked: AtomicBool,
        _pinned: PhantomPinned,
    }

    impl Default for Parker {
        fn default() -> Self {
            Self {
                thread: Cell::new(Some(thread::current())),
                is_unparked: AtomicBool::new(false),
                _pinned: PhantomPinned,
            }
        }
    }

    impl Parker {
        fn park(&self) {
            while !self.is_unparked.load(Ordering::Acquire) {
                thread::park();
            }
        }

        unsafe fn unpark(&self) {
            let is_unparked = NonNull::from(&self.is_unparked).as_ptr();
            let thread = self.thread.take().unwrap();
            drop(self);

            (*is_unparked).store(true, Ordering::Release);
            thread.unpark();
        }
    }

    #[repr(align(2))]
    #[derive(Default)]
    struct Waiter {
        next: Cell<Option<NonNull<Self>>>,
        parker: AtomicPtr<Parker>,
        _pinned: PhantomPinned,
    }

    impl Waiter {
        fn park(&self) {
            let mut p = self.parker.load(Ordering::Acquire);
            let notified = NonNull::dangling().as_ptr();

            if !ptr::eq(p, notified) {
                p = pinned::<Parker, _, _>(|parker| {
                    let parker_ptr = NonNull::from(&*parker).as_ptr();
                    assert!(!ptr::eq(parker_ptr, notified));

                    if let Err(p) = self.parker.compare_exchange(
                        ptr::null_mut(),
                        parker_ptr,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        return p;
                    }

                    parker.park();
                    self.parker.load(Ordering::Acquire)
                });
            }

            assert!(ptr::eq(p, notified));
            self.parker.store(ptr::null_mut(), Ordering::Relaxed);
        }

        unsafe fn unpark(&self) {
            let parker = NonNull::from(&self.parker).as_ptr();
            drop(self);

            let notified = NonNull::dangling().as_ptr();
            let parker = (*parker).swap(notified, Ordering::AcqRel);

            assert!(!ptr::eq(parker, notified));
            if !parker.is_null() {
                (*parker).unpark();
            }
        }
    }

    struct Lock<T> {
        state: AtomicPtr<Waiter>,
        value: UnsafeCell<T>,
    }

    unsafe impl<T: Send> Send for Lock<T> {}
    unsafe impl<T: Send> Sync for Lock<T> {}

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
            if !self.lock_fast() {
                self.lock_slow();
            }

            let result = f(unsafe { &mut *self.value.get() });

            if !self.unlock_fast() {
                self.unlock_slow();
            }

            result
        }

        #[inline(always)]
        fn lock_fast(&self) -> bool {
            self.state
                .compare_exchange_weak(
                    ptr::null_mut::<Waiter>(),
                    ptr::null_mut::<Waiter>().with_addr(Self::LOCKED),
                    Ordering::Acquire,
                    Ordering::Relaxed,
                )
                .is_ok()
        }

        #[cold]
        fn lock_slow(&self) {
            pinned::<Waiter, _, _>(|waiter| {
                let mut spin = Backoff::default();
                let mut state = self.state.load(Ordering::Relaxed);

                loop {
                    let mut backoff = Backoff::default();
                    while state.addr() & Self::LOCKED == 0 {
                        if let Ok(_) = self.state.compare_exchange_weak(
                            state,
                            state.map_addr(|ptr| ptr | Self::LOCKED),
                            Ordering::Acquire,
                            Ordering::Relaxed,
                        ) {
                            return;
                        }

                        backoff.yield_now();
                        state = self.state.load(Ordering::Relaxed);
                    }

                    let head = state.map_addr(|ptr| ptr & !Self::LOCKED);
                    if head.is_null() && spin.try_yield_now() {
                        state = self.state.load(Ordering::Relaxed);
                        continue;
                    }

                    let waiter_ptr = NonNull::from(&*waiter).as_ptr();
                    waiter.next.set(NonNull::new(head));

                    if let Err(e) = self.state.compare_exchange_weak(
                        state,
                        waiter_ptr.map_addr(|ptr| ptr | Self::LOCKED),
                        Ordering::Release,
                        Ordering::Relaxed,
                    ) {
                        state = e;
                        continue;
                    }

                    waiter.park();
                    state = self.state.load(Ordering::Relaxed);
                }
            })
        }

        #[inline(always)]
        fn unlock_fast(&self) -> bool {
            self.state
                .compare_exchange(
                    ptr::null_mut::<Waiter>().with_addr(Self::LOCKED),
                    ptr::null_mut::<Waiter>(),
                    Ordering::Release,
                    Ordering::Relaxed,
                )
                .is_ok()
        }

        #[cold]
        fn unlock_slow(&self) {
            unsafe {
                let mut state = self.state.load(Ordering::Acquire);
                loop {
                    assert_ne!(state.addr() & Self::LOCKED, 0);

                    let waiter = state.map_addr(|ptr| ptr & !Self::LOCKED);
                    assert!(!waiter.is_null());

                    let next = (*waiter).next.get();
                    let next = next.map(|ptr| ptr.as_ptr()).unwrap_or(ptr::null_mut());
                    assert_eq!(next.addr() & Self::LOCKED, 0);

                    match self.state.compare_exchange_weak(
                        state,
                        next,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => return (*waiter).unpark(),
                        Err(e) => state = e,
                    }
                }
            }
        }
    }

    struct Producer<T> {
        pending: AtomicBool,
        deque: Lock<VecDeque<T>>,
    }

    impl<T> Producer<T> {
        fn new() -> Self {
            Self {
                pending: AtomicBool::new(false),
                deque: Lock::new(VecDeque::new()),
            }
        }

        fn push(&self, item: T) {
            self.deque.with(|deque| deque.push_back(item));
            self.pending.store(true, Ordering::Release);
        }

        fn swap(&self, new: &mut VecDeque<T>) {
            if self.pending.load(Ordering::Acquire) {
                self.pending.store(false, Ordering::Relaxed);
                self.deque.with(|deque| swap(new, deque));
            }
        }
    }

    pub struct MpscQueue<T> {
        index: AtomicUsize,
        consumer: UnsafeCell<VecDeque<T>>,
        producers: Box<[CachePadded<Producer<T>>]>,
    }

    unsafe impl<T: Send> Send for MpscQueue<T> {}
    unsafe impl<T: Send> Sync for MpscQueue<T> {}

    impl<T> Default for MpscQueue<T> {
        fn default() -> Self {
            Self {
                index: AtomicUsize::new(0),
                consumer: UnsafeCell::new(VecDeque::new()),
                producers: (0..num_cpus().get())
                    .map(|_| CachePadded(Producer::new()))
                    .collect(),
            }
        }
    }

    impl<T> MpscQueue<T> {
        pub fn push(&self, value: T) {
            let index = self.index.fetch_add(1, Ordering::Relaxed);
            assert_ne!(self.producers.len(), 0);

            let producer = &self.producers[index % self.producers.len()];
            producer.push(value);
        }

        pub unsafe fn pop(&self) -> Option<T> {
            let consumer = &mut *self.consumer.get();
            if let Some(item) = consumer.pop_front() {
                return Some(item);
            }

            for producer in self.producers.iter() {
                producer.swap(consumer);
                if consumer.len() > 0 {
                    break;
                }
            }

            consumer.pop_front()
        }
    }
}
