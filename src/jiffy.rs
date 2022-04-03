use crate::utils;

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
