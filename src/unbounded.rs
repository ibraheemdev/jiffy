use crate::utils::{self, CachePadded, FetchAddPtr, StrictProvenance, UnsafeDeref};

use std::alloc::{self, Layout};
use std::cell::UnsafeCell;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::sync::atomic::{AtomicPtr, AtomicU32, AtomicU8, Ordering};

pub struct Queue<T> {
    head: CachePadded<AtomicPtr<Block<T>>>,
    tail: CachePadded<AtomicPtr<Block<T>>>,
    cached_tail: AtomicPtr<Block<T>>,
}

impl<T> Queue<T> {
    pub fn new() -> Queue<T> {
        let block = Block::alloc();
        let with_zero_index = (block as usize) | (INDEX_MASK & 0);

        Queue {
            head: CachePadded(AtomicPtr::new(with_zero_index as _)),
            tail: CachePadded(AtomicPtr::new(with_zero_index as _)),
            cached_tail: AtomicPtr::new(block),
        }
    }

    pub fn push(&self, value: T) {
        let value = ManuallyDrop::new(value);
        loop {
            let tail = self.tail.fetch_add(1, Ordering::Acquire);
            let tail = tail.map_addr(|addr| addr & !INDEX_MASK);
            let index = tail.addr() & INDEX_MASK;

            if index < BLOCK_SIZE {
                unsafe {
                    let slot = tail.deref().slots.get_unchecked(index);

                    slot.value
                        .get()
                        .deref_mut()
                        .as_mut_ptr()
                        .copy_from_nonoverlapping(&*value, 1);

                    let state = slot.state.fetch_add(Slot::WRITER, Ordering::Release);

                    if matches!(state, Slot::INACTIVE | Slot::RESUME) {
                        return;
                    }

                    if state == (Slot::NO_WRITER_YET | Slot::CONSUMED_OR_INVALIDATED | Slot::RESUME)
                    {
                        (*tail).try_reclaim(index + 1);
                    }

                    continue;
                }
            }
        }
    }
}

const BLOCK_SIZE: usize = 1024;
const INDEX_MASK: usize = (0x1 << 12) - 1;

#[repr(align(4096))] // reserve 12 bits (0x1 << 12)
struct Block<T> {
    slots: [Slot<T>; BLOCK_SIZE],
    control: Control,
    next: AtomicPtr<Block<T>>,
}

impl<T> Block<T> {
    fn alloc() -> *mut Block<T> {
        let layout = Layout::new::<Block<T>>();
        let ptr = unsafe { alloc::alloc_zeroed(layout) };

        if ptr.is_null() {
            alloc::handle_alloc_error(layout);
        }

        ptr as _
    }

    fn try_reclaim(&self, start: usize) {
        for slot in &self.slots[start..] {
            if slot.state.load(Ordering::Acquire) & Slot::CONSUMED != Slot::CONSUMED {
                if slot.state.fetch_add(Slot::RESUME, Ordering::Relaxed) & Slot::CONSUMED
                    != Slot::CONSUMED
                {
                    return;
                }
            }
        }
    }
}

struct Slot<T> {
    value: UnsafeCell<MaybeUninit<T>>,
    state: AtomicU8,
}

impl Slot<()> {
    const INACTIVE: u8 = 0;
    const RESUME: u8 = 0b0001;
    const WRITER: u8 = 0b0010;
    const CONSUMED_OR_INVALIDATED: u8 = 0b0100;
    const NO_WRITER_YET: u8 = 0b1000;
    const CONSUMED: u8 = Slot::WRITER | Slot::CONSUMED_OR_INVALIDATED;
}

struct Control {
    push: AtomicU32,
    pop: AtomicU32,
    reclaim: AtomicU32,
}
