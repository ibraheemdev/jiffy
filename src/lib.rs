#![allow(unused, dead_code)]

pub mod bounded;
pub mod unbounded;
pub mod utils;

pub mod protty_looish {
    use crate::utils::{CachePadded, FetchAddPtr, StrictProvenance};
    use std::{
        cell::{Cell, UnsafeCell},
        mem::{drop, MaybeUninit},
        ptr::{self, NonNull},
        sync::atomic::{AtomicBool, AtomicPtr, Ordering},
    };

    struct Value<T>(UnsafeCell<MaybeUninit<T>>);

    impl<T> Value<T> {
        const EMPTY: Self = Self(UnsafeCell::new(MaybeUninit::uninit()));
    }

    // max sender threads = BLOCK_ALIGN - BLOCK_SIZE - 1
    // thats how much the fetch_add() in push() is allowed to bump until it overflows into the block ptr.
    const BLOCK_ALIGN: usize = 4096;
    const BLOCK_MASK: usize = !(BLOCK_ALIGN - 1);
    const BLOCK_SIZE: usize = 1024;

    #[repr(align(4096))]
    struct Block<T> {
        values: [Value<T>; BLOCK_SIZE],
        stored: [AtomicBool; BLOCK_SIZE],
        next: AtomicPtr<Self>,
    }

    impl<T> Block<T> {
        const UNSTORED: AtomicBool = AtomicBool::new(false);
        const EMPTY: Self = Self {
            values: [Value::<T>::EMPTY; BLOCK_SIZE],
            stored: [Self::UNSTORED; BLOCK_SIZE],
            next: AtomicPtr::new(ptr::null_mut()),
        };
    }

    struct Producer<T> {
        block_and_index: AtomicPtr<Block<T>>,
    }

    struct Consumer<T> {
        block: AtomicPtr<Block<T>>,
        index: Cell<usize>,
    }

    pub struct Queue<T> {
        producer: CachePadded<Producer<T>>,
        consumer: CachePadded<Consumer<T>>,
    }

    unsafe impl<T: Send> Send for Queue<T> {}
    unsafe impl<T: Send> Sync for Queue<T> {}

    impl<T> Queue<T> {
        pub const EMPTY: Self = Self {
            producer: CachePadded(Producer {
                block_and_index: AtomicPtr::new(ptr::null_mut()),
            }),
            consumer: CachePadded(Consumer {
                block: AtomicPtr::new(ptr::null_mut()),
                index: Cell::new(0),
            }),
        };

        pub fn send(&self, value: T) {
            let mut allocated_block = ptr::null_mut::<Block<T>>();

            let (block, index, prev_block_next) = 'reserved: loop {
                let mut block_and_index = self
                    .producer
                    .block_and_index
                    .fetch_add(1, Ordering::Acquire);
                let block = block_and_index.map_addr(|addr| addr & BLOCK_MASK);
                let index = block_and_index.addr() & !BLOCK_MASK;

                if !block.is_null() && index < BLOCK_SIZE {
                    assert_ne!(index, 0);
                    break 'reserved (block, index, None);
                }

                if allocated_block.is_null() {
                    allocated_block = Box::into_raw(Box::new(Block::EMPTY));
                }

                loop {
                    let block = block_and_index.map_addr(|addr| addr & BLOCK_MASK);

                    if ptr::eq(block, allocated_block) {
                        break;
                    }

                    let index = block_and_index.addr() & !BLOCK_MASK;
                    if !block.is_null() && index < BLOCK_SIZE {
                        break;
                    }

                    if let Err(e) = self.producer.block_and_index.compare_exchange_weak(
                        block_and_index,
                        allocated_block.map_addr(|addr| addr | 1),
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                    ) {
                        block_and_index = e;
                        continue;
                    }

                    let prev_block_next = match NonNull::new(block) {
                        Some(prev_block) => NonNull::from(unsafe { &(*block).next }),
                        None => NonNull::from(&self.consumer.block),
                    };

                    break 'reserved (allocated_block, 0, Some(prev_block_next));
                }
            };

            unsafe {
                assert!(index < BLOCK_SIZE);
                let slot = (*block).values.get_unchecked(index);
                slot.0.get().write(MaybeUninit::new(value));

                let stored = (*block).stored.get_unchecked(index);
                assert!(!stored.load(Ordering::Relaxed));
                stored.store(true, Ordering::Release);

                if let Some(prev_block_next) = prev_block_next {
                    assert!(prev_block_next.as_ref().load(Ordering::Relaxed).is_null());
                    prev_block_next.as_ref().store(block, Ordering::Release);
                }

                if !allocated_block.is_null() && !ptr::eq(block, allocated_block) {
                    drop(Box::from_raw(allocated_block));
                }
            }
        }

        pub unsafe fn try_recv(&self) -> Option<T> {
            let mut block = self.consumer.block.load(Ordering::Acquire);
            if block.is_null() {
                return None;
            }

            let mut index = self.consumer.index.get();
            if index == BLOCK_SIZE {
                let next_block = (*block).next.load(Ordering::Acquire);
                if next_block.is_null() {
                    return None;
                }

                assert!({
                    let block_and_index = self.producer.block_and_index.load(Ordering::Relaxed);
                    let producer_block = block_and_index.map_addr(|addr| addr & BLOCK_MASK);
                    !ptr::eq(block, producer_block)
                });

                drop(Box::from_raw(block));
                block = next_block;
                self.consumer.block.store(next_block, Ordering::Relaxed);

                index = 0;
                self.consumer.index.set(0);
            }

            assert!(!block.is_null());
            assert!(index < BLOCK_SIZE);

            let stored = (*block).stored.get_unchecked(index);
            if !stored.load(Ordering::Acquire) {
                return None;
            }

            let slot = (*block).values.get_unchecked(index);
            self.consumer.index.set(index + 1);
            Some(slot.0.get().read().assume_init())
        }
    }

    impl<T> Drop for Queue<T> {
        fn drop(&mut self) {
            unsafe {
                while let Some(value) = self.try_recv() {
                    drop(value);
                }

                let block = self.consumer.block.load(Ordering::Relaxed);
                if block.is_null() {
                    return;
                }

                assert!({
                    let block_and_index = self.producer.block_and_index.load(Ordering::Relaxed);
                    let producer_block = block_and_index.map_addr(|addr| addr & BLOCK_MASK);
                    ptr::eq(block, producer_block)
                });

                assert!((*block).next.load(Ordering::Relaxed).is_null());
                drop(Box::from_raw(block));
            }
        }
    }
}
