use core::mem::MaybeUninit;
use core::ptr;
use core::sync::atomic::Ordering;

use crossbeam_epoch::{unprotected, Atomic, Guard, Owned, Shared};
use crossbeam_utils::CachePadded;

#[derive(Debug)]
struct Node<T> {
    data: MaybeUninit<T>,
    next: Atomic<Node<T>>,
}

#[derive(Debug)]
pub struct Queue<T> {
    head: CachePadded<Atomic<Node<T>>>,
    tail: CachePadded<Atomic<Node<T>>>,
}

unsafe impl<T: Send> Sync for Queue<T> {}
unsafe impl<T: Send> Send for Queue<T> {}

impl<T> Default for Queue<T> {
    fn default() -> Self {
        let queue = Self {
            head: CachePadded::new(Atomic::null()),
            tail: CachePadded::new(Atomic::null()),
        };

        let sentinel = Owned::new(Node {
            data: MaybeUninit::uninit(),
            next: Atomic::null(),
        });

        unsafe {
            let guard = &unprotected();
            let sentinel = sentinel.into_shared(guard);
            queue.head.store(sentinel, Ordering::Relaxed);
            queue.tail.store(sentinel, Ordering::Relaxed);
            queue
        }
    }
}

impl<T> Queue<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&self, t: T, guard: &Guard) {
        let new = Owned::new(Node {
            data: MaybeUninit::new(t),
            next: Atomic::null(),
        });

        let new = Owned::into_shared(new, guard);

        loop {
            let tail_snapshot = self.tail.load(Ordering::Acquire, guard);

            let tail_ref = unsafe { tail_snapshot.deref() };
            let next = tail_ref.next.load(Ordering::Acquire, guard);

            if !next.is_null() {
                let _ = self
                    .tail
                    .compare_and_set(tail_snapshot, next, Ordering::Release, guard);
                continue;
            }

            if tail_ref
                .next
                .compare_and_set(Shared::null(), new, Ordering::Release, guard)
                .is_ok()
            {
                let _ = self
                    .tail
                    .compare_and_set(tail_snapshot, new, Ordering::Release, guard);
                break;
            }
        }
    }

    pub fn try_pop(&self, guard: &Guard) -> Option<T> {
        loop {
            let head_snapshot = self.head.load(Ordering::Acquire, guard);
            let head = unsafe { head_snapshot.deref() };
            let next = head.next.load(Ordering::Acquire, guard);

            if let Some(next_node) = unsafe { next.as_ref() } {
                let tail_snapshot = self.tail.load(Ordering::Relaxed, guard);
                if tail_snapshot == head_snapshot {
                    let _ =
                        self.tail
                            .compare_and_set(tail_snapshot, next, Ordering::Release, guard);
                }

                if self
                    .head
                    .compare_and_set(head_snapshot, next, Ordering::Release, guard)
                    .is_ok()
                {
                    unsafe {
                        guard.defer_destroy(head_snapshot);
                        return Some(ptr::read(&next_node.data).assume_init());
                    }
                }
            } else {
                return None;
            }
        }
    }
}

impl<T> Drop for Queue<T> {
    fn drop(&mut self) {
        unsafe {
            let guard = unprotected();

            while self.try_pop(guard).is_some() {}

            let sentinel = self.head.load(Ordering::Relaxed, guard);
            drop(sentinel.into_owned());
        }
    }
}
