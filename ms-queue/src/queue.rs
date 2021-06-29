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

#[cfg(test)]
mod test {
    use super::*;
    use crossbeam_epoch::pin;
    use crossbeam_utils::thread;

    struct Queue<T> {
        queue: super::Queue<T>,
    }

    impl<T> Queue<T> {
        pub fn new() -> Queue<T> {
            Queue {
                queue: super::Queue::new(),
            }
        }

        pub fn push(&self, t: T) {
            let gurad = &pin();
            self.queue.push(t, gurad);
        }

        pub fn is_empty(&self) -> bool {
            let gurad = &pin();
            let head_snapshot = self.queue.head.load(Ordering::Acquire, gurad);
            let head = unsafe { head_snapshot.deref() };
            head.next.load(Ordering::Acquire, gurad).is_null()
        }

        pub fn try_pop(&self) -> Option<T> {
            let gurad = &pin();
            self.queue.try_pop(gurad)
        }

        pub fn pop(&self) -> T {
            loop {
                match self.try_pop() {
                    None => continue,
                    Some(t) => return t,
                }
            }
        }
    }

    const CONC_COUNT: i64 = 1000000;

    #[test]
    fn push_try_pop() {
        let queue: Queue<i64> = Queue::new();
        assert!(queue.is_empty());
        queue.push(39);
        assert!(!queue.is_empty());
        assert_eq!(queue.try_pop(), Some(39));
        assert!(queue.is_empty());
    }

    #[test]
    fn push_try_pop_many_spsc() {
        let queue: Queue<i64> = Queue::new();
        assert!(queue.is_empty());

        thread::scope(|scope| {
            scope.spawn(|_| {
                let mut next = 0;

                while next < CONC_COUNT {
                    if let Some(elem) = queue.try_pop() {
                        assert_eq!(next, elem);
                        next += 1;
                    }
                }
            });

            for i in 0..CONC_COUNT {
                queue.push(i)
            }
        })
        .unwrap();
    }

    #[test]
    fn push_try_pop_spmc() {
        fn recv(_: i32, queue: &Queue<i64>) {
            let mut cur = -1;
            for _ in 0..CONC_COUNT {
                if let Some(elem) = queue.try_pop() {
                    assert!(elem > cur);
                    cur = elem;

                    if cur == CONC_COUNT - 1 {
                        break;
                    }
                }
            }
        }

        let queue: Queue<i64> = Queue::new();
        assert!(queue.is_empty());

        thread::scope(|scope| {
            for i in 0..3 {
                let queue = &queue;
                scope.spawn(move |_| recv(i, queue));
            }
        })
        .unwrap();
    }

    #[test]
    fn push_try_pop_mpmc() {
        enum LR {
            Left(i64),
            Right(i64),
        }

        let queue: Queue<LR> = Queue::new();
        assert!(queue.is_empty());

        thread::scope(|scope| {
            scope.spawn(|_| {
                for i in 0..CONC_COUNT {
                    queue.push(LR::Left(i))
                }
            });
            scope.spawn(|_| {
                for i in 0..CONC_COUNT {
                    queue.push(LR::Right(i))
                }
            });
            scope.spawn(|_| {
                let mut vl = vec![];
                let mut vr = vec![];
                for _ in 0..2 * CONC_COUNT {
                    match queue.try_pop() {
                        Some(LR::Left(left_value)) => vl.push(left_value),
                        Some(LR::Right(right_value)) => vr.push(right_value),
                        _ => {}
                    }
                }

                let mut vl2 = vl.clone();
                let mut vr2 = vr.clone();

                vl2.sort();
                vr2.sort();

                assert_eq!(vl, vl2);
                assert_eq!(vr, vr2);
            });
        })
        .unwrap();
    }
}
