use core::mem::ManuallyDrop;
use core::ptr;
use core::sync::atomic::Ordering;

use crossbeam_epoch::{Atomic, Owned};

#[derive(Debug)]
pub struct Stack<T> {
    head: Atomic<Node<T>>,
}

#[derive(Debug)]
struct Node<T> {
    data: ManuallyDrop<T>,
    next: Atomic<Node<T>>,
}

impl<T> Default for Stack<T> {
    fn default() -> Self {
        Self {
            head: Atomic::null(),
        }
    }
}

impl<T> Stack<T> {
    pub fn new() -> Stack<T> {
        Self::default()
    }

    pub fn push(&self, t: T) {
        let mut new_node = Owned::new(Node {
            data: ManuallyDrop::new(t),
            next: Atomic::null(),
        });

        let guard = crossbeam_epoch::pin();

        loop {
            let head_snapshot = self.head.load(Ordering::Acquire, &guard);
            new_node.next.store(head_snapshot, Ordering::Relaxed);

            match self
                .head
                .compare_and_set(head_snapshot, new_node, Ordering::Release, &guard)
            {
                Ok(_) => break,
                Err(e) => new_node = e.new,
            }
        }
    }

    pub fn try_pop(&self) -> Option<T> {
        let guard = crossbeam_epoch::pin();

        loop {
            let head_snapshot = self.head.load(Ordering::Acquire, &guard);
            unsafe {
                match head_snapshot.as_ref() {
                    Some(head) => {
                        let next = head.next.load(Ordering::Acquire, &guard);
                        if self
                            .head
                            .compare_and_set(head_snapshot, next, Ordering::Release, &guard)
                            .is_ok()
                        {
                            guard.defer_destroy(head_snapshot);
                            let value = ManuallyDrop::into_inner(ptr::read(&head.data));
                            return Some(value);
                        }
                    }
                    None => return None,
                }
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        let guard = crossbeam_epoch::pin();
        self.head.load(Ordering::Acquire, &guard).is_null()
    }
}

impl<T> Drop for Stack<T> {
    fn drop(&mut self) {
        while self.try_pop().is_some() {}
    }
}

#[cfg(test)]
mod stack_tests {
    use super::*;
    use crossbeam_utils::thread::scope;

    #[test]
    fn push() {
        let stack = Stack::new();

        scope(|scope| {
            for _ in 0..10 {
                scope.spawn(|_| {
                    for i in 0..10_000 {
                        stack.push(i);
                        assert!(stack.try_pop().is_some());
                    }
                });
            }
        })
        .unwrap();

        assert!(stack.try_pop().is_none());
    }
}
