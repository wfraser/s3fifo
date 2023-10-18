//! Simple implementation of "S3-FIFO" from "FIFO Queues are ALL You Need for Cache Eviction" by
//! Juncheng Yang, et al: https://jasony.me/publication/sosp23-s3fifo.pdf

use std::collections::VecDeque;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering::SeqCst;

// The paper uses two bits to count accesses, for a max of 3. We use 8 bit atomics, but will limit
// the count to the same value, to prevent wrap-arounds causing problems.
const MAX_FREQ: u8 = 3;

struct Entry<K, V> {
    key: K,
    value: V,
    freq: AtomicU8,
}

impl<K, V> Entry<K, V> {
    pub fn new(key: K, value: V) -> Self {
        Self {
            key,
            value,
            freq: AtomicU8::new(0),
        }
    }
}

pub struct S3Fifo<K: PartialEq, V> {
    small: VecDeque<Entry<K, V>>,
    main: VecDeque<Entry<K, V>>,
    ghost: VecDeque<K>,
    small_size: usize,
    main_size: usize,
}

impl<K: PartialEq, V> S3Fifo<K, V> {
    pub fn new(small: usize, main: usize) -> Self {
        Self {
            small: VecDeque::with_capacity(small),
            main: VecDeque::with_capacity(main),
            ghost: VecDeque::with_capacity(main),
            small_size: small,
            main_size: main,
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        // This could be implemented using lock-free queues to not require &mut self, but that is
        // left as an exercise to the reader.
        if self.ghost.contains(&key) {
            if self.main.len() >= self.main_size {
                self.evict_main();
            }
            self.main.push_front(Entry::new(key, value));
        } else {
            if self.small.len() >= self.small_size {
                self.evict_small();
            }
            self.small.push_front(Entry::new(key, value));
        }
    }

    pub fn read(&self, key: &K) -> Option<&V> {
        if let Some(entry) = self.small.iter()
            .chain(self.main.iter())
            .find(|e| &e.key == key)
        {
            if entry.freq.fetch_add(1, SeqCst) + 1 > MAX_FREQ {
                // Clamp it.
                entry.freq.store(MAX_FREQ, SeqCst);
            }
            Some(&entry.value)
        } else {
            None
        }
    }

    fn evict_main(&mut self) {
        while let Some(tail) = self.main.pop_back() {
            let n = tail.freq.load(SeqCst);
            if n > 0 {
                tail.freq.store(n - 1, SeqCst);
                self.main.push_front(tail);
            } else {
                break;
            }
        }
    }

    fn evict_small(&mut self) {
        if let Some(tail) = self.small.pop_back() {
            if tail.freq.load(SeqCst) > 1 {
                if self.main.len() >= self.main_size {
                    self.evict_main();
                }
                self.main.push_front(tail);
            } else {
                if self.ghost.len() >= self.main_size {
                    self.ghost.pop_back();
                }
                self.ghost.push_front(tail.key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, SeedableRng};

    #[test]
    fn it_works() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(0);
        let mut q = S3Fifo::<u32, u32>::new(2, 20);

        let mut hit_rate = (0, 0);
        for i in 0 .. 10_000 {
            eprintln!("#{i}");
            let k = rng.gen_range(0..50);
            if rng.gen_bool(0.5) {
                eprintln!("read {k}");
                match q.read(&k) {
                    Some(v) => {
                        eprintln!("hit");
                        assert_eq!(v, &k);
                        hit_rate.0 += 1;
                        hit_rate.1 += 1;
                    }
                    None => {
                        eprintln!("miss");
                        assert!( q.main.iter().chain(q.small.iter()).find(|e| e.key == k).is_none());
                        hit_rate.1 += 1;
                    }
                }
            } else {
                eprintln!("insert {k}");
                q.insert(k, k);
            }
            assert!(q.main.len() <= q.main_size);
            assert!(q.small.len() <= q.small_size);
            assert!(q.ghost.len() <= q.main_size);
        }
        let (n, d) = hit_rate;
        println!("{n}/{d} = {}", (n as f64) / (d as f64));
    }
}
