use std::borrow::Borrow;
use std::cmp::Ordering;
use std::convert::TryInto;
use std::marker::PhantomData;
use std::ops::Index;

use crate::types::{Bytes, Entry};

// todo: rename this.
pub(crate) trait KeyExtractor<T: Borrow<Self>>: Eq {
    fn key(data: &T) -> &[u8];
}

impl<T: Index<usize, Output = Entry> + Borrow<Vec<Entry>>> KeyExtractor<T> for Vec<Entry> {
    fn key(data: &T) -> &[u8] {
        &(*data.index(0)).key
    }
}

// todo: remove Eq bound?
pub trait Comparator: Send + Sync + Eq {
    fn cmp(lhs: &[u8], rhs: &[u8]) -> Ordering
    where
        Self: Sized;
}

// todo: simplify this. put KeyExtractor into T.
#[derive(Eq, PartialEq)]
pub(crate) struct OrderingHelper<O: Comparator, E: KeyExtractor<T>, T: Borrow<E>> {
    pub data: T,
    _o: PhantomData<O>,
    _e: PhantomData<E>,
}

impl<O: Comparator, E: KeyExtractor<T>, T: Eq + Borrow<E>> Ord for OrderingHelper<O, E, T> {
    fn cmp(&self, other: &Self) -> Ordering {
        O::cmp(E::key(&self.data), E::key(&other.data))
    }
}

impl<O: Comparator, E: KeyExtractor<T>, T: PartialEq + Borrow<E>> PartialOrd
    for OrderingHelper<O, E, T>
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(O::cmp(E::key(&self.data), E::key(&other.data)))
    }
}

impl<O: Comparator, E: KeyExtractor<T>, T: Borrow<E>> From<T> for OrderingHelper<O, E, T> {
    fn from(data: T) -> Self {
        Self {
            data,
            _o: PhantomData,
            _e: PhantomData,
        }
    }
}

#[derive(Eq, PartialEq)]
/// This comparator returns `Ordering::Equal` for every operands.
/// Which will ignore the provided left and right bound and result a full table scan.
pub struct NoOrderComparator {}

impl Comparator for NoOrderComparator {
    fn cmp(_: &[u8], _: &[u8]) -> Ordering {
        Ordering::Equal
    }
}

pub fn encode_u64(data: u64) -> Bytes {
    data.to_le_bytes().to_vec()
}

pub fn decode_u64(data: &[u8]) -> u64 {
    u64::from_le_bytes(data.try_into().unwrap())
}
