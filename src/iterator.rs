use std::collections::BinaryHeap;

use async_trait::async_trait;
use tokio::sync::mpsc::Receiver;

use crate::error::Result;
use crate::types::Entry;
use crate::util::{Comparator, KeyExtractor, OrderingHelper};

// todo: add type param
#[async_trait]
pub trait Iterator {
    async fn next(&mut self) -> Result<Option<Entry>>;

    fn is_valid(&self) -> bool;
}

/// Iterate over timestamp. i.e, (ts 0, key 1) -> (ts 1, key 1) -> (ts 2, key 1)...
///
/// "Scan" is achieved via (lots of) `get()`
pub struct TimeIterator<C: Comparator> {
    inner: ShardMuxTimeIterator<C>,
    buf: Vec<Entry>,
}

impl<C: Comparator> TimeIterator<C> {
    pub(crate) fn new(mux_iter: ShardMuxTimeIterator<C>) -> Self {
        Self {
            inner: mux_iter,
            buf: vec![],
        }
    }

    pub(crate) async fn next(&mut self) -> Result<Option<Entry>> {
        if self.buf.is_empty() {
            self.buf = ok_unwrap!(self.inner.next().await);
        }

        Ok(self.buf.pop())
    }

    /// Valid when inner iterator is valid or its own buffer still contains things.
    pub(crate) fn is_valid(&self) -> bool {
        self.inner.is_valid() || !self.buf.is_empty()
    }
}

#[async_trait]
impl<C: Comparator + Eq> Iterator for TimeIterator<C> {
    async fn next(&mut self) -> Result<Option<Entry>> {
        self.next().await
    }

    fn is_valid(&self) -> bool {
        self.is_valid()
    }
}

pub(crate) struct ShardTimeIterator {
    ready: Option<Vec<Entry>>,
    source: Receiver<Vec<Entry>>,
    is_finished: bool,
}

impl ShardTimeIterator {
    // will wait source to yield the first element.
    pub(crate) async fn new(mut source: Receiver<Vec<Entry>>) -> Self {
        let ready = source.recv().await;
        let is_finished = ready.is_none();

        Self {
            ready,
            source,
            is_finished,
        }
    }

    // todo: maybe add a trait `PeekableIterator` : `Iterator`
    pub fn peek(&self) -> Option<&Vec<Entry>> {
        self.ready.as_ref()
    }

    /// Take current value but not step iterator after that.
    pub async fn take(&mut self) -> Result<Option<Vec<Entry>>> {
        // println!("going to take entry from shard iter {:?}", self.ready);
        let ready = self.ready.take();
        self.step().await?;

        Ok(ready)
    }

    async fn step(&mut self) -> Result<()> {
        if self.is_finished {
            return Ok(());
        }

        match self.source.recv().await {
            Some(item) => self.ready = Some(item),
            None => self.is_finished = true,
        }

        Ok(())
    }

    pub fn is_valid(&self) -> bool {
        !self.is_finished
    }
}

pub struct ShardMuxTimeIterator<C: Comparator> {
    iters: Vec<ShardTimeIterator>,
    entry_buf: BinaryHeap<OrderingHelper<C, Vec<Entry>>>,
}

impl<C: Comparator> ShardMuxTimeIterator<C> {
    pub(crate) async fn new(iters: Vec<ShardTimeIterator>, buf_size: usize) -> Self {
        let mut s = Self {
            iters,
            entry_buf: BinaryHeap::default(),
        };
        s.init(buf_size).await;

        s
    }

    async fn next(&mut self) -> Option<Vec<Entry>> {
        if self.entry_buf.is_empty() {
            return None;
        }

        let next = self.entry_buf.pop().unwrap().data;
        // todo: check this Result
        let _ = self.consume_one().await;

        Some(next)
    }

    /// Valid when underlying iters aren't all consumed or `entry_buf` still
    /// buffers some entries.
    fn is_valid(&self) -> bool {
        !self.iters.is_empty() || !self.entry_buf.is_empty()
    }

    async fn init(&mut self, buf_size: usize) {
        // sort underlying iterators
        self.purge_finished();
        self.iters.sort_by(|lhs, rhs| {
            C::cmp(
                Vec::<Entry>::key(lhs.peek().unwrap()),
                Vec::<Entry>::key(rhs.peek().unwrap()),
            )
        });

        // fill `entry_buf`
        while !self.iters.is_empty() && self.entry_buf.len() < buf_size {
            // todo: check this Result
            let _ = self.consume_one().await;
        }
    }

    /// Remove and deconstruct finished iterator to release source.
    fn purge_finished(&mut self) {
        self.iters.retain(|iter| iter.is_valid())
    }

    /// Get one element from underlying iterators and put it into `entry_buf`.
    /// Then step the iterator which supplies that element and reordering
    /// the iterator list to keep them ordered.
    async fn consume_one(&mut self) -> Result<()> {
        if self.iters.is_empty() {
            return Ok(());
        }

        // consume
        let mut first_iter = self.iters.pop().unwrap();
        let item = first_iter.take().await?.unwrap();
        self.entry_buf.push(item.into());
        // this iterator is finished
        if !first_iter.is_valid() {
            return Ok(());
        }

        // insert popped iterator to ordered position
        let new_entry = first_iter.peek().unwrap();
        let lhs = Vec::<Entry>::key(new_entry);
        let index = self
            .iters
            .binary_search_by(|iter| C::cmp(lhs, Vec::<Entry>::key(iter.peek().unwrap())))
            .unwrap_or_else(|x| x);
        self.iters.insert(index, first_iter);

        Ok(())
    }
}
