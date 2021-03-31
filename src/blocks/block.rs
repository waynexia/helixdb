pub struct Block {
    data: Vec<u8>,
}

impl Block {
    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

pub struct BlockBuilder {
    buf: Vec<u8>,
}

impl BlockBuilder {
    pub fn new(block_size: usize) -> Self {
        todo!();
    }

    /// Return whether block_size exceeded.
    /// This is a no-op when returning true.
    pub fn add(&mut self, data: &[u8]) -> bool {
        todo!()
    }

    pub fn curr_size(&self) -> usize {
        todo!()
    }

    pub fn finish(&mut self) -> Block {
        todo!()
    }

    pub fn reset(&mut self) {}
}

pub trait BlockIter {
    type Key;
    type Value;

    fn new(block: Block) -> Self;

    fn seek(&mut self, key: &Self::Key) -> Option<()>;

    fn next(&mut self);

    fn value(&self) -> &Self::Value;
}
