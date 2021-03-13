//! Wrap layer over async io interface.

mod buf;
mod context;
mod ring;
mod task;

// pub struct File;

// impl File {
//     pub async fn read(&self, offset: usize, buf: &[u8]) -> Result<()> {
//         todo!()
//     }

//     pub async fn write(&self, offset: usize, bytes: &[u8]) -> Result<()> {
//         todo!()
//     }
// }
