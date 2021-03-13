use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::oneshot;

use crate::error::Result;

pub struct RingTask {}

pub struct RingFuture<T> {
    _t: std::marker::PhantomData<T>,
}

impl<T> Future for RingFuture<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        todo!()
    }
}
