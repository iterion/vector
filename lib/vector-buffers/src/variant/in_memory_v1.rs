use std::error::Error;

use async_trait::async_trait;
use futures::{channel::mpsc::channel, SinkExt};

use crate::{
    buffer_usage_data::BufferUsageHandle,
    topology::{
        builder::IntoBuffer,
        channel::{ReceiverAdapter, SenderAdapter},
    },
    Acker, Bufferable,
};

pub struct MemoryV1Buffer {
    capacity: usize,
}

impl MemoryV1Buffer {
    pub fn new(capacity: usize) -> Self {
        MemoryV1Buffer { capacity }
    }
}

#[async_trait]
impl<T> IntoBuffer<T> for MemoryV1Buffer
where
    T: Bufferable,
{
    async fn into_buffer_parts(
        self: Box<Self>,
        usage_handle: BufferUsageHandle,
    ) -> Result<(SenderAdapter<T>, ReceiverAdapter<T>, Option<Acker>), Box<dyn Error + Send + Sync>>
    {
        usage_handle.set_buffer_limits(None, Some(self.capacity));

        let (tx, rx) = channel(self.capacity);
        Ok((
            SenderAdapter::opaque(tx.sink_map_err(|_| ())),
            ReceiverAdapter::opaque(rx),
            None,
        ))
    }
}
