use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use http_body::Body;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct ChunkedBody {
    chunks: Vec<Bytes>,
    index: usize,
}

impl ChunkedBody {
    fn new(chunks: Vec<Bytes>) -> Self {
        Self { chunks, index: 0 }
    }
}

impl Body for ChunkedBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        if self.index < self.chunks.len() {
            let chunk = self.chunks[self.index].clone();
            self.index += 1;
            Poll::Ready(Some(Ok(http_body::Frame::data(chunk))))
        } else {
            Poll::Ready(None)
        }
    }

    fn size_hint(&self) -> http_body::SizeHint {
        let total: usize = self.chunks.iter().map(|c| c.len()).sum();
        http_body::SizeHint::with_exact(total as u64)
    }
}

pub fn chunks_to_bytestream(chunks: Vec<Bytes>) -> ByteStream {
    let body = ChunkedBody::new(chunks);
    ByteStream::from_body_1_x(body)
}
