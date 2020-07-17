//! IO over in-process memory

use crate::io::{AsyncRead, AsyncWrite};

use std::{task::{self, Poll, Waker}, pin::Pin, sync::{Arc, Mutex}};
use bytes::{Buf, BytesMut};

/// A bidirectional pipe to read and write bytes in memory.
///
/// A pair of `DuplexStream`s are created together, and they act as a "channel"
/// that can be used as in-memory IO types. Writing to one of the pairs will
/// allow that data to be read from the other, and vice versa.
///
/// # Example
///
/// ```
/// # async fn ex() -> std::io::Result<()> {
/// # use tokio::io::{AsyncReadExt, AsyncWriteExt};
/// let (mut client, mut server) = tokio::io::duplex();
///
/// client.write_all(b"ping").await?;
///
/// let mut buf = [0u8; 4];
/// server.read_exact(&mut buf).await?;
/// assert_eq!(&buf, b"ping");
///
/// server.write_all(b"pong").await?;
///
/// client.read_exact(&mut buf).await?;
/// assert_eq!(&buf, b"pong");
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct DuplexStream {
    read: Arc<Mutex<Pipe>>,
    write: Arc<Mutex<Pipe>>,
}

/// A unidirectional IO over a piece of memory.
///
/// Data can be written to the pipe, and reading will return that data.
#[derive(Debug)]
struct Pipe {
    /// The buffer storing the bytes written, also read from.
    ///
    /// Using a `BytesMut` because it has efficient `Buf` and `BufMut`
    /// functionality already. Additionally, it can try to copy data in the
    /// same buffer if there read index has advanced far enough.
    buffer: BytesMut,
    /// Determines if the write side has been closed.
    is_closed: bool,
    /// If the `read` side has been polled and is pending, this is the waker
    /// for that parked task.
    read_waker: Option<Waker>,
}

// ===== impl DuplexStream =====

/// Create a new pair of `DuplexStream`s that act like a pair of connected sockets.
pub fn duplex() -> (DuplexStream, DuplexStream) {
    let one = Arc::new(Mutex::new(Pipe::new()));
    let two = Arc::new(Mutex::new(Pipe::new()));

    (
        DuplexStream {
            read: one.clone(),
            write: two.clone(),
        },
        DuplexStream {
            read: two,
            write: one,
        },
    )
}

impl AsyncRead for DuplexStream {
    fn poll_read(self: Pin<&mut Self>, cx: &mut task::Context<'_>, buf: &mut [u8])
        -> Poll<std::io::Result<usize>>
    {
        Pin::new(&mut *self.read.lock().unwrap()).poll_read(cx, buf)
    }
}

impl AsyncWrite for DuplexStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut task::Context<'_>, buf: &[u8])
        -> Poll<std::io::Result<usize>>
    {
        Pin::new(&mut *self.write.lock().unwrap()).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut task::Context<'_>)
        -> Poll<std::io::Result<()>>
    {
        Pin::new(&mut *self.write.lock().unwrap()).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut task::Context<'_>)
        -> Poll<std::io::Result<()>>
    {
        Pin::new(&mut *self.write.lock().unwrap()).poll_shutdown(cx)
    }
}

impl Drop for DuplexStream {
    fn drop(&mut self) {
        // notify the other side of the closure
        self.write.lock().unwrap().close();
    }
}

// ===== impl Pipe =====

impl Pipe {
    fn new() -> Self {
        Pipe {
            buffer: BytesMut::new(),
            is_closed: false,
            read_waker: None,
        }
    }

    fn close(&mut self) {
        self.is_closed = true;
        if let Some(waker) = self.read_waker.take() {
            waker.wake();
        }
    }
}

impl AsyncRead for Pipe {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>, buf: &mut [u8])
        -> Poll<std::io::Result<usize>>
    {
        if self.buffer.has_remaining() {
            let max = self.buffer.remaining().min(buf.len());
            self.buffer.copy_to_slice(&mut buf[..max]);
            Poll::Ready(Ok(max))
        } else if self.is_closed {
            Poll::Ready(Ok(0))
        } else {
            self.read_waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl AsyncWrite for Pipe {
    fn poll_write(mut self: Pin<&mut Self>, _: &mut task::Context<'_>, buf: &[u8])
        -> Poll<std::io::Result<usize>>
    {
        if self.is_closed {
            return Poll::Ready(Err(std::io::ErrorKind::BrokenPipe.into()));
        }
        self.buffer.extend_from_slice(buf);
        if let Some(waker) = self.read_waker.take() {
            waker.wake();
        }
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut task::Context<'_>)
        -> Poll<std::io::Result<()>>
    {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _: &mut task::Context<'_>)
        -> Poll<std::io::Result<()>>
    {
        self.close();
        Poll::Ready(Ok(()))
    }
}
