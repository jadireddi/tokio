//! `TcpStream` owned split support.
//!
//! A `TcpStream` can be split into an `OwnedReadHalf` and a `OwnedWriteHalf`
//! with the `TcpStream::into_split` method.  `OwnedReadHalf` implements
//! `AsyncRead` while `OwnedWriteHalf` implements `AsyncWrite`.
//!
//! Compared to the generic split of `AsyncRead + AsyncWrite`, this specialized
//! split has no associated overhead and enforces all invariants at the type
//! level.

use crate::future::poll_fn;
use crate::io::{AsyncRead, AsyncWrite};
use crate::net::TcpStream;

use bytes::Buf;
use std::error::Error;
use std::mem::MaybeUninit;
use std::net::Shutdown;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{fmt, io};

/// Owned read half of a [`TcpStream`], created by [`into_split`].
///
/// [`TcpStream`]: TcpStream
/// [`into_split`]: TcpStream::into_split()
#[derive(Debug)]
pub struct OwnedReadHalf {
    inner: Arc<TcpStream>,
}

/// Owned write half of a [`TcpStream`], created by [`into_split`].
///
/// Note that in the `AsyncWrite` implemenation of this type, `poll_shutdown` will
/// shut down the TCP stream in the write direction.
///
/// Dropping the write half will close the TCP stream in both directions.
///
/// [`TcpStream`]: TcpStream
/// [`into_split`]: TcpStream::into_split()
#[derive(Debug)]
pub struct OwnedWriteHalf {
    inner: Arc<TcpStream>,
    shutdown_on_drop: bool,
}

pub(crate) fn split_owned(stream: TcpStream) -> (OwnedReadHalf, OwnedWriteHalf) {
    let arc = Arc::new(stream);
    let read = OwnedReadHalf {
        inner: Arc::clone(&arc),
    };
    let write = OwnedWriteHalf {
        inner: arc,
        shutdown_on_drop: true,
    };
    (read, write)
}

pub(crate) fn reunite(
    read: OwnedReadHalf,
    write: OwnedWriteHalf,
) -> Result<TcpStream, ReuniteError> {
    if Arc::ptr_eq(&read.inner, &write.inner) {
        write.forget();
        // This unwrap cannot fail as the api does not allow creating more than two Arcs,
        // and we just dropped the other half.
        Ok(Arc::try_unwrap(read.inner).expect("Too many handles to Arc"))
    } else {
        Err(ReuniteError(read, write))
    }
}

/// Error indicating two halves were not from the same socket, and thus could
/// not be reunited.
#[derive(Debug)]
pub struct ReuniteError(pub OwnedReadHalf, pub OwnedWriteHalf);

impl fmt::Display for ReuniteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "tried to reunite halves that are not from the same socket"
        )
    }
}

impl Error for ReuniteError {}

impl OwnedReadHalf {
    /// Attempts to put the two halves of a `TcpStream` back together and
    /// recover the original socket. Succeeds only if the two halves
    /// originated from the same call to [`into_split`].
    ///
    /// [`into_split`]: TcpStream::into_split()
    pub fn reunite(self, other: OwnedWriteHalf) -> Result<TcpStream, ReuniteError> {
        reunite(self, other)
    }

    /// Attempt to receive data on the socket, without removing that data from
    /// the queue, registering the current task for wakeup if data is not yet
    /// available.
    ///
    /// See the [`TcpStream::poll_peek`] level documenation for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tokio::io;
    /// use tokio::net::TcpStream;
    ///
    /// use futures::future::poll_fn;
    ///
    /// #[tokio::main]
    /// async fn main() -> io::Result<()> {
    ///     let stream = TcpStream::connect("127.0.0.1:8000").await?;
    ///     let (mut read_half, _) = stream.into_split();
    ///     let mut buf = [0; 10];
    ///
    ///     poll_fn(|cx| {
    ///         read_half.poll_peek(cx, &mut buf)
    ///     }).await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// [`TcpStream::poll_peek`]: TcpStream::poll_peek
    pub fn poll_peek(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        self.inner.poll_peek2(cx, buf)
    }

    /// Receives data on the socket from the remote address to which it is
    /// connected, without removing that data from the queue. On success,
    /// returns the number of bytes peeked.
    ///
    /// See the [`TcpStream::peek`] level documenation for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tokio::net::TcpStream;
    /// use tokio::prelude::*;
    /// use std::error::Error;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn Error>> {
    ///     // Connect to a peer
    ///     let stream = TcpStream::connect("127.0.0.1:8080").await?;
    ///     let (mut read_half, _) = stream.into_split();
    ///
    ///     let mut b1 = [0; 10];
    ///     let mut b2 = [0; 10];
    ///
    ///     // Peek at the data
    ///     let n = read_half.peek(&mut b1).await?;
    ///
    ///     // Read the data
    ///     assert_eq!(n, read_half.read(&mut b2[..n]).await?);
    ///     assert_eq!(&b1[..n], &b2[..n]);
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// [`TcpStream::peek`]: TcpStream::peek
    pub async fn peek(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        poll_fn(|cx| self.poll_peek(cx, buf)).await
    }
}

impl AsyncRead for OwnedReadHalf {
    unsafe fn prepare_uninitialized_buffer(&self, _: &mut [MaybeUninit<u8>]) -> bool {
        false
    }

    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        self.inner.poll_read_priv(cx, buf)
    }
}

impl OwnedWriteHalf {
    /// Attempts to put the two halves of a `TcpStream` back together and
    /// recover the original socket. Succeeds only if the two halves
    /// originated from the same call to [`into_split`].
    ///
    /// [`into_split`]: TcpStream::into_split()
    pub fn reunite(self, other: OwnedReadHalf) -> Result<TcpStream, ReuniteError> {
        reunite(other, self)
    }
    /// Destroy the write half, but don't close the stream until the read half
    /// is dropped. If the read half has already been dropped, this closes the
    /// stream.
    pub fn forget(mut self) {
        self.shutdown_on_drop = false;
        drop(self);
    }
}

impl Drop for OwnedWriteHalf {
    fn drop(&mut self) {
        if self.shutdown_on_drop {
            let _ = self.inner.shutdown(Shutdown::Both);
        }
    }
}

impl AsyncWrite for OwnedWriteHalf {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.inner.poll_write_priv(cx, buf)
    }

    fn poll_write_buf<B: Buf>(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut B,
    ) -> Poll<io::Result<usize>> {
        self.inner.poll_write_buf_priv(cx, buf)
    }

    #[inline]
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        // tcp flush is a no-op
        Poll::Ready(Ok(()))
    }

    // `poll_shutdown` on a write half shutdowns the stream in the "write" direction.
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.shutdown(Shutdown::Write).into()
    }
}

impl AsRef<TcpStream> for OwnedReadHalf {
    fn as_ref(&self) -> &TcpStream {
        &*self.inner
    }
}

impl AsRef<TcpStream> for OwnedWriteHalf {
    fn as_ref(&self) -> &TcpStream {
        &*self.inner
    }
}
