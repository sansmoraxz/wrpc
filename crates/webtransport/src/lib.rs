//! wRPC WebTransport transport

use core::ops::{Deref, DerefMut};
use core::pin::Pin;
use core::task::{Context, Poll, ready};

use anyhow::Context as _;
use bytes::Bytes;
use tokio::io::{AsyncBufRead, AsyncBufReadExt as _, AsyncRead, AsyncWrite, BufReader, ReadBuf};
use web_transport_quinn::Session;
use wrpc_transport::Invoke;
use wrpc_transport::frame::{Incoming, Outgoing, invoke};

const DONE: u32 = 1;

const ABORTED: u32 = 0;

/// WebTransport server
pub type Server = wrpc_transport::Server<(), RecvStream, SendStream>;

/// Buffered receive side of a WebTransport stream which, once dropped, stops the stream with
/// WebTransport application error code `1` as soon as the peer sends more, signaling the peer
/// that it is done receiving data. A peer that only finishes its side is left to finish:
/// Firefox fails reads of a stream whose other direction was stopped, losing the results it
/// was reading.
pub struct RecvStream(Option<BufReader<web_transport_quinn::RecvStream>>);

impl From<web_transport_quinn::RecvStream> for RecvStream {
    fn from(rx: web_transport_quinn::RecvStream) -> Self {
        Self(Some(BufReader::new(rx)))
    }
}

impl Drop for RecvStream {
    fn drop(&mut self) {
        let Some(mut rx) = self.0.take() else {
            return;
        };
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            _ = rx.get_mut().stop(DONE);
            return;
        };
        runtime.spawn(async move {
            // Ends with the stream finishing, the peer sending more, or the stream failing
            match rx.fill_buf().await {
                Ok([]) | Err(..) => {}
                Ok(..) => {
                    _ = rx.get_mut().stop(DONE);
                }
            }
        });
    }
}

impl AsyncRead for RecvStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match &mut self.get_mut().0 {
            Some(rx) => Pin::new(rx).poll_read(cx, buf),
            None => Poll::Ready(Ok(())),
        }
    }
}

impl AsyncBufRead for RecvStream {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<&[u8]>> {
        match &mut self.get_mut().0 {
            Some(rx) => Pin::new(rx).poll_fill_buf(cx),
            None => Poll::Ready(Ok(&[])),
        }
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        if let Some(rx) = &mut self.get_mut().0 {
            Pin::new(rx).consume(amt);
        }
    }
}

/// Send side of a WebTransport stream, which is reset with WebTransport application error code `0` once
/// dropped before being shut down
#[derive(Debug)]
pub struct SendStream {
    inner: web_transport_quinn::SendStream,
    shutdown: bool,
}

impl From<web_transport_quinn::SendStream> for SendStream {
    fn from(inner: web_transport_quinn::SendStream) -> Self {
        Self {
            inner,
            shutdown: false,
        }
    }
}

impl Drop for SendStream {
    fn drop(&mut self) {
        if !self.shutdown {
            _ = self.inner.reset(ABORTED);
        }
    }
}

impl AsyncWrite for SendStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        AsyncWrite::poll_write(Pin::new(&mut self.get_mut().inner), cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        AsyncWrite::poll_flush(Pin::new(&mut self.get_mut().inner), cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        ready!(AsyncWrite::poll_shutdown(Pin::new(&mut this.inner), cx))?;
        this.shutdown = true;
        Poll::Ready(Ok(()))
    }
}

/// WebTransport wRPC client
#[derive(Clone, Debug)]
pub struct Client(Session);

impl From<Session> for Client {
    fn from(session: Session) -> Self {
        Self(session)
    }
}

impl Deref for Client {
    type Target = Session;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Client {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Invoke for &Client {
    type Context = ();

    async fn invoke<P>(
        &self,
        (): Self::Context,
        instance: &str,
        func: &str,
        params: Bytes,
        paths: impl AsRef<[P]> + Send,
    ) -> anyhow::Result<(Outgoing, Incoming)>
    where
        P: AsRef<[Option<usize>]> + Send + Sync,
    {
        let (tx, rx) = self
            .0
            .open_bi()
            .await
            .context("failed to open parameter stream")?;
        invoke(
            SendStream::from(tx),
            RecvStream::from(rx),
            instance,
            func,
            params,
            paths,
        )
        .await
    }
}

impl Invoke for Client {
    type Context = ();

    async fn invoke<P>(
        &self,
        (): Self::Context,
        instance: &str,
        func: &str,
        params: Bytes,
        paths: impl AsRef<[P]> + Send,
    ) -> anyhow::Result<(Outgoing, Incoming)>
    where
        P: AsRef<[Option<usize>]> + Send + Sync,
    {
        (&self).invoke((), instance, func, params, paths).await
    }
}
