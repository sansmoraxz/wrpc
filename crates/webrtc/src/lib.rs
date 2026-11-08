//! wRPC WebRTC transport
//!
//! A WebRTC data channel carries messages, not a byte stream, and browsers cap a message's
//! size. So this transport multiplexes invocations over one ordered, reliable channel, as
//! QUIC multiplexes streams over a connection: every message is one frame of one stream,
//!
//! ```text
//! [stream id: u32, little endian][kind: u8][payload]
//! ```
//!
//! A stream is what wRPC needs per invocation: two directions, each ended on its own. Its
//! id's lowest bit is the [`Side`] that opened it, so both ends open streams without
//! agreeing first, and a stream first seen with a peer's id is one the peer opened. A frame
//! is at most [`MAX_MESSAGE`] bytes, which every browser takes, and the frames of streams
//! written at once interleave, so a large value does not hold up a small one behind it for
//! longer than a frame.
//!
//! One channel rather than one per invocation: a browser can hand a data channel to a worker
//! only in the task that made it, so a worker could not open one per call itself, and
//! opening a channel costs a round of the data channel establishment protocol.
//!
//! The channel is anything implementing [`Channel`]; with the `webrtc` feature, a
//! [`webrtc-rs`](https://docs.rs/webrtc) data channel is one ([`DataChannel`]).

use core::future::Future;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use core::task::{Context, Poll, ready};

use std::collections::{BTreeSet, HashMap};
use std::io;
use std::sync::{Arc, Mutex, MutexGuard};

use bytes::{Buf as _, BufMut as _, Bytes, BytesMut};
use tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tracing::{debug, trace};
use wrpc_transport::Invoke;
use wrpc_transport::frame::{Incoming, Outgoing, invoke};

#[cfg(feature = "webrtc")]
mod rtc;
#[cfg(feature = "webrtc")]
pub use rtc::DataChannel;

/// The largest message a frame takes, its header included.
pub const MAX_MESSAGE: usize = 64 * 1024;

const HEADER: usize = 5;

/// The most a stream's frames may be waiting to be sent, summed over a link's streams.
const WINDOW: usize = 1 << 20;

/// Bytes of a stream.
const DATA: u8 = 0;
/// The sender writes no more on this stream.
const END: u8 = 1;
/// The sender gave up on this stream before ending it.
const RESET: u8 = 2;
/// The receiver reads no more of this stream: the sender should stop.
const STOP: u8 = 3;

/// An ordered, reliable carrier of binary messages: a WebRTC data channel, say.
pub trait Channel: Send + Sync + 'static {
    /// Sends one message, of at most [`MAX_MESSAGE`] bytes. May wait for the channel's
    /// send buffer to drain.
    fn send(&self, message: Bytes) -> impl Future<Output = io::Result<()>> + Send;

    /// The next message, or `None` once the channel is closed.
    fn recv(&self) -> impl Future<Output = Option<Bytes>> + Send;

    /// Closes the channel: its peer's [`recv`](Self::recv) ends. Called once the [`Link`]
    /// over it and all its streams are gone.
    fn close(&self) -> impl Future<Output = ()> + Send;
}

/// Which end of a channel this is. The two ends of one channel must differ: for a WebRTC
/// peer connection, the end that offered and the end that answered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    /// Opens even streams.
    Offer,
    /// Opens odd streams.
    Answer,
}

impl Side {
    fn bit(self) -> u32 {
        match self {
            Self::Offer => 0,
            Self::Answer => 1,
        }
    }
}

/// A frame to send, with the share of the window it holds until it is.
type Frame = (Bytes, Option<OwnedSemaphorePermit>);

/// The peer's streams this end has done receiving: every id below `floor`, and those in
/// `above`. What tells a frame of a new stream from a late one of an old: a peer opens
/// streams in order of id, but may write to them in any.
struct Done {
    floor: u32,
    above: BTreeSet<u32>,
}

impl Done {
    fn contains(&self, id: u32) -> bool {
        id < self.floor || self.above.contains(&id)
    }

    fn insert(&mut self, id: u32) {
        if id >= self.floor {
            self.above.insert(id);
        }
        while self.above.remove(&self.floor) {
            self.floor += 2;
        }
    }
}

struct Shared {
    /// Per stream still receiving, where its bytes go.
    readers: Mutex<HashMap<u32, mpsc::UnboundedSender<io::Result<Bytes>>>>,
    /// The peer's streams done receiving.
    done: Mutex<Done>,
    /// Per stream still sending, whether its receiver stopped reading.
    writers: Mutex<HashMap<u32, Arc<AtomicBool>>>,
    frames: mpsc::UnboundedSender<Frame>,
    window: Arc<Semaphore>,
    closed: AtomicBool,
    side: Side,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn frame(id: u32, kind: u8, payload: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(HEADER + payload.len());
    buf.put_u32_le(id);
    buf.put_u8(kind);
    buf.put_slice(payload);
    buf.freeze()
}

impl Shared {
    /// This end is done receiving stream `id`.
    fn finished(&self, id: u32, side: Side) {
        if id & 1 != side.bit() {
            lock(&self.done).insert(id);
        }
    }

    fn send_control(&self, id: u32, kind: u8) {
        // The writer is gone only once the link is.
        let _ = self.frames.send((frame(id, kind, &[]), None));
    }

    /// A stream's two ends, registered.
    fn stream(self: &Arc<Self>, id: u32) -> (SendStream, RecvStream) {
        let (tx, rx) = mpsc::unbounded_channel();
        let stopped = Arc::new(AtomicBool::new(false));
        lock(&self.readers).insert(id, tx);
        lock(&self.writers).insert(id, stopped.clone());
        (
            SendStream {
                id,
                shared: self.clone(),
                stopped,
                acquiring: None,
                shutdown: false,
            },
            RecvStream {
                id,
                shared: self.clone(),
                rx,
                buf: Bytes::new(),
                done: false,
            },
        )
    }
}

/// wRPC server for a [`Link`]
pub type Server = wrpc_transport::Server<(), RecvStream, SendStream>;

/// One channel's invocations, both ways: a wRPC client ([`Invoke`]), and what the peer
/// invokes, to [`accept`](Self::accept) for a [`Server`].
pub struct Link {
    shared: Arc<Shared>,
    side: Side,
    next: AtomicU32,
    accepted: tokio::sync::Mutex<mpsc::UnboundedReceiver<(SendStream, RecvStream)>>,
}

impl Link {
    /// Multiplexes over `channel`, which this end holds as `side`. Spawns its reader and
    /// writer on the current Tokio runtime; they end when the channel closes.
    pub fn new<C: Channel>(channel: C, side: Side) -> Self {
        let channel = Arc::new(channel);
        let (frames, mut queued) = mpsc::unbounded_channel::<Frame>();
        let shared = Arc::new(Shared {
            readers: Mutex::default(),
            done: Mutex::new(Done {
                floor: 1 - side.bit(),
                above: BTreeSet::new(),
            }),
            writers: Mutex::default(),
            frames,
            window: Arc::new(Semaphore::new(WINDOW)),
            closed: AtomicBool::new(false),
            side,
        });
        let (accept_tx, accepted) = mpsc::unbounded_channel();
        let writer = channel.clone();
        let writer_shared = Arc::downgrade(&shared);
        tokio::spawn(async move {
            while let Some((message, permit)) = queued.recv().await {
                if let Err(err) = writer.send(message).await {
                    debug!(?err, "failed to send a frame: the link is gone");
                    if let Some(shared) = writer_shared.upgrade() {
                        shared.closed.store(true, Ordering::SeqCst);
                    }
                    break;
                }
                drop(permit);
            }
            // Every sender is gone: the link and its streams.
            writer.close().await;
        });
        tokio::spawn(read(channel, Arc::downgrade(&shared), side, accept_tx));
        Self {
            shared,
            side,
            next: AtomicU32::new(0),
            accepted: tokio::sync::Mutex::new(accepted),
        }
    }

    /// Opens a stream to the peer.
    pub fn open(&self) -> io::Result<(SendStream, RecvStream)> {
        if self.shared.closed.load(Ordering::SeqCst) {
            return Err(io::ErrorKind::NotConnected.into());
        }
        let n = self.next.fetch_add(1, Ordering::Relaxed);
        let id = n
            .checked_mul(2)
            .ok_or_else(|| io::Error::other("out of stream ids"))?
            | self.side.bit();
        Ok(self.shared.stream(id))
    }

    /// The next stream the peer opened, or `None` once the channel is closed.
    pub async fn accept(&self) -> Option<(SendStream, RecvStream)> {
        self.accepted.lock().await.recv().await
    }

    /// Whether the channel has closed.
    pub fn is_closed(&self) -> bool {
        self.shared.closed.load(Ordering::SeqCst)
    }
}

async fn read<C: Channel>(
    channel: Arc<C>,
    shared: std::sync::Weak<Shared>,
    side: Side,
    accepted: mpsc::UnboundedSender<(SendStream, RecvStream)>,
) {
    while let Some(mut message) = channel.recv().await {
        let Some(shared) = shared.upgrade() else {
            return;
        };
        if message.len() < HEADER {
            debug!(
                len = message.len(),
                "dropping a message too short to be a frame"
            );
            continue;
        }
        let id = message.get_u32_le();
        let kind = message.get_u8();
        trace!(id, kind, len = message.len(), "frame");
        match kind {
            DATA | END => {
                let known = lock(&shared.readers).contains_key(&id);
                if !known && id & 1 != side.bit() && !lock(&shared.done).contains(id) {
                    let ends = shared.stream(id);
                    if accepted.send(ends).is_err() {
                        // No one accepts: the peer is told by the ends' drop.
                    }
                }
                let mut readers = lock(&shared.readers);
                if kind == DATA {
                    if let Some(tx) = readers.get(&id)
                        && !message.is_empty()
                    {
                        let _ = tx.send(Ok(message));
                    }
                } else {
                    // Dropping its sender ends what it reads.
                    readers.remove(&id);
                    shared.finished(id, side);
                }
            }
            RESET => {
                if let Some(tx) = lock(&shared.readers).remove(&id) {
                    let _ = tx.send(Err(io::ErrorKind::ConnectionReset.into()));
                }
                shared.finished(id, side);
            }
            STOP => {
                if let Some(stopped) = lock(&shared.writers).get(&id) {
                    stopped.store(true, Ordering::SeqCst);
                }
            }
            kind => debug!(kind, "dropping a frame of an unknown kind"),
        }
    }
    if let Some(shared) = shared.upgrade() {
        shared.closed.store(true, Ordering::SeqCst);
        for (_, tx) in lock(&shared.readers).drain() {
            let _ = tx.send(Err(io::ErrorKind::ConnectionAborted.into()));
        }
        // Nothing waits on a stopped peer's window again.
        shared.window.close();
    }
}

type Acquiring = Pin<
    Box<dyn Future<Output = Result<OwnedSemaphorePermit, tokio::sync::AcquireError>> + Send + Sync>,
>;

/// Send side of a stream, which resets the stream once dropped before being shut down
pub struct SendStream {
    id: u32,
    shared: Arc<Shared>,
    stopped: Arc<AtomicBool>,
    acquiring: Option<Acquiring>,
    shutdown: bool,
}

impl Drop for SendStream {
    fn drop(&mut self) {
        lock(&self.shared.writers).remove(&self.id);
        if !self.shutdown {
            self.shared.send_control(self.id, RESET);
        }
    }
}

impl AsyncWrite for SendStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.shutdown {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        if self.stopped.load(Ordering::SeqCst) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "the receiver stopped reading",
            )));
        }
        if self.shared.closed.load(Ordering::SeqCst) {
            return Poll::Ready(Err(io::ErrorKind::ConnectionAborted.into()));
        }
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let want = buf.len().min(MAX_MESSAGE - HEADER);
        let window = self.shared.window.clone();
        // At most WINDOW, so it can always be had.
        let acquiring = self
            .acquiring
            .get_or_insert_with(|| Box::pin(window.acquire_many_owned(want as u32)));
        let permit = ready!(acquiring.as_mut().poll(cx));
        self.acquiring = None;
        let Ok(permit) = permit else {
            return Poll::Ready(Err(io::ErrorKind::ConnectionAborted.into()));
        };
        // `buf` may be shorter than when the window was asked for.
        let n = (permit.num_permits()).min(buf.len());
        let message = frame(self.id, DATA, &buf[..n]);
        if self.shared.frames.send((message, Some(permit))).is_err() {
            return Poll::Ready(Err(io::ErrorKind::ConnectionAborted.into()));
        }
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.shutdown {
            self.shutdown = true;
            self.shared.send_control(self.id, END);
        }
        Poll::Ready(Ok(()))
    }
}

/// Receive side of a stream, which tells the peer to stop sending once dropped before the
/// stream ended
pub struct RecvStream {
    id: u32,
    shared: Arc<Shared>,
    rx: mpsc::UnboundedReceiver<io::Result<Bytes>>,
    buf: Bytes,
    done: bool,
}

impl Drop for RecvStream {
    fn drop(&mut self) {
        if !self.done && lock(&self.shared.readers).remove(&self.id).is_some() {
            self.shared.send_control(self.id, STOP);
        }
        self.shared.finished(self.id, self.shared.side);
    }
}

impl AsyncBufRead for RecvStream {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        let this = self.get_mut();
        if this.buf.is_empty() && !this.done {
            match ready!(this.rx.poll_recv(cx)) {
                Some(Ok(bytes)) => this.buf = bytes,
                Some(Err(err)) => {
                    this.done = true;
                    return Poll::Ready(Err(err));
                }
                None => this.done = true,
            }
        }
        Poll::Ready(Ok(&this.buf))
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        self.get_mut().buf.advance(amt);
    }
}

impl AsyncRead for RecvStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let filled = ready!(self.as_mut().poll_fill_buf(cx))?;
        let n = filled.len().min(buf.remaining());
        buf.put_slice(&filled[..n]);
        self.consume(n);
        Poll::Ready(Ok(()))
    }
}

impl Invoke for Link {
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
        let (tx, rx) = self.open()?;
        invoke(tx, rx, instance, func, params, paths).await
    }
}

impl Invoke for &Link {
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
        (*self).invoke((), instance, func, params, paths).await
    }
}
