use std::io;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use bytes::Bytes;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::{Mutex, mpsc};
use wrpc_webrtc::{Channel, Link, MAX_MESSAGE, Server, Side};

/// One end of an in-memory channel, checking every message fits.
struct Mem {
    tx: std::sync::Mutex<Option<mpsc::UnboundedSender<Bytes>>>,
    rx: Mutex<mpsc::UnboundedReceiver<Bytes>>,
}

impl Channel for Mem {
    async fn send(&self, message: Bytes) -> io::Result<()> {
        assert!(
            message.len() <= MAX_MESSAGE,
            "a message of {} B",
            message.len()
        );
        let tx = self.tx.lock().unwrap().clone();
        tx.ok_or(io::ErrorKind::BrokenPipe)?
            .send(message)
            .map_err(|_| io::ErrorKind::BrokenPipe.into())
    }

    async fn close(&self) {
        self.tx.lock().unwrap().take();
    }

    async fn recv(&self) -> Option<Bytes> {
        self.rx.lock().await.recv().await
    }
}

fn mem() -> (Mem, Mem) {
    let (a_tx, a_rx) = mpsc::unbounded_channel();
    let (b_tx, b_rx) = mpsc::unbounded_channel();
    (
        Mem {
            tx: std::sync::Mutex::new(Some(a_tx)),
            rx: Mutex::new(b_rx),
        },
        Mem {
            tx: std::sync::Mutex::new(Some(b_tx)),
            rx: Mutex::new(a_rx),
        },
    )
}

/// Serves what `link`'s peer invokes, on `srv`.
fn serve(link: Arc<Link>, srv: Arc<Server>) {
    tokio::spawn(async move {
        while let Some((tx, rx)) = link.accept().await {
            let srv = srv.clone();
            tokio::spawn(async move { srv.accept((), tx, rx).await });
        }
    });
}

#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn loopback() -> anyhow::Result<()> {
    let (a, b) = mem();
    let client = Link::new(a, Side::Offer);
    let server = Arc::new(Link::new(b, Side::Answer));
    let srv = Arc::new(Server::new());
    serve(server, srv.clone());
    wrpc_test::assert_single_invocation((), &client, srv.as_ref()).await?;
    Ok(())
}

#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn both_ends_invoke() -> anyhow::Result<()> {
    let (a, b) = mem();
    let a = Arc::new(Link::new(a, Side::Offer));
    let b = Arc::new(Link::new(b, Side::Answer));
    let (srv_a, srv_b) = (Arc::new(Server::new()), Arc::new(Server::new()));
    serve(a.clone(), srv_a.clone());
    serve(b.clone(), srv_b.clone());
    wrpc_test::assert_single_invocation((), a.as_ref(), srv_b.as_ref()).await?;
    wrpc_test::assert_single_invocation((), b.as_ref(), srv_a.as_ref()).await?;
    Ok(())
}

/// Streams written at once interleave: a small one ends while large ones are in flight,
/// and every byte of each arrives, in order.
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn streams_interleave() -> anyhow::Result<()> {
    let (a, b) = mem();
    let a = Link::new(a, Side::Offer);
    let b = Arc::new(Link::new(b, Side::Answer));
    let echo = b.clone();
    tokio::spawn(async move {
        while let Some((mut tx, mut rx)) = echo.accept().await {
            tokio::spawn(async move {
                let mut body = Vec::new();
                rx.read_to_end(&mut body).await?;
                tx.write_all(&body).await?;
                tx.shutdown().await?;
                io::Result::Ok(())
            });
        }
    });
    let big: Vec<u8> = (0..3 << 20).map(|i: u32| (i % 251) as u8).collect();
    let mut large = Vec::new();
    for _ in 0..4 {
        let (mut tx, mut rx) = a.open()?;
        let big = big.clone();
        large.push(tokio::spawn(async move {
            tx.write_all(&big).await?;
            tx.shutdown().await?;
            let mut back = Vec::new();
            rx.read_to_end(&mut back).await?;
            anyhow::ensure!(back == big, "a large stream came back changed");
            anyhow::Ok(())
        }));
    }
    let (mut tx, mut rx) = a.open()?;
    tx.write_all(b"small").await?;
    tx.shutdown().await?;
    let mut back = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), rx.read_to_end(&mut back))
        .await
        .context("the small stream waited")??;
    assert_eq!(back, b"small");
    for task in large {
        task.await??;
    }
    Ok(())
}

#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn drops_tell_the_peer() -> anyhow::Result<()> {
    let (a, b) = mem();
    let a = Link::new(a, Side::Offer);
    let b = Link::new(b, Side::Answer);

    // A send side dropped before it ends resets what the peer reads.
    let (mut tx, _rx) = a.open()?;
    tx.write_all(b"foo").await?;
    drop(tx);
    let (btx, mut rx) = b.accept().await.context("no stream")?;
    drop(btx);
    let mut body = Vec::new();
    let err = rx.read_to_end(&mut body).await.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::ConnectionReset);
    assert_eq!(body, b"foo");

    // One shut down ends it.
    let (mut tx, _rx) = a.open()?;
    tx.write_all(b"bar").await?;
    tx.shutdown().await?;
    let (btx, mut rx) = b.accept().await.context("no stream")?;
    drop(btx);
    let mut body = Vec::new();
    rx.read_to_end(&mut body).await?;
    assert_eq!(body, b"bar");

    // A receive side dropped stops the peer's writes.
    let (mut tx, _rx) = a.open()?;
    tx.write_all(b"baz").await?;
    let (_btx, brx) = b.accept().await.context("no stream")?;
    drop(brx);
    let stopped = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if tx.write_all(b"more").await.is_err() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(
        stopped.is_ok(),
        "writes went on after the peer stopped reading"
    );

    // A closed channel fails what waits on it: one closes once its link and every stream
    // over it are gone.
    let (a, b) = mem();
    let (a, b) = (Link::new(a, Side::Offer), Link::new(b, Side::Answer));
    let (_tx, mut rx) = a.open()?;
    drop(b);
    let mut body = Vec::new();
    let read = tokio::time::timeout(Duration::from_secs(5), rx.read_to_end(&mut body)).await?;
    assert!(read.is_err());
    Ok(())
}

#[cfg(feature = "webrtc")]
mod rtc {
    use std::sync::Arc;

    use anyhow::Context as _;
    use tokio::sync::mpsc;
    use webrtc::data_channel::RTCDataChannelInit;
    use webrtc::peer_connection::{
        PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCConfigurationBuilder,
        RTCIceGatheringState,
    };
    use wrpc_webrtc::{DataChannel, Link, Server, Side};

    struct Handler {
        gathered: mpsc::UnboundedSender<()>,
        channels: mpsc::UnboundedSender<Arc<dyn webrtc::data_channel::DataChannel>>,
    }

    #[async_trait::async_trait]
    impl PeerConnectionEventHandler for Handler {
        async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
            if state == RTCIceGatheringState::Complete {
                let _ = self.gathered.send(());
            }
        }

        async fn on_data_channel(&self, channel: Arc<dyn webrtc::data_channel::DataChannel>) {
            let _ = self.channels.send(channel);
        }
    }

    async fn peer() -> anyhow::Result<(
        Arc<dyn PeerConnection>,
        mpsc::UnboundedReceiver<()>,
        mpsc::UnboundedReceiver<Arc<dyn webrtc::data_channel::DataChannel>>,
    )> {
        let (gathered, gathered_rx) = mpsc::unbounded_channel();
        let (channels, channels_rx) = mpsc::unbounded_channel();
        let pc = PeerConnectionBuilder::new()
            .with_configuration(RTCConfigurationBuilder::new().build())
            .with_handler(Arc::new(Handler { gathered, channels }))
            .with_runtime(webrtc::runtime::default_runtime().context("no runtime")?)
            .with_udp_addrs(vec!["127.0.0.1:0".to_string()])
            .build()
            .await?;
        Ok((Arc::new(pc), gathered_rx, channels_rx))
    }

    /// Over a channel both ends make (negotiated out of band), as a browser can hand only a
    /// channel it made to a worker.
    #[test_log::test(tokio::test(flavor = "multi_thread"))]
    async fn over_webrtc() -> anyhow::Result<()> {
        let (offer, mut offer_gathered, _) = peer().await?;
        let (answer, mut answer_gathered, _) = peer().await?;
        let init = || {
            Some(RTCDataChannelInit {
                negotiated: Some(0),
                ..Default::default()
            })
        };
        let channel = offer.create_data_channel("wrpc", init()).await?;
        let accepted = answer.create_data_channel("wrpc", init()).await?;
        offer
            .set_local_description(offer.create_offer(None).await?)
            .await?;
        offer_gathered.recv().await;
        answer
            .set_remote_description(offer.local_description().await.context("no offer")?)
            .await?;
        answer
            .set_local_description(answer.create_answer(None).await?)
            .await?;
        answer_gathered.recv().await;
        offer
            .set_remote_description(answer.local_description().await.context("no answer")?)
            .await?;

        let client = Link::new(DataChannel::open(channel).await?, Side::Offer);
        let server = Arc::new(Link::new(DataChannel::open(accepted).await?, Side::Answer));
        let srv = Arc::new(Server::new());
        super::serve(server, srv.clone());
        wrpc_test::assert_single_invocation((), &client, srv.as_ref()).await?;
        assert!(!client.is_closed());
        let _ = (offer.close().await, answer.close().await);
        Ok(())
    }
}
