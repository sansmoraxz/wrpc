//! A webrtc-rs data channel as a [`Channel`].

use std::io;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use webrtc::data_channel::{DataChannelEvent, RTCDataChannelState};

use crate::Channel;

/// An open webrtc-rs data channel. Make it with [`DataChannel::open`], which waits for it to
/// open; its events are this transport's from then on.
pub struct DataChannel(Arc<dyn webrtc::data_channel::DataChannel>);

impl DataChannel {
    /// Waits for `channel` to open, if it has not.
    ///
    /// # Errors
    ///
    /// Returns an error if the channel closes first.
    pub async fn open(channel: Arc<dyn webrtc::data_channel::DataChannel>) -> io::Result<Self> {
        if channel.ready_state().await.map_err(io::Error::other)? != RTCDataChannelState::Open {
            loop {
                match channel.poll().await {
                    Some(DataChannelEvent::OnOpen) => break,
                    Some(DataChannelEvent::OnClose) | None => {
                        return Err(io::ErrorKind::ConnectionRefused.into());
                    }
                    Some(_) => {}
                }
            }
        }
        Ok(Self(channel))
    }

    /// The channel.
    #[must_use]
    pub fn get(&self) -> &Arc<dyn webrtc::data_channel::DataChannel> {
        &self.0
    }
}

impl Channel for DataChannel {
    async fn send(&self, message: Bytes) -> io::Result<()> {
        // Not copied: a frame is its one owner.
        let message = message
            .try_into_mut()
            .unwrap_or_else(|shared| BytesMut::from(&shared[..]));
        self.0.send(message).await.map_err(io::Error::other)
    }

    async fn close(&self) {
        let _ = self.0.close().await;
    }

    async fn recv(&self) -> Option<Bytes> {
        loop {
            match self.0.poll().await? {
                DataChannelEvent::OnMessage(message) => return Some(message.data.freeze()),
                DataChannelEvent::OnClose => return None,
                _ => {}
            }
        }
    }
}
