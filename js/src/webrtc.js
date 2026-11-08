// The WebRTC transport: invocations multiplexed over one data channel, as `wrpc-webrtc`
// does in Rust, frame for frame.
//
// A data channel carries messages, not a byte stream, and browsers cap a message's size.
// So every message is one frame of one stream,
//
//   [stream id: u32, little endian][kind: u8][payload]
//
// and a stream is what wRPC needs per invocation: two directions, each ended on its own.
// Its id's lowest bit is the side that opened it (even for the end that offered, odd for
// the one that answered), so both ends open streams without agreeing first. A frame is at
// most `MAX_MESSAGE` bytes, which every browser takes, and the frames of streams written at
// once interleave, so a large value holds up a small one behind it by a frame at most.
//
// One channel per link rather than one per invocation: a page can hand a data channel to a
// worker only in the task that made it, so a worker could not open one per call itself.

/** @typedef {import("./mux.js").Transport} Transport */

/** The largest message a frame takes, its header included. */
export const MAX_MESSAGE = 64 * 1024;

const HEADER = 5;

/** The most a link's frames may wait in its channel's send buffer. */
const WINDOW = 1 << 20;

/** Bytes of a stream. */
const DATA = 0;
/** The sender writes no more on this stream. */
const END = 1;
/** The sender gave up on this stream before ending it. */
const RESET = 2;
/** The receiver reads no more of this stream: the sender should stop. */
const STOP = 3;

/**
 * An ordered, reliable carrier of binary messages, as an `RTCDataChannel` is: what a
 * {@link Link} needs of one.
 * @typedef {object} Channel
 * @property {(message: Uint8Array) => void} send
 * @property {number} bufferedAmount
 * @property {number} bufferedAmountLowThreshold
 * @property {string} readyState
 * @property {string} binaryType
 * @property {(type: string, listener: (event: any) => void) => void} addEventListener
 * @property {(type: string, listener: (event: any) => void) => void} removeEventListener
 * @property {() => void} close
 */

/** @param {number} id @param {number} kind @param {Uint8Array} [payload] */
function frame(id, kind, payload) {
  const length = payload ? payload.length : 0;
  const out = new Uint8Array(HEADER + length);
  new DataView(out.buffer).setUint32(0, id, true);
  out[4] = kind;
  if (payload) out.set(payload, HEADER);
  return out;
}

/**
 * The peer's streams this end is done receiving: every id below `floor`, and those in
 * `above`. What tells a frame of a new stream from a late one of an old: a peer opens
 * streams in order of id, but may write to them in any.
 */
class Done {
  /** @param {number} floor */
  constructor(floor) {
    this.floor = floor;
    /** @type {Set<number>} */
    this.above = new Set();
  }

  /** @param {number} id */
  has(id) {
    return id < this.floor || this.above.has(id);
  }

  /** @param {number} id */
  add(id) {
    if (id >= this.floor) this.above.add(id);
    while (this.above.delete(this.floor)) this.floor += 2;
  }
}

/** One stream of a link: its receive side's queue, and whether each side is over. */
class Stream {
  /** @param {Link} link @param {number} id */
  constructor(link, id) {
    this.link = link;
    this.id = id;
    /** @type {Uint8Array[]} */
    this.chunks = [];
    /** @type {((value: Uint8Array | null) => void) | undefined} */
    this.wake = undefined;
    /** @type {((err: unknown) => void) | undefined} */
    this.fail = undefined;
    /** Ended (`null`), reset or the link gone: what reading finds after the chunks. @type {null | Error | undefined} */
    this.ended = undefined;
    this.receiving = true;
    this.sending = true;
    /** The receiver stopped reading. */
    this.stopped = false;
  }

  /** @param {Uint8Array} chunk */
  push(chunk) {
    if (this.wake) {
      const wake = this.wake;
      this.wake = this.fail = undefined;
      wake(chunk);
    } else {
      this.chunks.push(chunk);
    }
  }

  /** The receive side is over: `null` at its end, or why not. @param {null | Error} why */
  finish(why) {
    if (this.ended !== undefined) return;
    this.ended = why;
    this.receiving = false;
    if (this.wake) {
      const [wake, fail] = [this.wake, this.fail];
      this.wake = this.fail = undefined;
      why === null ? wake(null) : fail?.(why);
    }
    this.link._finished(this.id);
    this.link._forget(this);
  }

  /** @returns {Promise<Uint8Array | null>} */
  read() {
    const chunk = this.chunks.shift();
    if (chunk) return Promise.resolve(chunk);
    if (this.ended === null) return Promise.resolve(null);
    if (this.ended) return Promise.reject(this.ended);
    return new Promise((resolve, reject) => {
      this.wake = resolve;
      this.fail = reject;
    });
  }

  /** @param {Uint8Array} bytes */
  async write(bytes) {
    if (!this.sending) throw new Error("the stream's send side is closed");
    for (let at = 0; at < bytes.length;) {
      if (this.stopped) throw new Error("the receiver stopped reading");
      await this.link._room();
      const n = Math.min(bytes.length - at, MAX_MESSAGE - HEADER);
      this.link._send(frame(this.id, DATA, bytes.subarray(at, at + n)));
      at += n;
    }
  }

  closeWrite() {
    if (!this.sending) return;
    this.sending = false;
    this.link._send(frame(this.id, END));
    this.link._forget(this);
  }

  /** Gives up on both sides: the peer is told by a reset, and a stop. */
  abort() {
    if (this.sending) {
      this.sending = false;
      this.link._send(frame(this.id, RESET));
    }
    if (this.receiving) {
      this.link._send(frame(this.id, STOP));
      this.finish(new Error("aborted"));
    }
    this.link._forget(this);
  }

  /** @returns {Transport} */
  transport() {
    return {
      read: () => this.read(),
      write: (bytes) => this.write(bytes),
      closeWrite: () => this.closeWrite(),
      abort: () => this.abort(),
    };
  }
}

/**
 * One data channel's invocations, both ways: {@link open} gives a {@link Transport} for an
 * invocation of the peer's, {@link accept} the next the peer opened, for `accept` to serve.
 * `side` must differ at the two ends: `"offer"` for the end that offered the peer
 * connection, `"answer"` for the other.
 */
export class Link {
  /** @param {Channel} channel @param {"offer" | "answer"} side */
  constructor(channel, side) {
    this.channel = channel;
    this.bit = side === "offer" ? 0 : 1;
    this.next = 0;
    this.closed = false;
    /** @type {Map<number, Stream>} */
    this.streams = new Map();
    this.done = new Done(1 - this.bit);
    /** @type {Stream[]} */
    this.incoming = [];
    /** @type {((stream: Stream | null) => void)[]} */
    this.accepting = [];
    /** @type {(() => void)[]} */
    this.waiting = [];
    channel.binaryType = "arraybuffer";
    channel.bufferedAmountLowThreshold = WINDOW / 2;
    this._onMessage = (/** @type {MessageEvent} */ event) =>
      this._receive(new Uint8Array(event.data));
    this._onLow = () => this._drained();
    this._onClose = () => this._gone();
    channel.addEventListener("message", this._onMessage);
    channel.addEventListener("bufferedamountlow", this._onLow);
    channel.addEventListener("close", this._onClose);
    if (channel.readyState === "closed") this._gone();
  }

  /** A stream to the peer, for one invocation. @returns {Transport} */
  open() {
    if (this.closed) throw new Error("the link is closed");
    const id = this.next * 2 + this.bit;
    this.next += 1;
    const stream = new Stream(this, id);
    this.streams.set(id, stream);
    return stream.transport();
  }

  /** The next stream the peer opened, `null` once the channel closed. @returns {Promise<Transport | null>} */
  accept() {
    const stream = this.incoming.shift();
    if (stream) return Promise.resolve(stream.transport());
    if (this.closed) return Promise.resolve(null);
    return new Promise((resolve) => this.accepting.push((s) => resolve(s ? s.transport() : null)));
  }

  /** Closes the channel: the peer's streams end. */
  close() {
    this.channel.close();
    this._gone();
  }

  /** @param {Uint8Array} message */
  _receive(message) {
    if (message.length < HEADER) return;
    const id = new DataView(message.buffer, message.byteOffset, message.byteLength).getUint32(
      0,
      true,
    );
    const kind = message[4];
    const payload = message.subarray(HEADER);
    let stream = this.streams.get(id);
    switch (kind) {
      case DATA:
      case END:
        if (!stream && (id & 1) !== this.bit && !this.done.has(id)) {
          stream = new Stream(this, id);
          this.streams.set(id, stream);
          const accept = this.accepting.shift();
          if (accept) accept(stream);
          else this.incoming.push(stream);
        }
        if (!stream?.receiving) return;
        if (kind === DATA) {
          if (payload.length) stream.push(payload);
        } else {
          stream.finish(null);
        }
        return;
      case RESET:
        if (stream?.receiving) stream.finish(new Error("the peer reset the stream"));
        else this._finished(id);
        return;
      case STOP:
        if (stream) stream.stopped = true;
        return;
    }
  }

  /** @param {Uint8Array} message */
  _send(message) {
    if (this.closed) return;
    try {
      this.channel.send(message);
    } catch {
      this._gone();
    }
  }

  /** Waits until the channel's send buffer has room in the window. */
  async _room() {
    while (!this.closed && this.channel.bufferedAmount >= WINDOW) {
      await new Promise((resolve) => this.waiting.push(() => resolve(undefined)));
    }
    if (this.closed) throw new Error("the link is closed");
  }

  _drained() {
    for (const wake of this.waiting.splice(0)) wake();
  }

  /** This end is done receiving stream `id`. @param {number} id */
  _finished(id) {
    if ((id & 1) !== this.bit) this.done.add(id);
  }

  /** A stream with both sides over is forgotten. @param {Stream} stream */
  _forget(stream) {
    if (!stream.receiving && !stream.sending && this.streams.get(stream.id) === stream) {
      this.streams.delete(stream.id);
    }
  }

  _gone() {
    if (this.closed) return;
    this.closed = true;
    this.channel.removeEventListener("message", this._onMessage);
    this.channel.removeEventListener("bufferedamountlow", this._onLow);
    this.channel.removeEventListener("close", this._onClose);
    for (const stream of [...this.streams.values()]) {
      stream.sending = false;
      if (stream.receiving) stream.finish(new Error("the link is closed"));
    }
    this.streams.clear();
    for (const accept of this.accepting.splice(0)) accept(null);
    this._drained();
  }
}
