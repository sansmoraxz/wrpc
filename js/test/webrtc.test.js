import assert from "node:assert/strict";
import { test } from "node:test";

import * as t from "../src/types.js";
import { invoke, accept } from "../src/session.js";
import { Link, MAX_MESSAGE } from "../src/webrtc.js";

/**
 * One end of an in-memory channel, as a data channel behaves: each message delivered
 * whole, in order, on a later task, with a send buffer that drains as it is delivered.
 */
class FakeChannel {
  binaryType = "blob";
  bufferedAmount = 0;
  bufferedAmountLowThreshold = 0;
  readyState = "open";
  /** The length of each message sent. @type {number[]} */
  sent = [];
  /** @type {FakeChannel | undefined} */
  peer = undefined;
  /** @type {Map<string, Set<(event: any) => void>>} */
  listeners = new Map();

  /** @param {string} type @param {(event: any) => void} listener */
  addEventListener(type, listener) {
    if (!this.listeners.has(type)) this.listeners.set(type, new Set());
    this.listeners.get(type)?.add(listener);
  }

  /** @param {string} type @param {(event: any) => void} listener */
  removeEventListener(type, listener) {
    this.listeners.get(type)?.delete(listener);
  }

  /** @param {string} type @param {any} event */
  emit(type, event) {
    for (const listener of [...(this.listeners.get(type) ?? [])]) listener(event);
  }

  /** @param {Uint8Array} message */
  send(message) {
    if (this.readyState !== "open") throw new Error("closed");
    assert.ok(message.length <= MAX_MESSAGE, `a message of ${message.length} bytes`);
    this.sent.push(message.length);
    this.bufferedAmount += message.length;
    const copy = message.slice().buffer;
    setTimeout(() => {
      this.bufferedAmount -= message.length;
      if (this.bufferedAmount <= this.bufferedAmountLowThreshold)
        this.emit("bufferedamountlow", {});
      if (this.peer?.readyState === "open") this.peer.emit("message", { data: copy });
    }, 0);
  }

  close() {
    for (const end of [this, this.peer]) {
      if (!end || end.readyState === "closed") continue;
      end.readyState = "closed";
      setTimeout(() => end.emit("close", {}), 0);
    }
  }
}

/** @returns {[FakeChannel, FakeChannel]} */
function channelPair() {
  const a = new FakeChannel();
  const b = new FakeChannel();
  a.peer = b;
  b.peer = a;
  return [a, b];
}

/**
 * @template T
 * @param {T | null | undefined} value
 * @returns {T}
 */
function must(value) {
  assert.ok(value != null);
  return value;
}

/** @param {import("../src/index.js").Transport} transport @param {unknown} reason */
function abort(transport, reason) {
  return must(transport.abort)(reason);
}

/** Serves `echo(list<u8>) -> list<u8>` and `add(u32, u32) -> u32` on what `link` accepts. */
/** @param {Link} link */
async function serve(link) {
  for (;;) {
    const transport = await link.accept();
    if (!transport) return;
    (async () => {
      const inv = await accept(transport);
      if (inv.func === "echo") {
        const { params } = await inv.receiveParams([t.list(t.u8)]);
        await inv.sendResults([t.list(t.u8)], params);
      } else {
        const {
          params: [x, y],
        } = await inv.receiveParams([t.u32, t.u32]);
        await inv.sendResults([t.u32], [x + y]);
      }
      await inv.finished;
    })().catch(() => {});
  }
}

test("both ends invoke over one channel, large values interleaved with small", async () => {
  const [a, b] = channelPair();
  const offer = new Link(a, "offer");
  const answer = new Link(b, "answer");
  serve(offer);
  serve(answer);
  const big = Uint8Array.from({ length: 3 << 20 }, (_, i) => i % 251);
  const calls = [];
  for (const [from, n] of /** @type {[Link, number][]} */ ([
    [offer, 0],
    [answer, 1],
  ])) {
    calls.push(
      invoke(from.open(), "x", "echo", [t.list(t.u8)], [big], [t.list(t.u8)]).then(async (r) => {
        await r.finished;
        assert.deepEqual(r.results[0], big);
        return "big";
      }),
    );
    for (let i = 0; i < 20; i++) {
      calls.push(
        invoke(from.open(), "x", "add", [t.u32, t.u32], [i, n], [t.u32]).then(async (r) => {
          await r.finished;
          assert.equal(r.results[0], i + n);
          return "small";
        }),
      );
    }
  }
  /** @type {string[]} */
  const order = [];
  await Promise.all(calls.map((c) => c.then((kind) => order.push(kind))));
  // Small calls made after a big one finish before it: the big one's frames interleave.
  assert.notEqual(order[0], "big");
  assert.ok(Math.max(...a.sent, ...b.sent) <= MAX_MESSAGE);
  offer.close();
  assert.equal(await answer.accept(), null);
});

test("the send buffer bounds what waits to be sent", async () => {
  const [a, b] = channelPair();
  const offer = new Link(a, "offer");
  const answer = new Link(b, "answer");
  serve(answer);
  let most = 0;
  const watch = setInterval(() => (most = Math.max(most, a.bufferedAmount)), 0);
  const big = new Uint8Array(8 << 20);
  const r = await invoke(offer.open(), "x", "echo", [t.list(t.u8)], [big], [t.list(t.u8)]);
  await r.finished;
  clearInterval(watch);
  assert.equal(r.results[0].length, big.length);
  assert.ok(most <= (1 << 20) + MAX_MESSAGE, `${most} bytes waited`);
  offer.close();
});

test("a stream reset or a channel closed reaches the other end", async () => {
  const [a, b] = channelPair();
  const offer = new Link(a, "offer");
  const answer = new Link(b, "answer");
  const sent = offer.open();
  await sent.write(Uint8Array.of(1, 2, 3));
  const got = must(await answer.accept());
  assert.deepEqual(await got.read(), Uint8Array.of(1, 2, 3));
  await abort(sent, new Error("gave up"));
  await assert.rejects(got.read(), /reset/);

  // The receiver stopping tells the sender.
  const second = offer.open();
  await second.write(Uint8Array.of(9));
  const theirs = must(await answer.accept());
  await abort(theirs, new Error("done"));
  await new Promise((r) => setTimeout(r, 5));
  await assert.rejects(async () => second.write(Uint8Array.of(9)), /stopped/);

  const pending = offer.open();
  await pending.write(Uint8Array.of(7));
  const open = must(await answer.accept());
  assert.deepEqual(await open.read(), Uint8Array.of(7));
  const reading = open.read();
  offer.close();
  await assert.rejects(reading, /closed/);
  assert.throws(() => offer.open(), /closed/);
});
