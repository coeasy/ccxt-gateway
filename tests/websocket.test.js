import test from 'node:test';
import assert from 'node:assert/strict';
import WebSocket from 'ws';
import { buildApp } from '../src/app.js';
import { createFakeCcxt, testConfig } from './fakes.js';

function nextMessage(socket, predicate, timeoutMs = 3000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      socket.off('message', onMessage);
      reject(new Error('Timed out waiting for WebSocket message'));
    }, timeoutMs);
    const onMessage = (raw) => {
      const message = JSON.parse(raw.toString());
      if (!predicate(message)) return;
      clearTimeout(timer);
      socket.off('message', onMessage);
      resolve(message);
    };
    socket.on('message', onMessage);
  });
}

test('WebSocket requires first-frame auth and shares live CCXT streams', async (t) => {
  const { ccxt, metrics } = createFakeCcxt({ watchDelayMs: 10 });
  const { fastify } = buildApp({ config: testConfig(), ccxt, logger: false });
  await fastify.listen({ host: '127.0.0.1', port: 0 });
  const address = fastify.server.address();
  const url = `ws://127.0.0.1:${address.port}/ws`;
  const socket = new WebSocket(url);
  let secondSocket;
  t.after(async () => {
    socket.close();
    secondSocket?.close();
    await fastify.close();
  });
  await new Promise((resolve, reject) => {
    socket.once('open', resolve);
    socket.once('error', reject);
  });

  const ready = nextMessage(socket, (message) => message.type === 'ready');
  socket.send(JSON.stringify({ op: 'auth', token: 'test-api-key-0123456789abcdef-0123456789' }));
  await ready;

  const subscribed = nextMessage(socket, (message) => message.type === 'subscribed');
  socket.send(JSON.stringify({ op: 'subscribe', id: 'btc', exchange: 'binance', channel: 'ticker', symbol: 'BTC/USDT' }));
  await subscribed;
  const raceSubscribed = nextMessage(socket, (message) => message.type === 'subscribed' && message.id === 'race');
  const duplicateId = nextMessage(socket, (message) => message.type === 'error' && message.id === 'race' && message.error === 'duplicate_subscription_id');
  const raceRequest = JSON.stringify({ op: 'subscribe', id: 'race', exchange: 'binance', channel: 'ticker', symbol: 'BTC/USDT' });
  socket.send(raceRequest);
  socket.send(raceRequest);
  await raceSubscribed;
  await duplicateId;
  const raceUnsubscribed = nextMessage(socket, (message) => message.type === 'unsubscribed' && message.id === 'race');
  socket.send(JSON.stringify({ op: 'unsubscribe', id: 'race' }));
  await raceUnsubscribed;
  const data = await nextMessage(socket, (message) => message.type === 'data');
  assert.equal(data.data.symbol, 'BTC/USDT');
  assert.ok(data.subscriptionIds.includes('btc'));
  assert.ok(metrics.watchCalls >= 1);

  const fundingSubscribed = nextMessage(socket, (message) => message.type === 'subscribed' && message.id === 'funding');
  socket.send(JSON.stringify({
    op: 'subscribe', id: 'funding', exchange: 'binance', channel: 'fundingRate', symbol: 'BTC/USDT:USDT',
  }));
  await fundingSubscribed;
  const fundingUpdate = await nextMessage(socket, (message) => message.type === 'data' && message.channel === 'fundingRate');
  assert.equal(fundingUpdate.data.fundingRate, 0.0001);
  const fundingUnsubscribed = nextMessage(socket, (message) => message.type === 'unsubscribed' && message.id === 'funding');
  socket.send(JSON.stringify({ op: 'unsubscribe', id: 'funding' }));
  await fundingUnsubscribed;

  secondSocket = new WebSocket(url);
  await new Promise((resolve, reject) => {
    secondSocket.once('open', resolve);
    secondSocket.once('error', reject);
  });
  const secondReady = nextMessage(secondSocket, (message) => message.type === 'ready');
  secondSocket.send(JSON.stringify({ op: 'auth', token: 'test-api-key-0123456789abcdef-0123456789' }));
  await secondReady;
  const secondSubscribed = nextMessage(secondSocket, (message) => message.type === 'subscribed');
  secondSocket.send(JSON.stringify({ op: 'subscribe', id: 'btc-2', exchange: 'binance', channel: 'ticker', symbol: 'BTC/USDT' }));
  await secondSubscribed;
  const secondData = await nextMessage(secondSocket, (message) => message.type === 'data');
  assert.ok(secondData.subscriptionIds.includes('btc-2'));
  assert.equal(metrics.maxConcurrentWatches, 1);

  const unsubscribed = nextMessage(socket, (message) => message.type === 'unsubscribed');
  socket.send(JSON.stringify({ op: 'unsubscribe', id: 'btc' }));
  await unsubscribed;
  const afterFirstLeaves = await nextMessage(secondSocket, (message) => message.type === 'data');
  assert.ok(afterFirstLeaves.subscriptionIds.includes('btc-2'));
  const secondUnsubscribed = nextMessage(secondSocket, (message) => message.type === 'unsubscribed');
  secondSocket.send(JSON.stringify({ op: 'unsubscribe', id: 'btc-2' }));
  await secondUnsubscribed;
});

test('limits distinct upstream WebSocket streams', async (t) => {
  const { ccxt } = createFakeCcxt();
  const { fastify } = buildApp({ config: testConfig({ wsMaxStreams: 1 }), ccxt, logger: false });
  await fastify.listen({ host: '127.0.0.1', port: 0 });
  const address = fastify.server.address();
  const socket = new WebSocket(`ws://127.0.0.1:${address.port}/ws`);
  t.after(async () => {
    socket.close();
    await fastify.close();
  });
  await new Promise((resolve, reject) => {
    socket.once('open', resolve);
    socket.once('error', reject);
  });

  const ready = nextMessage(socket, (message) => message.type === 'ready');
  socket.send(JSON.stringify({ op: 'auth', token: testConfig().apiKey }));
  await ready;
  const subscribed = nextMessage(socket, (message) => message.type === 'subscribed' && message.id === 'first');
  socket.send(JSON.stringify({ op: 'subscribe', id: 'first', exchange: 'binance', channel: 'ticker', symbol: 'BTC/USDT' }));
  await subscribed;

  const rejected = nextMessage(socket, (message) => message.type === 'error' && message.id === 'second');
  socket.send(JSON.stringify({ op: 'subscribe', id: 'second', exchange: 'binance', channel: 'ticker', symbol: 'ETH/USDT' }));
  const error = await rejected;
  assert.equal(error.error, 'subscription_failed');
});

test('rejects an invalid WebSocket token before opening an exchange stream', async (t) => {
  const { ccxt, metrics } = createFakeCcxt();
  const { fastify } = buildApp({ config: testConfig(), ccxt, logger: false });
  await fastify.listen({ host: '127.0.0.1', port: 0 });
  const address = fastify.server.address();
  const socket = new WebSocket(`ws://127.0.0.1:${address.port}/ws`);
  t.after(async () => {
    socket.close();
    await fastify.close();
  });
  await new Promise((resolve, reject) => {
    socket.once('open', resolve);
    socket.once('error', reject);
  });

  const denied = nextMessage(socket, (message) => message.type === 'error' && message.error === 'unauthorized');
  const closed = new Promise((resolve) => socket.once('close', (code) => resolve(code)));
  socket.send(JSON.stringify({ op: 'auth', token: 'wrong-token' }));
  await denied;
  assert.equal(await closed, 4401);
  assert.equal(metrics.instances, 0);
});
