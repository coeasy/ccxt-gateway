import { secureTokenEquals } from './auth.js';

const CHANNELS = new Set(['ticker', 'orderbook', 'trades', 'ohlcv']);
const MAX_SUBSCRIPTIONS_PER_CLIENT = 50;
const MAX_QUEUED_MESSAGES = 100;
const AUTH_TIMEOUT_MS = 5_000;

function send(socket, value) {
  if (socket.readyState !== 1) return;
  try {
    socket.send(JSON.stringify(value));
  } catch {
    socket.close(1011, 'Unable to encode response');
  }
}

function validId(value) {
  return typeof value === 'string' && value.length > 0 && value.length <= 128;
}

export function registerWebSocket(fastify, { hub, apiKey, maxClients = 500 }) {
  let clients = 0;
  fastify.get('/ws', { websocket: true }, (socket) => {
    if (clients >= maxClients) {
      socket.close(1013, 'Server is at capacity');
      return;
    }
    clients += 1;
    let counted = true;
    let authenticated = false;
    const subscriptions = new Map();
    let queuedMessages = 0;
    let messageQueue = Promise.resolve();

    const authTimer = setTimeout(() => {
      if (!authenticated) socket.close(4401, 'Authentication required');
    }, AUTH_TIMEOUT_MS);
    authTimer.unref?.();

    const cleanup = () => {
      clearTimeout(authTimer);
      if (counted) {
        counted = false;
        clients -= 1;
      }
      for (const unsubscribe of subscriptions.values()) unsubscribe();
      subscriptions.clear();
    };

    socket.on('close', cleanup);
    socket.on('error', cleanup);
    socket.on('message', (raw) => {
      if (queuedMessages >= MAX_QUEUED_MESSAGES) {
        socket.close(1008, 'Too many queued messages');
        return;
      }
      queuedMessages += 1;
      messageQueue = messageQueue
        .then(() => handleMessage(raw))
        .catch(() => { send(socket, { type: 'error', error: 'internal_error' }); })
        .finally(() => { queuedMessages -= 1; });
    });

    async function handleMessage(raw) {
      if (socket.readyState !== 1) return;
      let message;
      try {
        message = JSON.parse(raw.toString());
      } catch {
        send(socket, { type: 'error', error: 'invalid_json' });
        return;
      }
      if (message === null || Array.isArray(message) || typeof message !== 'object') {
        send(socket, { type: 'error', error: 'invalid_message' });
        return;
      }

      if (!authenticated) {
        if (message.op !== 'auth' || !secureTokenEquals(message.token, apiKey)) {
          send(socket, { type: 'error', error: 'unauthorized' });
          socket.close(4401, 'Unauthorized');
          return;
        }
        authenticated = true;
        clearTimeout(authTimer);
        send(socket, { type: 'ready' });
        return;
      }

      if (message.op === 'ping') {
        send(socket, { type: 'pong', id: message.id });
        return;
      }

      if (message.op === 'unsubscribe') {
        if (!validId(message.id)) {
          send(socket, { type: 'error', error: 'invalid_subscription_id' });
          return;
        }
        const unsubscribe = subscriptions.get(message.id);
        if (unsubscribe) {
          subscriptions.delete(message.id);
          unsubscribe();
        }
        send(socket, { type: 'unsubscribed', id: message.id });
        return;
      }

      if (message.op !== 'subscribe') {
        send(socket, { type: 'error', error: 'unsupported_operation' });
        return;
      }

      if (!validId(message.id) || typeof message.exchange !== 'string' || typeof message.symbol !== 'string' || !CHANNELS.has(message.channel)) {
        send(socket, { type: 'error', error: 'invalid_subscription' });
        return;
      }
      if (subscriptions.has(message.id)) {
        send(socket, { type: 'error', id: message.id, error: 'duplicate_subscription_id' });
        return;
      }
      if (subscriptions.size >= MAX_SUBSCRIPTIONS_PER_CLIENT) {
        send(socket, { type: 'error', error: 'subscription_limit_reached' });
        return;
      }
      if (message.channel === 'ohlcv' && typeof message.timeframe !== 'string') {
        send(socket, { type: 'error', id: message.id, error: 'timeframe_required' });
        return;
      }

      try {
        const unsubscribe = await hub.subscribe(socket, {
          exchange: message.exchange,
          channel: message.channel,
          symbol: message.symbol,
          timeframe: message.timeframe,
          limit: Number.isSafeInteger(message.limit) ? Math.min(Math.max(message.limit, 1), 1000) : undefined,
        }, message.id);
        if (socket.readyState !== 1) {
          unsubscribe();
          return;
        }
        subscriptions.set(message.id, unsubscribe);
        send(socket, { type: 'subscribed', id: message.id, exchange: message.exchange, channel: message.channel, symbol: message.symbol });
      } catch (error) {
        send(socket, { type: 'error', id: message.id, error: 'subscription_failed', message: 'The requested stream is unavailable' });
      }
    }
  });
}
