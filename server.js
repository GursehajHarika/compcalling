const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const cors = require('cors');
const path = require('path');
require('dotenv').config();

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const PORT = parseInt(process.env.PORT, 10) || 8080;
const HOST = '0.0.0.0';
const VAPI_SECRET = process.env.VAPI_SECRET_TOKEN;

// Middleware
app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// In-memory call registry: callId -> Call Object
const callStore = new Map();

// Broadcast event payload to all open WebSocket connections
function broadcast(event, data) {
  const payload = JSON.stringify({ event, data });
  wss.clients.forEach((client) => {
    if (client.readyState === 1) { // 1 = OPEN
      client.send(payload);
    }
  });
}

// Deep scanner to inspect all potential locations for structured outputs
function findStructuredData(obj) {
  if (!obj || typeof obj !== 'object') return null;

  const candidates = [
    obj.artifact?.structuredOutputs,
    obj.call?.artifact?.structuredOutputs,
    obj.analysis?.structuredData,
    obj.call?.analysis?.structuredData,
    obj.artifact?.structuredData,
    obj.call?.artifact?.structuredData,
    obj.structuredOutputs,
    obj.structuredData,
    obj.message?.artifact?.structuredOutputs,
    obj.message?.analysis?.structuredData,
    obj.message?.structuredData
  ];

  for (const c of candidates) {
    if (c && typeof c === 'object' && Object.keys(c).length > 0) {
      return c;
    }
  }
  return null;
}

// Normalizer for Vapi's UUID-based structured output objects: { name, result }
function extractStructuredOutputs(data) {
  if (!data || typeof data !== 'object') return {};
  const normalized = {};

  const fieldMap = {
    'systemUsed': ['what was the system used', 'system_used', 'system used', 'what was the system used?'],
    'hoops': ['hoops', 'hoop'],
    'ivrOptions': ['ivr va options', 'ivr_va_options', 'ivr options', 'ivr va option'],
    'timeToAgent': ['time to reach agent', 'time_to_reach_agent', 'time to agent'],
    'cxToAgent': ['customer experience to agent', 'customer_experience_to_agent', 'cx to agent'],
    'waitTimes': ['wait times', 'wait_times', 'wait time', 'wait_time']
  };

  for (const [key, val] of Object.entries(data)) {
    let fieldName = key;
    let fieldValue = val;

    // Unpack UUID-keyed objects: { name: "...", result: "..." }
    if (val && typeof val === 'object' && val.name && val.result !== undefined) {
      fieldName = val.name;
      fieldValue = val.result;
    }

    const cleanName = String(fieldName).trim().toLowerCase();
    for (const [targetKey, synonyms] of Object.entries(fieldMap)) {
      if (synonyms.includes(cleanName)) {
        normalized[targetKey] = fieldValue;
      }
    }
    normalized[fieldName] = fieldValue;
  }
  return normalized;
}

// Sync past calls directly from Vapi's REST API
async function syncPastCallsFromVapi() {
  const apiKey = (process.env.VAPI_SECRET_TOKEN || '').trim();
  if (!apiKey) {
    console.warn('[Vapi Sync] Skipping sync: VAPI_SECRET_TOKEN is not configured.');
    return;
  }

  try {
    console.log('[Vapi Sync] Fetching call history from Vapi...');
    const response = await fetch('https://api.vapi.ai/call?limit=50', {
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json'
      }
    });

    if (!response.ok) {
      console.error(`[Vapi Sync Error] Status ${response.status}: ${await response.text()}`);
      return;
    }

    const pastCalls = await response.json();
    if (Array.isArray(pastCalls)) {
      pastCalls.forEach((call) => {
        const rawStructured = findStructuredData(call) || {};
        const parsed = extractStructuredOutputs(rawStructured);

        const record = {
          id: call.id,
          status: call.status || 'ended',
          customerNumber: call.customer?.number || call.destination?.number || 'Outbound Scout',
          assistantName: 'Rogers Assistant',
          startedAt: call.startedAt || call.createdAt,
          endedAt: call.endedAt,
          updatedAt: call.updatedAt || call.createdAt,
          endedReason: call.endedReason || 'completed',
          structuredOutputs: {
            systemUsed: parsed.systemUsed || '—',
            hoops: parsed.hoops || '—',
            ivrOptions: parsed.ivrOptions || '—',
            timeToAgent: parsed.timeToAgent || '—',
            cxToAgent: parsed.cxToAgent || '—',
            waitTimes: parsed.waitTimes || '—'
          },
          rawOutputs: rawStructured
        };

        if (!callStore.has(call.id) || callStore.get(call.id).status === 'ended') {
          callStore.set(call.id, record);
        }
      });
      console.log(`[Vapi Sync] Successfully synced ${pastCalls.length} calls from Vapi.`);
    }
  } catch (err) {
    console.error('[Vapi Sync Exception]:', err.message);
  }
}

// Perform initial sync on boot
syncPastCallsFromVapi();

// API Endpoints
app.get('/api/sync-history', async (req, res) => {
  await syncPastCallsFromVapi();
  const sorted = Array.from(callStore.values()).sort(
    (a, b) => new Date(b.startedAt) - new Date(a.startedAt)
  );
  broadcast('INIT_STATE', sorted);
  res.json({ success: true, count: sorted.length, calls: sorted });
});

app.get('/api/calls', (req, res) => {
  const sorted = Array.from(callStore.values()).sort(
    (a, b) => new Date(b.startedAt) - new Date(a.startedAt)
  );
  res.json(sorted);
});

// Terminate an active call via Vapi REST API
app.post('/api/calls/:id/end', async (req, res) => {
  const { id } = req.params;
  const apiKey = (process.env.VAPI_SECRET_TOKEN || '').trim();

  if (!apiKey) {
    console.error('[Vapi End Call] VAPI_SECRET_TOKEN is missing');
    return res.status(500).json({ error: 'Server missing VAPI_SECRET_TOKEN' });
  }

  console.log(`[Vapi API] Initiating hangup for call: ${id}`);

  try {
    const response = await fetch(`https://api.vapi.ai/call/${id}`, {
      method: 'DELETE',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json'
      }
    });

    const respText = await response.text();
    console.log(`[Vapi API Result ${response.status}]:`, respText);

    if (response.ok || response.status === 404) {
      if (callStore.has(id)) {
        const c = callStore.get(id);
        c.status = 'ended';
        c.endedReason = 'manually-ended';
        c.endedAt = new Date().toISOString();
        callStore.set(id, c);
        broadcast('CALL_UPDATED', c);
      }
      return res.json({ success: true, message: 'Call ended' });
    }

    return res.status(response.status).json({ error: respText });
  } catch (err) {
    console.error('[Vapi End Call Exception]:', err.message);
    return res.status(500).json({ error: err.message });
  }
});

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.get('/health', (req, res) => {
  res.status(200).send('OK');
});

// Vapi Webhook Handler
app.post('/webhook/vapi', (req, res) => {
  const { message } = req.body;
  if (!message || !message.call) {
    return res.status(200).json({ received: true });
  }

  const call = message.call;
  const callId = call.id;
  const now = new Date().toISOString();

  let record = callStore.get(callId) || {
    id: callId,
    status: call.status || 'in-progress',
    customerNumber: call.customer?.number || 'Outbound Scout',
    assistantName: 'Rogers Assistant',
    startedAt: call.startedAt || now,
    updatedAt: now,
    structuredOutputs: {
      systemUsed: 'Scanning...',
      hoops: 'Scanning...',
      ivrOptions: 'Listening...',
      timeToAgent: 'Pending...',
      cxToAgent: 'Pending...',
      waitTimes: 'Scouting...'
    },
    rawOutputs: {}
  };

  const rawStructured = findStructuredData(message) || findStructuredData(call);
  if (rawStructured) {
    const parsed = extractStructuredOutputs(rawStructured);
    record.structuredOutputs = { ...record.structuredOutputs, ...parsed };
    record.rawOutputs = { ...record.rawOutputs, ...rawStructured };
  }

  if (message.type === 'status-update') {
    record.status = call.status;
  } else if (message.type === 'end-of-call-report') {
    record.status = 'ended';
    record.endedReason = message.endedReason || 'completed';
    if (call.endedAt) record.endedAt = call.endedAt;
  }

  record.updatedAt = now;
  callStore.set(callId, record);
  broadcast('CALL_UPDATED', record);

  return res.status(200).json({ success: true });
});

// WebSocket Connection Lifecycle
wss.on('connection', (ws) => {
  const sorted = Array.from(callStore.values()).sort(
    (a, b) => new Date(b.startedAt) - new Date(a.startedAt)
  );
  ws.send(JSON.stringify({ event: 'INIT_STATE', data: sorted }));
});

// Process Protection
process.on('uncaughtException', (err) => {
  console.error('[Uncaught Exception]:', err);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('[Unhandled Rejection] at:', promise, 'reason:', reason);
});

process.on('SIGTERM', () => {
  console.log('Received SIGTERM, closing gracefully');
  server.close(() => {
    process.exit(0);
  });
});

// Start Server
server.listen(PORT, HOST, () => {
  console.log(`Scout Monitor actively listening on http://${HOST}:${PORT}`);
});