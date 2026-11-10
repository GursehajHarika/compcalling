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
const VAPI_SECRET = (process.env.VAPI_SECRET_TOKEN || '').trim();

// Use environment variables or fallback to IDs extracted from your previous logs
const VAPI_ASSISTANT_ID = (process.env.VAPI_ASSISTANT_ID || '944f6021-5305-4895-ad7d-8782fddfcc10').trim();
const VAPI_PHONE_NUMBER_ID = (process.env.VAPI_PHONE_NUMBER_ID || '8ead1121-e11a-4bd0-b3ba-e1feaacdb242').trim();

app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// In-memory call registry
const callStore = new Map();

function broadcast(event, data) {
  const payload = JSON.stringify({ event, data });
  wss.clients.forEach((client) => {
    if (client.readyState === 1) client.send(payload);
  });
}

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
    if (c && typeof c === 'object' && Object.keys(c).length > 0) return c;
  }
  return null;
}

function extractStructuredOutputs(data) {
  if (!data || typeof data !== 'object') return {};
  const normalized = {};

  const fieldMap = {
    'brand': ['brand', 'carrier', 'company', 'organization'],
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

async function syncPastCallsFromVapi() {
  if (!VAPI_SECRET) return;

  try {
    const response = await fetch('https://api.vapi.ai/call?limit=50', {
      headers: {
        'Authorization': `Bearer ${VAPI_SECRET}`,
        'Content-Type': 'application/json'
      }
    });

    if (!response.ok) return;

    const pastCalls = await response.json();
    if (Array.isArray(pastCalls)) {
      pastCalls.forEach((call) => {
        const rawStructured = findStructuredData(call) || {};
        const parsed = extractStructuredOutputs(rawStructured);

        const record = {
          id: call.id,
          status: call.status || 'ended',
          controlUrl: call.monitor?.controlUrl || call.controlUrl,
          customerNumber: call.customer?.number || call.destination?.number || 'Outbound Call',
          startedAt: call.startedAt || call.createdAt,
          endedAt: call.endedAt,
          updatedAt: call.updatedAt || call.createdAt,
          endedReason: call.endedReason || 'completed',
          structuredOutputs: {
            brand: parsed.brand || '—',
            systemUsed: parsed.systemUsed || '—',
            hoops: parsed.hoops || '—',
            ivrOptions: parsed.ivrOptions || '—',
            timeToAgent: parsed.timeToAgent || '—',
            cxToAgent: parsed.cxToAgent || '—',
            waitTimes: parsed.waitTimes || '—'
          }
        };

        if (!callStore.has(call.id) || callStore.get(call.id).status === 'ended') {
          callStore.set(call.id, record);
        }
      });
    }
  } catch (err) {
    console.error('[Vapi Sync Exception]:', err.message);
  }
}

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

// START CALL ENDPOINT
app.post('/api/calls/start', async (req, res) => {
  const { targetNumber } = req.body;

  if (!VAPI_SECRET) {
    return res.status(500).json({ error: 'Server missing VAPI_SECRET_TOKEN' });
  }
  if (!targetNumber) {
    return res.status(400).json({ error: 'Target phone number is required' });
  }

  console.log(`[Vapi Outbound] Initiating call to: ${targetNumber}`);

  try {
    const response = await fetch('https://api.vapi.ai/call', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${VAPI_SECRET}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        assistantId: VAPI_ASSISTANT_ID,
        phoneNumberId: VAPI_PHONE_NUMBER_ID,
        customer: {
          number: targetNumber
        }
      })
    });

    if (!response.ok) {
      const errText = await response.text();
      console.error(`[Vapi Outbound Error ${response.status}]:`, errText);
      return res.status(response.status).json({ error: errText });
    }

    const callData = await response.json();
    return res.json({ success: true, call: callData });
  } catch (err) {
    console.error('[Vapi Outbound Exception]:', err.message);
    return res.status(500).json({ error: err.message });
  }
});

// END CALL ENDPOINT
app.post('/api/calls/:id/end', async (req, res) => {
  const { id } = req.params;

  if (!VAPI_SECRET) {
    return res.status(500).json({ error: 'Server missing VAPI_SECRET_TOKEN' });
  }

  try {
    const getCall = await fetch(`https://api.vapi.ai/call/${id}`, { headers: { 'Authorization': `Bearer ${VAPI_SECRET}` } });
    let controlUrl = callStore.get(id)?.controlUrl;
    if (getCall.ok) {
      const callData = await getCall.json();
      controlUrl = callData.monitor?.controlUrl || callData.controlUrl;
    }

    if (controlUrl) {
      try {
        await fetch(controlUrl, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ type: 'control', control: 'end-call' })
        });
      } catch (e) {}
    }

    try {
      await fetch(`https://api.vapi.ai/call/${id}/say`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${VAPI_SECRET}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ message: "Ending call.", endCallAfterSpoken: true })
      });
    } catch (e) {}

    await fetch(`https://api.vapi.ai/call/${id}`, {
      method: 'DELETE',
      headers: {
        'Authorization': `Bearer ${VAPI_SECRET}`,
        'Content-Type': 'application/json'
      }
    });

    if (callStore.has(id)) {
      const c = callStore.get(id);
      c.status = 'ended';
      c.endedReason = 'manually-ended';
      c.endedAt = new Date().toISOString();
      callStore.set(id, c);
      broadcast('CALL_UPDATED', c);
    }

    return res.json({ success: true, message: 'Hangup dispatched' });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));
app.get('/health', (req, res) => res.status(200).send('OK'));

// Webhook for live events
app.post('/webhook/vapi', (req, res) => {
  const { message } = req.body;
  if (!message || !message.call) return res.status(200).json({ received: true });

  const call = message.call;
  const callId = call.id;
  const now = new Date().toISOString();

  let record = callStore.get(callId) || {
    id: callId,
    status: call.status || 'in-progress',
    customerNumber: call.customer?.number || 'Outbound Call',
    startedAt: call.startedAt || now,
    updatedAt: now,
    structuredOutputs: {
      brand: 'Identifying...',
      systemUsed: 'Scanning...',
      hoops: 'Scanning...',
      ivrOptions: 'Listening...',
      timeToAgent: 'Pending...',
      cxToAgent: 'Pending...',
      waitTimes: 'Measuring...'
    }
  };

  const liveControlUrl = call.monitor?.controlUrl || message.monitor?.controlUrl || call.controlUrl;
  if (liveControlUrl) record.controlUrl = liveControlUrl;

  if (message.type === 'transcript') {
    const text = message.transcript || message.text;
    if (text) {
      record.transcript = record.transcript || [];
      const segment = {
        role: message.role || (message.transcriptType === 'final' ? 'user' : 'assistant'),
        text: text.trim(),
        time: now,
        isFinal: message.transcriptType === 'final'
      };

      if (record.transcript.length > 0 && !record.transcript[record.transcript.length - 1].isFinal && !segment.isFinal) {
        record.transcript[record.transcript.length - 1] = segment;
      } else {
        record.transcript.push(segment);
      }
      broadcast('TRANSCRIPT_UPDATE', { callId, segment });
    }
  }

  const rawStructured = findStructuredData(message) || findStructuredData(call);
  if (rawStructured) {
    const parsed = extractStructuredOutputs(rawStructured);
    record.structuredOutputs = { ...record.structuredOutputs, ...parsed };
  }

  if (message.type === 'status-update') {
    record.status = call.status || record.status;
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

wss.on('connection', (ws) => {
  const sorted = Array.from(callStore.values()).sort(
    (a, b) => new Date(b.startedAt) - new Date(a.startedAt)
  );
  ws.send(JSON.stringify({ event: 'INIT_STATE', data: sorted }));
});

process.on('uncaughtException', (err) => console.error('[Uncaught Exception]:', err));
process.on('unhandledRejection', (reason, promise) => console.error('[Unhandled Rejection]:', reason));
process.on('SIGTERM', () => {
  setTimeout(() => server.close(() => process.exit(0)), 5000);
});

server.listen(PORT, HOST, () => {
  console.log(`Comp Calling actively listening on http://${HOST}:${PORT}`);
});