const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
require('dotenv').config();

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const PORT = process.env.PORT || 8080;
const VAPI_SECRET = (process.env.VAPI_SECRET_TOKEN || '').trim();

// Default Fallback
const VAPI_ASSISTANT_ID = (process.env.VAPI_ASSISTANT_ID || '944f6021-5305-4895-ad7d-8782fddfcc10').trim();
const VAPI_PHONE_NUMBER_ID = (process.env.VAPI_PHONE_NUMBER_ID || '8ead1121-e11a-4bd0-b3ba-e1feaacdb242').trim();

// Multi-Agent Routing IDs (Configurable in Railway Variables)
const AGENTS = {
  'billing_english': process.env.VAPI_BILLING_EN_ID || VAPI_ASSISTANT_ID,
  'billing_french': process.env.VAPI_BILLING_FR_ID || VAPI_ASSISTANT_ID,
  'sales_english': process.env.VAPI_SALES_EN_ID || VAPI_ASSISTANT_ID,
  'sales_french': process.env.VAPI_SALES_FR_ID || VAPI_ASSISTANT_ID,
};

// Fallback number lookup for historical Vapi calls without metadata
const NUMBER_FALLBACK_MAP = {
  '+18887643771': { brand: 'Rogers', segment: 'Consumer' },
  '+18667272141': { brand: 'Rogers', segment: 'Small Business', useCase: 'Billing' },
  '+18772680444': { brand: 'Rogers', segment: 'Small Business', useCase: 'Sales' },
  '+18777421888': { brand: 'Rogers', segment: 'Small Business' },
  '+18663011942': { brand: 'Bell', segment: 'Consumer' },
  '+18884662415': { brand: 'Bell', segment: 'Small Business' },
  '+18665582273': { brand: 'Telus', segment: 'Consumer' },
  '+18888112323': { brand: 'Telus', segment: 'Small Business' },
  '+18775120911': { brand: 'Videotron', segment: 'Consumer' },
  '+18775128590': { brand: 'Videotron', segment: 'Small Business' },
  '+18883823366': { brand: 'Videotron', segment: 'Small Business' },
  '+18884813436': { brand: 'Fido', segment: 'Consumer' },
  '+18889992321': { brand: 'Virgin Plus', segment: 'Consumer' },
  '+18669956636': { brand: 'Koodo', segment: 'Consumer' },
  '+18779463184': { brand: 'Freedom Mobile', segment: 'Consumer' }
};

app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// Persistent Cache
const dataDir = path.join(__dirname, 'data');
if (!fs.existsSync(dataDir)) {
  try { fs.mkdirSync(dataDir, { recursive: true }); } catch (e) {}
}
const dbPath = path.join(dataDir, 'calls.json');
const callStore = new Map();

function loadDB() {
  try {
    if (fs.existsSync(dbPath)) {
      const data = JSON.parse(fs.readFileSync(dbPath, 'utf8'));
      for (const [key, value] of Object.entries(data)) callStore.set(key, value);
    }
  } catch (e) {}
}
function saveDB() {
  try { fs.writeFileSync(dbPath, JSON.stringify(Object.fromEntries(callStore), null, 2), 'utf8'); } catch (e) {}
}
loadDB();

function broadcast(event, data) {
  const payload = JSON.stringify({ event, data });
  wss.clients.forEach((c) => { if (c.readyState === 1) c.send(payload); });
}

// Extraction Utilities
function extractStructuredOutputs(data) {
  if (!data || typeof data !== 'object') return {};
  const normalized = {};
  const fieldMap = {
    'brand': ['brand', 'carrier', 'company', 'organization'],
    'segment': ['segment', 'customer segment', 'line of business', 'lob'],
    'useCase': ['usecase', 'use_case', 'use case', 'department', 'intent'],
    'language': ['language', 'lang'],
    'systemUsed': ['what was the system used', 'system_used', 'system used'],
    'hoops': ['hoops', 'hoop'],
    'ivrOptions': ['ivr va options', 'ivr_va_options', 'ivr options'],
    'timeToAgent': ['time to reach agent', 'time_to_reach_agent', 'time to agent', 'time_to_agent', 'tta'],
    'cxToAgent': ['customer experience to agent', 'cx to agent'],
    'waitTimes': ['wait times', 'wait time']
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
      if (synonyms.includes(cleanName)) normalized[targetKey] = fieldValue;
    }
    normalized[fieldName] = fieldValue;
  }
  return normalized;
}

function findStructuredData(obj) {
  if (!obj || typeof obj !== 'object') return null;
  const candidates = [
    obj.artifact?.structuredOutputs, obj.call?.artifact?.structuredOutputs,
    obj.analysis?.structuredData, obj.call?.analysis?.structuredData,
    obj.structuredOutputs, obj.structuredData
  ];
  for (const c of candidates) {
    if (c && typeof c === 'object' && Object.keys(c).length > 0) return c;
  }
  return null;
}

function isMeaningful(val) {
  if (val === null || val === undefined) return false;
  const s = String(val).trim().toLowerCase();
  return s !== '' && !['—', '-', 'unknown', 'identifying...', 'pending...', 'scanning...', 'listening...', 'measuring...', 'not identified'].includes(s);
}

function inferCallAttributes(call, existingOutputs = {}, parsedOutputs = {}) {
  const meta = call?.metadata || {};
  const number = call?.customer?.number || call?.destination?.number || '';
  const numLookup = NUMBER_FALLBACK_MAP[number] || {};

  let inferredUseCase = null;
  let inferredLanguage = null;
  const aid = call?.assistantId;
  if (aid) {
    if (process.env.VAPI_BILLING_EN_ID && aid === process.env.VAPI_BILLING_EN_ID.trim()) {
      inferredUseCase = 'Billing'; inferredLanguage = 'English';
    } else if (process.env.VAPI_BILLING_FR_ID && aid === process.env.VAPI_BILLING_FR_ID.trim()) {
      inferredUseCase = 'Billing'; inferredLanguage = 'French';
    } else if (process.env.VAPI_SALES_EN_ID && aid === process.env.VAPI_SALES_EN_ID.trim()) {
      inferredUseCase = 'Sales'; inferredLanguage = 'English';
    } else if (process.env.VAPI_SALES_FR_ID && aid === process.env.VAPI_SALES_FR_ID.trim()) {
      inferredUseCase = 'Sales'; inferredLanguage = 'French';
    }
  }

  const pick = (...vals) => vals.find(isMeaningful);

  return {
    brand: pick(meta.brand, parsedOutputs.brand, existingOutputs.brand, numLookup.brand) || '—',
    segment: pick(meta.segment, parsedOutputs.segment, existingOutputs.segment, numLookup.segment) || 'Consumer',
    useCase: pick(meta.useCase, parsedOutputs.useCase, existingOutputs.useCase, inferredUseCase, numLookup.useCase) || 'Billing',
    language: pick(meta.language, parsedOutputs.language, existingOutputs.language, inferredLanguage) || 'English'
  };
}

function parseHistoricalTranscript(call) {
  if (Array.isArray(call.messages)) {
    return call.messages.filter(m => m.message || m.content).map(m => ({
      role: m.role || 'assistant',
      text: m.message || m.content,
      time: m.time || m.timestamp || null
    }));
  }
  if (typeof call.transcript === 'string' && call.transcript.trim()) {
    return [{ role: 'system', text: call.transcript, time: null }];
  }
  return [];
}

async function makeVapiOutboundCall(targetNumber, useCase = 'Billing', language = 'English', extraMeta = {}) {
  const routingKey = `${useCase}_${language}`.toLowerCase();
  const selectedAssistantId = AGENTS[routingKey] || VAPI_ASSISTANT_ID;

  const metadata = {
    brand: extraMeta.brand || '',
    segment: extraMeta.segment || 'Consumer',
    useCase: useCase || 'Billing',
    language: language || 'English'
  };

  const response = await fetch('https://api.vapi.ai/call', {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${VAPI_SECRET}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      assistantId: selectedAssistantId,
      phoneNumberId: VAPI_PHONE_NUMBER_ID,
      customer: { number: targetNumber },
      metadata
    })
  });
  if (!response.ok) throw new Error(await response.text());
  const callData = await response.json();

  if (callData && callData.id) {
    const now = new Date().toISOString();
    const initialRecord = {
      id: callData.id,
      status: callData.status || 'queued',
      controlUrl: callData.monitor?.controlUrl || callData.controlUrl,
      customerNumber: targetNumber,
      startedAt: callData.startedAt || callData.createdAt || now,
      updatedAt: now,
      transcript: [],
      structuredOutputs: {
        brand: metadata.brand || 'Identifying...',
        segment: metadata.segment,
        useCase: metadata.useCase,
        language: metadata.language,
        systemUsed: 'Scanning...',
        hoops: 'Scanning...',
        ivrOptions: 'Listening...',
        timeToAgent: 'Pending...',
        cxToAgent: 'Pending...',
        waitTimes: 'Measuring...'
      }
    };
    callStore.set(callData.id, initialRecord);
    saveDB();
    broadcast('CALL_UPDATED', initialRecord);
  }

  return callData;
}

// Scheduled Call Execution Engine
setInterval(() => {
  const now = Date.now();
  let dbChanged = false;
  for (const [id, call] of callStore.entries()) {
    if (call.status === 'scheduled') {
      const executeTime = new Date(call.startedAt).getTime();
      if (now >= executeTime) {
        const s = call.structuredOutputs || {};
        console.log(`[Scheduler] Auto-dialing ${s.brand} (${s.useCase}/${s.language})`);

        callStore.delete(id);
        dbChanged = true;
        broadcast('CALL_DELETED', { id });

        makeVapiOutboundCall(call.customerNumber, s.useCase, s.language, {
          brand: s.brand,
          segment: s.segment
        }).catch(err => {
          console.error(`[Scheduler Error] Failed to dial:`, err.message);
        });
      }
    }
  }
  if (dbChanged) saveDB();
}, 5000);

async function syncPastCallsFromVapi() {
  if (!VAPI_SECRET) return;
  try {
    const response = await fetch('https://api.vapi.ai/call?limit=100', {
      headers: { 'Authorization': `Bearer ${VAPI_SECRET}`, 'Content-Type': 'application/json' }
    });
    if (!response.ok) return;
    const pastCalls = await response.json();
    let dbChanged = false;

    if (Array.isArray(pastCalls)) {
      pastCalls.forEach((call) => {
        const existing = callStore.get(call.id);
        const existingOutputs = existing?.structuredOutputs || {};
        const parsed = extractStructuredOutputs(findStructuredData(call) || {});
        const attrs = inferCallAttributes(call, existingOutputs, parsed);

        const record = {
          id: call.id,
          status: call.status || 'ended',
          controlUrl: call.monitor?.controlUrl || call.controlUrl || existing?.controlUrl,
          customerNumber: call.customer?.number || call.destination?.number || existing?.customerNumber || 'Outbound Call',
          startedAt: call.startedAt || call.createdAt || existing?.startedAt,
          endedAt: call.endedAt || existing?.endedAt,
          updatedAt: call.updatedAt || call.createdAt || new Date().toISOString(),
          endedReason: call.endedReason || existing?.endedReason || 'completed',
          transcript: parseHistoricalTranscript(call).length > 0 ? parseHistoricalTranscript(call) : (existing?.transcript || []),
          structuredOutputs: {
            brand: attrs.brand,
            segment: attrs.segment,
            useCase: attrs.useCase,
            language: attrs.language,
            systemUsed: parsed.systemUsed || existingOutputs.systemUsed || '—',
            hoops: parsed.hoops || existingOutputs.hoops || '—',
            ivrOptions: parsed.ivrOptions || existingOutputs.ivrOptions || '—',
            timeToAgent: parsed.timeToAgent || existingOutputs.timeToAgent || '—',
            cxToAgent: parsed.cxToAgent || existingOutputs.cxToAgent || '—',
            waitTimes: parsed.waitTimes || existingOutputs.waitTimes || '—'
          }
        };

        if (!existing || existing.status === 'ended' || call.status === 'ended') {
          callStore.set(call.id, record);
          dbChanged = true;
        }
      });
      if (dbChanged) saveDB();
    }
  } catch (err) {}
}
syncPastCallsFromVapi();

// API Endpoints
app.get('/api/sync-history', async (req, res) => {
  await syncPastCallsFromVapi();
  const sorted = Array.from(callStore.values()).sort((a, b) => new Date(b.startedAt) - new Date(a.startedAt));
  broadcast('INIT_STATE', sorted);
  res.json({ success: true, count: sorted.length, calls: sorted });
});

app.post('/api/calls/start', async (req, res) => {
  const { targetNumber, scheduleAt, scheduleBucket, brand, segment, useCase, language } = req.body;
  if (!VAPI_SECRET) return res.status(500).json({ error: 'Server missing VAPI_SECRET_TOKEN' });
  if (!targetNumber) return res.status(400).json({ error: 'Target phone number required' });

  try {
    if (scheduleAt && scheduleBucket) {
      const scheduledTime = new Date(scheduleAt);
      if (scheduledTime.getTime() > Date.now()) {
        const reqDateStr = scheduledTime.toISOString().split('T')[0];

        // SMART CONFLICT DETECTION
        for (const [, call] of callStore.entries()) {
          if (call.status === 'scheduled') {
            const existingDateStr = new Date(call.startedAt).toISOString().split('T')[0];
            if (
              existingDateStr === reqDateStr &&
              call.bucket === scheduleBucket &&
              call.structuredOutputs.brand === brand &&
              call.structuredOutputs.segment === segment &&
              call.structuredOutputs.useCase === useCase &&
              call.structuredOutputs.language === language
            ) {
              return res.status(409).json({ error: `Conflict: A ${useCase} (${language}) call to ${brand} ${segment} is already scheduled in this time block on ${reqDateStr}.` });
            }
          }
        }

        const fakeId = 'sched_' + Date.now() + '_' + Math.floor(Math.random() * 1000);
        const scheduledRecord = {
          id: fakeId,
          status: 'scheduled',
          customerNumber: targetNumber,
          bucket: scheduleBucket,
          startedAt: scheduledTime.toISOString(),
          updatedAt: new Date().toISOString(),
          structuredOutputs: {
            brand: brand || 'Manual Dial',
            segment: segment || 'Consumer',
            useCase: useCase || 'Billing',
            language: language || 'English',
            systemUsed: '—',
            hoops: '—',
            ivrOptions: '—',
            timeToAgent: '—',
            cxToAgent: '—',
            waitTimes: '—'
          }
        };
        callStore.set(fakeId, scheduledRecord);
        saveDB();
        broadcast('CALL_UPDATED', scheduledRecord);
        return res.json({ success: true, scheduled: true, call: scheduledRecord });
      }
    }

    const callData = await makeVapiOutboundCall(targetNumber, useCase, language, { brand, segment });
    return res.json({ success: true, scheduled: false, call: callData });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

function handleCancelScheduled(req, res) {
  const { id } = req.params;
  if (callStore.has(id) && callStore.get(id).status === 'scheduled') {
    callStore.delete(id);
    saveDB();
    broadcast('CALL_DELETED', { id });
    return res.json({ success: true });
  }
  return res.status(404).json({ error: 'Scheduled call not found' });
}
app.delete('/api/calls/schedule/:id', handleCancelScheduled);
app.delete('/api/calls/cancel/:id', handleCancelScheduled);

async function handleEndCall(req, res) {
  const { id } = req.params;
  if (!VAPI_SECRET) return res.status(500).json({ error: 'Missing VAPI key' });
  try {
    await fetch(`https://api.vapi.ai/call/${id}`, { method: 'DELETE', headers: { 'Authorization': `Bearer ${VAPI_SECRET}` } });
    let c = callStore.get(id);
    if (!c) c = { id, customerNumber: 'Unknown', structuredOutputs: {} };
    c.status = 'ended';
    c.endedReason = 'manually-ended';
    c.endedAt = new Date().toISOString();
    callStore.set(id, c);
    saveDB();
    broadcast('CALL_UPDATED', c);
    return res.json({ success: true });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
}
app.post('/api/calls/:id/end', handleEndCall);
app.post('/api/calls/terminate/:id', handleEndCall);

// Health check routes
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));
app.get('/health', (req, res) => res.status(200).send('OK'));

app.post('/webhook/vapi', (req, res) => {
  const { message } = req.body;
  if (!message || !message.call) return res.status(200).json({ received: true });
  const call = message.call;
  const callId = call.id;
  const now = new Date().toISOString();

  const existing = callStore.get(callId);
  const existingOutputs = existing?.structuredOutputs || {};
  const rawStructured = findStructuredData(message) || findStructuredData(call);
  const parsedStructured = rawStructured ? extractStructuredOutputs(rawStructured) : {};
  const attrs = inferCallAttributes(call, existingOutputs, parsedStructured);

  let record = existing || {
    id: callId,
    status: call.status || 'in-progress',
    customerNumber: call.customer?.number || 'Outbound Call',
    startedAt: call.startedAt || now,
    updatedAt: now,
    transcript: [],
    structuredOutputs: {
      brand: attrs.brand,
      segment: attrs.segment,
      useCase: attrs.useCase,
      language: attrs.language,
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

  if (message.type === 'conversation-update' && Array.isArray(message.messages)) {
    record.transcript = message.messages.map(m => ({
      role: m.role || 'assistant',
      text: m.message || m.content || '',
      time: m.time || now
    }));
  }

  record.structuredOutputs = {
    ...record.structuredOutputs,
    ...parsedStructured,
    brand: attrs.brand !== '—' ? attrs.brand : record.structuredOutputs.brand,
    segment: attrs.segment,
    useCase: attrs.useCase,
    language: attrs.language
  };

  if (message.type === 'status-update') {
    record.status = call.status || record.status;
  } else if (message.type === 'end-of-call-report') {
    record.status = 'ended';
    record.endedReason = message.endedReason || 'completed';
    if (call.endedAt) record.endedAt = call.endedAt;
    if (Array.isArray(message.artifact?.messages)) {
      record.transcript = message.artifact.messages.map(m => ({
        role: m.role || 'assistant',
        text: m.message || m.content || '',
        time: m.time || now
      }));
    } else if (typeof message.artifact?.transcript === 'string') {
      record.transcript = [{ role: 'system', text: message.artifact.transcript, time: now }];
    }
  }

  record.updatedAt = now;
  callStore.set(callId, record);
  saveDB();
  broadcast('CALL_UPDATED', record);
  return res.status(200).json({ success: true });
});

wss.on('connection', (ws) => {
  const sorted = Array.from(callStore.values()).sort((a, b) => new Date(b.startedAt) - new Date(a.startedAt));
  ws.send(JSON.stringify({ event: 'INIT_STATE', data: sorted }));
});

process.on('uncaughtException', (err) => console.error('[Exception]:', err));
process.on('unhandledRejection', (reason) => console.error('[Rejection]:', reason));
process.on('SIGTERM', () => setTimeout(() => server.close(() => process.exit(0)), 5000));
server.listen(PORT, () => console.log(`Comp Calling listening on port ${PORT}`));
