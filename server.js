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

// Multi-Agent Routing IDs (Configurable in Railway Variables later)
const AGENTS = {
  'billing_english': process.env.VAPI_BILLING_EN_ID || VAPI_ASSISTANT_ID,
  'billing_french': process.env.VAPI_BILLING_FR_ID || VAPI_ASSISTANT_ID,
  'sales_english': process.env.VAPI_SALES_EN_ID || VAPI_ASSISTANT_ID,
  'sales_french': process.env.VAPI_SALES_FR_ID || VAPI_ASSISTANT_ID,
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
    'systemUsed': ['what was the system used', 'system_used', 'system used'],
    'hoops': ['hoops', 'hoop'],
    'ivrOptions': ['ivr va options', 'ivr_va_options', 'ivr options'],
    'timeToAgent': ['time to reach agent', 'time_to_reach_agent', 'time to agent'],
    'cxToAgent': ['customer experience to agent', 'cx to agent'],
    'waitTimes': ['wait times', 'wait time']
  };
  for (const [key, val] of Object.entries(data)) {
    let fieldName = key;
    let fieldValue = val;
    if (val && typeof val === 'object' && val.name && val.result !== undefined) {
      fieldName = val.name; fieldValue = val.result;
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
  for (const c of candidates) { if (c && typeof c === 'object' && Object.keys(c).length > 0) return c; }
  return null;
}

function parseHistoricalTranscript(call) {
  if (Array.isArray(call.messages)) {
    return call.messages.filter(m => m.message || m.content).map(m => ({
      role: m.role || 'assistant', text: m.message || m.content, time: m.time || m.timestamp || null
    }));
  }
  if (typeof call.transcript === 'string' && call.transcript.trim()) {
    return [{ role: 'system', text: call.transcript, time: null }];
  }
  return [];
}

async function makeVapiOutboundCall(targetNumber, useCase = 'billing', language = 'english') {
  const routingKey = `${useCase}_${language}`.toLowerCase();
  const selectedAssistantId = AGENTS[routingKey] || VAPI_ASSISTANT_ID;

  const response = await fetch('https://api.vapi.ai/call', {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${VAPI_SECRET}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      assistantId: selectedAssistantId,
      phoneNumberId: VAPI_PHONE_NUMBER_ID,
      customer: { number: targetNumber }
    })
  });
  if (!response.ok) throw new Error(await response.text());
  return await response.json();
}

// Scheduled Call Execution Engine
setInterval(() => {
  const now = Date.now();
  let dbChanged = false;
  for (const [id, call] of callStore.entries()) {
    if (call.status === 'scheduled') {
      const executeTime = new Date(call.startedAt).getTime();
      if (now >= executeTime) {
        console.log(`[Scheduler] Auto-dialing ${call.structuredOutputs.brand} (${call.structuredOutputs.useCase}/${call.structuredOutputs.language})`);
        const useCase = call.structuredOutputs.useCase;
        const language = call.structuredOutputs.language;
        
        callStore.delete(id);
        dbChanged = true;
        broadcast('CALL_DELETED', { id });
        
        makeVapiOutboundCall(call.customerNumber, useCase, language).catch(err => {
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
    const response = await fetch('https://api.vapi.ai/call?limit=50', {
      headers: { 'Authorization': `Bearer ${VAPI_SECRET}`, 'Content-Type': 'application/json' }
    });
    if (!response.ok) return;
    const pastCalls = await response.json();
    let dbChanged = false;
    if (Array.isArray(pastCalls)) {
      pastCalls.forEach((call) => {
        const parsed = extractStructuredOutputs(findStructuredData(call) || {});
        const record = {
          id: call.id,
          status: call.status || 'ended',
          controlUrl: call.monitor?.controlUrl || call.controlUrl,
          customerNumber: call.customer?.number || call.destination?.number || 'Outbound Call',
          startedAt: call.startedAt || call.createdAt,
          endedAt: call.endedAt,
          updatedAt: call.updatedAt || call.createdAt,
          endedReason: call.endedReason || 'completed',
          transcript: parseHistoricalTranscript(call),
          structuredOutputs: {
            brand: parsed.brand || '—', systemUsed: parsed.systemUsed || '—',
            hoops: parsed.hoops || '—', ivrOptions: parsed.ivrOptions || '—',
            timeToAgent: parsed.timeToAgent || '—', cxToAgent: parsed.cxToAgent || '—',
            waitTimes: parsed.waitTimes || '—',
            useCase: parsed.useCase || 'Unknown', language: parsed.language || 'Unknown', segment: parsed.segment || 'Unknown'
          }
        };
        if (!callStore.has(call.id) || callStore.get(call.id).status === 'ended') {
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
        for (const [id, call] of callStore.entries()) {
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

        const fakeId = 'sched_' + Date.now();
        const scheduledRecord = {
          id: fakeId, status: 'scheduled', customerNumber: targetNumber, bucket: scheduleBucket,
          startedAt: scheduledTime.toISOString(), updatedAt: new Date().toISOString(),
          structuredOutputs: { brand: brand || 'Manual Dial', segment: segment, useCase: useCase, language: language, systemUsed: '—', hoops: '—', ivrOptions: '—', timeToAgent: '—', cxToAgent: '—', waitTimes: '—' }
        };
        callStore.set(fakeId, scheduledRecord);
        saveDB();
        broadcast('CALL_UPDATED', scheduledRecord);
        return res.json({ success: true, scheduled: true, call: scheduledRecord });
      }
    }
    
    // Live execution uses the specific routing agent
    const callData = await makeVapiOutboundCall(targetNumber, useCase, language);
    return res.json({ success: true, scheduled: false, call: callData });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

app.delete('/api/calls/schedule/:id', (req, res) => {
  const { id } = req.params;
  if (callStore.has(id) && callStore.get(id).status === 'scheduled') {
    callStore.delete(id);
    saveDB();
    broadcast('CALL_DELETED', { id });
    return res.json({ success: true });
  }
  return res.status(404).json({ error: 'Scheduled call not found' });
});

app.post('/api/calls/:id/end', async (req, res) => {
  const { id } = req.params;
  if (!VAPI_SECRET) return res.status(500).json({ error: 'Missing VAPI key' });
  try {
    await fetch(`https://api.vapi.ai/call/${id}`, { method: 'DELETE', headers: { 'Authorization': `Bearer ${VAPI_SECRET}` } });
    let c = callStore.get(id);
    if (!c) c = { id, customerNumber: 'Unknown', structuredOutputs: {} };
    c.status = 'ended'; c.endedReason = 'manually-ended'; c.endedAt = new Date().toISOString();
    callStore.set(id, c);
    saveDB();
    broadcast('CALL_UPDATED', c);
    return res.json({ success: true });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

// Health check routes
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));
app.get('/health', (req, res) => res.status(200).send('OK'));

app.post('/webhook/vapi', (req, res) => {
  const { message } = req.body;
  if (!message || !message.call) return res.status(200).json({ received: true });
  const call = message.call;
  const callId = call.id;
  const now = new Date().toISOString();

  let record = callStore.get(callId) || {
    id: callId, status: call.status || 'in-progress',
    customerNumber: call.customer?.number || 'Outbound Call',
    startedAt: call.startedAt || now, updatedAt: now,
    transcript: [],
    structuredOutputs: { brand: 'Identifying...', systemUsed: 'Scanning...', hoops: 'Scanning...', ivrOptions: 'Listening...', timeToAgent: 'Pending...', cxToAgent: 'Pending...', waitTimes: 'Measuring...' }
  };
  const liveControlUrl = call.monitor?.controlUrl || message.monitor?.controlUrl || call.controlUrl;
  if (liveControlUrl) record.controlUrl = liveControlUrl;

  if (message.type === 'transcript') {
    const text = message.transcript || message.text;
    if (text) {
      record.transcript = record.transcript || [];
      const segment = { role: message.role || (message.transcriptType === 'final' ? 'user' : 'assistant'), text: text.trim(), time: now, isFinal: message.transcriptType === 'final' };
      if (record.transcript.length > 0 && !record.transcript[record.transcript.length - 1].isFinal && !segment.isFinal) {
        record.transcript[record.transcript.length - 1] = segment;
      } else { record.transcript.push(segment); }
      broadcast('TRANSCRIPT_UPDATE', { callId, segment });
    }
  }
  
  if (message.type === 'conversation-update' && Array.isArray(message.messages)) {
    record.transcript = message.messages.map(m => ({ role: m.role || 'assistant', text: m.message || m.content || '', time: m.time || now }));
  }

  const rawStructured = findStructuredData(message) || findStructuredData(call);
  if (rawStructured) record.structuredOutputs = { ...record.structuredOutputs, ...extractStructuredOutputs(rawStructured) };

  if (message.type === 'status-update') {
    record.status = call.status || record.status;
  } else if (message.type === 'end-of-call-report') {
    record.status = 'ended'; 
    record.endedReason = message.endedReason || 'completed';
    if (call.endedAt) record.endedAt = call.endedAt;
    if (Array.isArray(message.artifact?.messages)) {
      record.transcript = message.artifact.messages.map(m => ({ role: m.role || 'assistant', text: m.message || m.content || '', time: m.time || now }));
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