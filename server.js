const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const cors = require('cors');
const path = require('path');
require('dotenv').config();

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const PORT = process.env.PORT || 3000;
const VAPI_SECRET = process.env.VAPI_SECRET_TOKEN;

app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// In-memory store: callId -> Call Details
const callStore = new Map();

function broadcast(event, data) {
  const payload = JSON.stringify({ event, data });
  wss.clients.forEach((client) => {
    if (client.readyState === 1) {
      client.send(payload);
    }
  });
}

wss.on('connection', (ws) => {
  ws.send(JSON.stringify({
    event: 'INIT_STATE',
    data: Array.from(callStore.values())
  }));
});

// Helper to normalize structured outputs into your defined schema
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
    const cleanKey = key.trim().toLowerCase();
    for (const [targetKey, synonyms] of Object.entries(fieldMap)) {
      if (synonyms.includes(cleanKey)) {
        normalized[targetKey] = val;
      }
    }
    // Also keep the raw key if not mapped
    normalized[key] = val;
  }
  return normalized;
}

app.post('/webhook/vapi', (req, res) => {
  if (VAPI_SECRET && req.headers['x-vapi-secret'] !== VAPI_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

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

  switch (message.type) {
    case 'status-update':
      record.status = call.status;
      record.updatedAt = now;

      if (message.artifact?.structuredData) {
        const parsed = extractStructuredOutputs(message.artifact.structuredData);
        record.structuredOutputs = { ...record.structuredOutputs, ...parsed };
        record.rawOutputs = { ...record.rawOutputs, ...message.artifact.structuredData };
      }
      callStore.set(callId, record);
      broadcast('CALL_UPDATED', record);
      break;

    case 'tool-calls':
      if (Array.isArray(message.toolCalls)) {
        message.toolCalls.forEach((tool) => {
          try {
            const args = typeof tool.function?.arguments === 'string'
              ? JSON.parse(tool.function.arguments)
              : tool.function?.arguments;

            if (args) {
              const parsed = extractStructuredOutputs(args);
              record.structuredOutputs = { ...record.structuredOutputs, ...parsed };
              record.rawOutputs = { ...record.rawOutputs, ...args };
            }
          } catch (e) {
            console.error('Failed to parse tool call args', e);
          }
        });
      }
      record.updatedAt = now;
      callStore.set(callId, record);
      broadcast('CALL_UPDATED', record);
      break;

    case 'end-of-call-report':
      record.status = 'ended';
      record.updatedAt = now;
      record.endedReason = message.endedReason || 'completed';

      const finalOutputs = message.analysis?.structuredData || message.artifact?.structuredData || message.structuredData;
      if (finalOutputs) {
        const parsed = extractStructuredOutputs(finalOutputs);
        record.structuredOutputs = { ...record.structuredOutputs, ...parsed };
        record.rawOutputs = { ...record.rawOutputs, ...finalOutputs };
      }

      callStore.set(callId, record);
      broadcast('CALL_UPDATED', record);
      break;
  }

  return res.status(200).json({ success: true });
});

app.get('/health', (req, res) => {
  res.status(200).json({ status: 'healthy', calls: callStore.size });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Scout Monitor running on port ${PORT}`);
});