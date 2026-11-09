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
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// In-memory call registry: callId -> Call Object
const activeCalls = new Map();

// Broadcast data to all connected WebSocket clients
function broadcast(event, data) {
  const payload = JSON.stringify({ event, data });
  wss.clients.forEach((client) => {
    if (client.readyState === 1) { // 1 = OPEN
      client.send(payload);
    }
  });
}

// WebSocket Connection Handler
wss.on('connection', (ws) => {
  // Send current active calls immediately upon connection
  ws.send(JSON.stringify({
    event: 'INIT_STATE',
    data: Array.from(activeCalls.values())
  }));
});

// Vapi Webhook Endpoint
app.post('/webhook/vapi', (req, res) => {
  // Optional security check
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

  switch (message.type) {
    case 'status-update':
      if (['in-progress', 'ringing', 'queued'].includes(call.status)) {
        const callData = {
          id: callId,
          status: call.status,
          customerNumber: call.customer?.number || 'Unknown',
          assistantId: call.assistantId || 'Default Assistant',
          startedAt: call.startedAt || now,
          updatedAt: now,
          cost: call.cost || 0
        };
        activeCalls.set(callId, callData);
        broadcast('CALL_UPDATED', callData);
      } else if (['ended', 'failed'].includes(call.status)) {
        activeCalls.delete(callId);
        broadcast('CALL_ENDED', { id: callId, status: call.status, endedAt: now });
      }
      break;

    case 'end-of-call-report':
      activeCalls.delete(callId);
      broadcast('CALL_ENDED', { 
        id: callId, 
        endedReason: message.endedReason || 'completed',
        summary: message.summary || '',
        endedAt: now 
      });
      break;

    case 'transcript':
      // Forward real-time transcript snippets if active
      if (activeCalls.has(callId)) {
        broadcast('CALL_TRANSCRIPT', {
          id: callId,
          role: message.role,
          transcript: message.transcript
        });
      }
      break;
  }

  return res.status(200).json({ success: true });
});

// Health check endpoint for Railway
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'healthy', activeCalls: activeCalls.size });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Server listening on port ${PORT}`);
});
