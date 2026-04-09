const express = require('express')
const Database = require('better-sqlite3')
const WebSocket = require('ws')
const http = require('http')
const path = require('path')

const app = express()
const server = http.createServer(app)
const wss = new WebSocket.Server({ server })
const db = new Database(path.join(__dirname, 'engagement.db'))

// ── Schema ────────────────────────────────────────────────────────
db.exec(`
  CREATE TABLE IF NOT EXISTS clients (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    industry TEXT,
    region TEXT
  );
  CREATE TABLE IF NOT EXISTS entities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    entity_type TEXT
  );
  CREATE TABLE IF NOT EXISTS deliverables (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id INTEGER NOT NULL,
    client_id INTEGER NOT NULL,
    type TEXT NOT NULL,
    period TEXT,
    status TEXT DEFAULT 'not_started',
    due_date TEXT,
    assigned_to TEXT
  );
  CREATE INDEX IF NOT EXISTS idx_ent_client ON entities(client_id);
  CREATE INDEX IF NOT EXISTS idx_del_entity ON deliverables(entity_id);
  CREATE INDEX IF NOT EXISTS idx_del_client ON deliverables(client_id);
`)

// ── Seed ──────────────────────────────────────────────────────────
function seed() {
  const existing = db.prepare('SELECT COUNT(*) as c FROM clients').get()
  if (existing.c > 0) {
    const total = db.prepare('SELECT COUNT(*) as c FROM deliverables').get().c
    console.log(`Database ready — ${total.toLocaleString()} deliverables`)
    return
  }

  console.log('Seeding database...')

  const nameA = ['Apex','Summit','Atlas','Meridian','Pinnacle','Sterling','Vantage','Clearwater','Ridgeline','Horizon','Cascade','Ironwood','Northgate','Westfield','Lakewood','Stonehaven','Riverdale','Highpoint','Fairview','Glenmore']
  const nameB = ['Group','Holdings','Partners','Capital','Industries','Resources','Solutions','Ventures','International','Enterprises','Properties','Management','Associates','Systems','Technologies']
  const industries = ['Manufacturing','Real Estate','Technology','Healthcare','Financial Services','Retail','Energy','Construction','Mining','Agriculture']
  const regions = ['Ontario','Quebec','British Columbia','Alberta','Manitoba','Nova Scotia','Saskatchewan','New Brunswick']
  const ePfx = ['Alpha','Beta','Gamma','Delta','Epsilon']
  const eTypes = ['Operating Company','Holding Company','Trust','Limited Partnership','Subsidiary','Joint Venture']
  const eSfx = ['Ltd.','Inc.','Corp.','LP','Trust','Holdings Inc.']
  const dTypes = ['Corporate Tax (T2)','GST/HST Return','Payroll Tax','Transfer Pricing','Trust Return (T3)','PST Return','SR&ED Claim','Personal Tax (T1)','Information Return (T5013)','Instalment Review']
  const periods = ['FY 2023','FY 2024','Q1 2024','Q2 2024','Q3 2024','Q4 2024','Q1 2025','Q2 2025']
  const statuses = ['not_started','not_started','in_progress','in_progress','review','filed','complete','complete']
  const assignees = ['Alex Chen','Jordan Smith','Morgan Lee','Taylor Brown','Casey Wilson','Riley Davis','Quinn Martinez','Avery Johnson','Sam Taylor','Drew Parker']

  const insC = db.prepare('INSERT INTO clients (name, industry, region) VALUES (?, ?, ?)')
  const insE = db.prepare('INSERT INTO entities (client_id, name, entity_type) VALUES (?, ?, ?)')
  const insD = db.prepare('INSERT INTO deliverables (entity_id, client_id, type, period, status, due_date, assigned_to) VALUES (?, ?, ?, ?, ?, ?, ?)')

  let delCount = 0
  let rng = 42 // deterministic seed

  function rand(n) { rng = (rng * 1664525 + 1013904223) & 0xffffffff; return Math.abs(rng) % n }

  db.transaction(() => {
    for (let c = 0; c < 200 && delCount < 10000; c++) {
      const name = `${nameA[c % nameA.length]} ${nameB[c % nameB.length]}${c >= nameA.length ? ` ${Math.floor(c / nameA.length) + 1}` : ''}`
      const rc = insC.run(name, industries[c % industries.length], regions[c % regions.length])
      const cId = rc.lastInsertRowid

      for (let e = 0; e < 5 && delCount < 10000; e++) {
        const eName = `${nameA[c % nameA.length]} ${ePfx[e]} ${eSfx[e % eSfx.length]}`
        const re = insE.run(cId, eName, eTypes[e % eTypes.length])
        const eId = re.lastInsertRowid

        for (let d = 0; d < 10 && delCount < 10000; d++) {
          const mo = String((d % 12) + 1).padStart(2, '0')
          const dy = String((d * 3 % 28) + 1).padStart(2, '0')
          insD.run(eId, cId, dTypes[d % dTypes.length], periods[d % periods.length],
            statuses[rand(statuses.length)], `2025-${mo}-${dy}`, assignees[rand(assignees.length)])
          delCount++
        }
      }
    }
  })()

  console.log(`Seeded: ${delCount.toLocaleString()} deliverables across 200 clients, 1,000 entities`)
}

seed()

// ── Middleware ────────────────────────────────────────────────────
app.use(express.json())
app.use(express.static(path.join(__dirname, 'public')))
app.get('/', (req, res) => res.redirect('/engagement.html'))

// ── API: Waterfall stream (SSE — N+1 in 3 visible phases) ────────
app.get('/api/waterfall-stream', async (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream')
  res.setHeader('Cache-Control', 'no-cache')
  res.setHeader('Connection', 'keep-alive')
  res.flushHeaders()

  const send = data => res.write(`data: ${JSON.stringify(data)}\n\n`)
  const start = Date.now()
  let qCount = 0

  // Phase 1 — clients (1 query)
  const clients = db.prepare('SELECT * FROM clients').all()
  qCount++
  send({ type: 'clients', data: clients, queryCount: qCount, elapsed: Date.now() - start })

  // Phase 2 — entities (N queries, CONCURRENCY at a time)
  const entityBatches = Math.ceil(clients.length / CONCURRENCY)
  await sleep(entityBatches * DEMO_CALL_MS)
  const entities = db.prepare('SELECT * FROM entities').all()
  qCount += clients.length
  send({ type: 'entities', data: entities, queryCount: qCount, entityBatches, concurrency: CONCURRENCY, elapsed: Date.now() - start })

  // Phase 3 — deliverables (N×M queries, CONCURRENCY at a time)
  const delivBatches = Math.ceil(entities.length / CONCURRENCY)
  await sleep(delivBatches * DEMO_CALL_MS)
  const deliverables = flatQuery.all()
  qCount += entities.length
  const prodEquivalentMs = (entityBatches + delivBatches) * 2000
  send({ type: 'deliverables', data: deliverables, queryCount: qCount, delivBatches, concurrency: CONCURRENCY, prodEquivalentMs, totalTime: Date.now() - start })

  res.end()
})

// ── Simulation constants ───────────────────────────────────────────
// Models production: 6 concurrent API calls, ~2000ms per call
// Demo runs at 50ms/batch (40× compressed) so it completes in ~10s
const sleep = ms => new Promise(r => setTimeout(r, ms))
const CONCURRENCY = 6
const DEMO_CALL_MS = 50

app.get('/api/waterfall', async (req, res) => {
  const start = Date.now()
  let queryCount = 0

  // Query 1: get all clients
  const clients = db.prepare('SELECT id FROM clients').all()
  queryCount++

  // N entity queries — batched at CONCURRENCY
  const entBatches = Math.ceil(clients.length / CONCURRENCY)
  for (let i = 0; i < entBatches; i++) {
    await sleep(DEMO_CALL_MS)
    const batch = clients.slice(i * CONCURRENCY, (i + 1) * CONCURRENCY)
    for (const c of batch) {
      db.prepare('SELECT id FROM entities WHERE client_id = ?').all(c.id)
      queryCount++
    }
  }

  // N×M deliverable queries — batched at CONCURRENCY
  const entities = db.prepare('SELECT id FROM entities').all()
  const delBatches = Math.ceil(entities.length / CONCURRENCY)
  for (let i = 0; i < delBatches; i++) {
    await sleep(DEMO_CALL_MS)
    const batch = entities.slice(i * CONCURRENCY, (i + 1) * CONCURRENCY)
    for (const e of batch) {
      db.prepare('SELECT id FROM deliverables WHERE entity_id = ?').all(e.id)
      queryCount++
    }
  }

  const prodEquivalentMs = (entBatches + delBatches) * 2000
  res.json({ queryCount, totalTime: Date.now() - start, rowCount: 10000, prodEquivalentMs })
})

// ── API: Flat single query ────────────────────────────────────────
const flatQuery = db.prepare(`
  SELECT
    d.id, d.type, d.period, d.status, d.due_date, d.assigned_to,
    e.id AS entity_id, e.name AS entity_name, e.entity_type,
    c.id AS client_id, c.name AS client_name, c.industry, c.region
  FROM deliverables d
  JOIN entities e ON d.entity_id = e.id
  JOIN clients c ON e.client_id = c.id
  ORDER BY c.name, e.name, d.type
`)

app.get('/api/flat', (req, res) => {
  const start = Date.now()
  const data = flatQuery.all()
  res.json({ queryCount: 1, totalTime: Date.now() - start, rowCount: data.length, data })
})

// ── WebSocket: push live status updates ───────────────────────────
const STATUSES = ['not_started', 'in_progress', 'review', 'filed', 'complete']
const maxId = db.prepare('SELECT MAX(id) as m FROM deliverables').get().m
const updateStmt = db.prepare('UPDATE deliverables SET status = ? WHERE id = ?')

setInterval(() => {
  const id = Math.floor(Math.random() * maxId) + 1
  const status = STATUSES[Math.floor(Math.random() * STATUSES.length)]
  updateStmt.run(status, id)

  const msg = JSON.stringify({ type: 'update', id, status, ts: Date.now() })
  wss.clients.forEach(ws => { if (ws.readyState === WebSocket.OPEN) ws.send(msg) })
}, 2000)

// ── Start ─────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3001
server.listen(PORT, () => {
  console.log(`Engagement POC → http://localhost:${PORT}/engagement.html`)
})
