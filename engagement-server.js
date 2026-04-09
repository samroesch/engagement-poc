const express = require('express')
const Database = require('better-sqlite3')
const WebSocket = require('ws')
const http = require('http')
const path = require('path')

const app = express()
const server = http.createServer(app)
const wss = new WebSocket.Server({ server })
const db = new Database(path.join(__dirname, 'engagement.db'))

// ── Schema migration ───────────────────────────────────────────────
// Drop legacy tables if the extended schema (v2) isn't present yet
const hasV2 = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='service_lines'").get()
if (!hasV2) {
  console.log('Schema v1 detected — dropping tables for v2 reseed')
  db.exec('DROP TABLE IF EXISTS deliverables; DROP TABLE IF EXISTS entities; DROP TABLE IF EXISTS clients;')
}

// ── Schema ────────────────────────────────────────────────────────
db.exec(`
  CREATE TABLE IF NOT EXISTS service_lines (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS staff (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    role TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS clients (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    name     TEXT NOT NULL,
    industry TEXT,
    region   TEXT
  );
  CREATE TABLE IF NOT EXISTS entities (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id   INTEGER NOT NULL,
    name        TEXT NOT NULL,
    entity_type TEXT
  );
  CREATE TABLE IF NOT EXISTS deliverables (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id            INTEGER NOT NULL,
    client_id            INTEGER NOT NULL,
    type                 TEXT NOT NULL,
    period               TEXT,
    status               TEXT DEFAULT 'not_started',
    due_date             TEXT,
    milestone_start_date TEXT,
    service_line_id      INTEGER NOT NULL,
    primary_staff_id     INTEGER NOT NULL,
    reviewer_id          INTEGER NOT NULL,
    complexity           TEXT DEFAULT 'medium',
    e_file_status        TEXT DEFAULT 'not_applicable',
    farmer               INTEGER DEFAULT 0,
    us_resident          INTEGER DEFAULT 0
  );
  CREATE INDEX IF NOT EXISTS idx_ent_client  ON entities(client_id);
  CREATE INDEX IF NOT EXISTS idx_del_entity  ON deliverables(entity_id);
  CREATE INDEX IF NOT EXISTS idx_del_client  ON deliverables(client_id);
  CREATE INDEX IF NOT EXISTS idx_del_sl      ON deliverables(service_line_id);
  CREATE INDEX IF NOT EXISTS idx_del_staff   ON deliverables(primary_staff_id);
`)

// ── Seed ──────────────────────────────────────────────────────────
function seed() {
  const existing = db.prepare('SELECT COUNT(*) as c FROM clients').get()
  if (existing.c > 0) {
    const total = db.prepare('SELECT COUNT(*) as c FROM deliverables').get().c
    console.log(`Database ready — ${total.toLocaleString()} deliverables`)
    return
  }

  console.log('Seeding database (v2 schema — 100,000 deliverables)…')

  // ── Reference data ──────────────────────────────────────────────
  const slNames = [
    'Corporate Tax', "SR&ED & Gov't Incentives", 'International Tax',
    'Transfer Pricing', 'Tax Controversy', 'Indirect Tax (GST/HST)',
    'Personal Tax', 'Payroll & Employment'
  ]
  const staffData = [
    ['Alex Chen',      'Partner'],          ['Jordan Smith',   'Senior Manager'],
    ['Morgan Lee',     'Senior Manager'],   ['Taylor Brown',   'Manager'],
    ['Casey Wilson',   'Manager'],          ['Riley Davis',    'Senior Associate'],
    ['Quinn Martinez', 'Senior Associate'], ['Avery Johnson',  'Associate'],
    ['Sam Taylor',     'Associate'],        ['Drew Parker',    'Associate'],
    ['Blake Rivera',   'Partner'],          ['Cameron Hughes', 'Senior Manager'],
    ['Dakota Foster',  'Manager'],          ['Emerson Gray',   'Manager'],
    ['Finley Brooks',  'Senior Associate'], ['Harley Cooper',  'Senior Associate'],
    ['Indigo Reed',    'Associate'],        ['Jules Powell',   'Associate'],
    ['Kennedy Bell',   'Senior Manager'],   ['Logan Howard',   'Associate']
  ]

  const insS  = db.prepare('INSERT INTO service_lines (name) VALUES (?)')
  const insSt = db.prepare('INSERT INTO staff (name, role) VALUES (?, ?)')

  db.transaction(() => {
    for (const name of slNames)           insS.run(name)
    for (const [name, role] of staffData) insSt.run(name, role)
  })()

  const SL_COUNT    = slNames.length     // 8
  const STAFF_COUNT = staffData.length   // 20

  // ── Client / entity / deliverable data ──────────────────────────
  const nameA = ['Apex','Summit','Atlas','Meridian','Pinnacle','Sterling','Vantage','Clearwater',
                 'Ridgeline','Horizon','Cascade','Ironwood','Northgate','Westfield','Lakewood',
                 'Stonehaven','Riverdale','Highpoint','Fairview','Glenmore']
  const nameB = ['Group','Holdings','Partners','Capital','Industries','Resources','Solutions',
                 'Ventures','International','Enterprises','Properties','Management','Associates',
                 'Systems','Technologies']
  const industries = ['Manufacturing','Real Estate','Technology','Healthcare','Financial Services',
                      'Retail','Energy','Construction','Mining','Agriculture']
  const regions    = ['Ontario','Quebec','British Columbia','Alberta','Manitoba',
                      'Nova Scotia','Saskatchewan','New Brunswick']
  const ePfx  = ['Alpha','Beta','Gamma','Delta','Epsilon','Zeta','Eta','Theta','Iota','Kappa']
  const eTypes = ['Operating Company','Holding Company','Trust','Limited Partnership','Subsidiary','Joint Venture']
  const eSfx  = ['Ltd.','Inc.','Corp.','LP','Trust','Holdings Inc.']
  const dTypes = ['Corporate Tax (T2)','GST/HST Return','Payroll Tax','Transfer Pricing',
                  'Trust Return (T3)','PST Return','SR&ED Claim','Personal Tax (T1)',
                  'Information Return (T5013)','Instalment Review']
  const periods      = ['FY 2022','FY 2023','FY 2024','Q1 2024','Q2 2024','Q3 2024','Q4 2024','Q1 2025','Q2 2025','Q3 2025']
  const statuses     = ['not_started','not_started','in_progress','in_progress','review','filed','complete','complete']
  const complexities = ['low','low','medium','medium','medium','high','critical']
  const eFileStats   = ['not_applicable','not_applicable','not_filed','pending','accepted','rejected']

  const insC = db.prepare('INSERT INTO clients (name, industry, region) VALUES (?, ?, ?)')
  const insE = db.prepare('INSERT INTO entities (client_id, name, entity_type) VALUES (?, ?, ?)')
  const insD = db.prepare(`
    INSERT INTO deliverables
      (entity_id, client_id, type, period, status, due_date, milestone_start_date,
       service_line_id, primary_staff_id, reviewer_id, complexity, e_file_status, farmer, us_resident)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `)

  let delCount = 0
  let rng = 42
  function rand(n) { rng = (rng * 1664525 + 1013904223) & 0xffffffff; return Math.abs(rng) % n }

  const TARGET            = 100000
  const CLIENTS           = 200
  const ENTITIES_PER_CLI  = 10
  const DELS_PER_ENTITY   = 50

  db.transaction(() => {
    for (let c = 0; c < CLIENTS && delCount < TARGET; c++) {
      const cName = `${nameA[c % nameA.length]} ${nameB[c % nameB.length]}${c >= nameA.length ? ` ${Math.floor(c / nameA.length) + 1}` : ''}`
      const rc = insC.run(cName, industries[c % industries.length], regions[c % regions.length])
      const cId = rc.lastInsertRowid

      for (let e = 0; e < ENTITIES_PER_CLI && delCount < TARGET; e++) {
        const eName = `${nameA[c % nameA.length]} ${ePfx[e]} ${eSfx[e % eSfx.length]}`
        const re = insE.run(cId, eName, eTypes[e % eTypes.length])
        const eId = re.lastInsertRowid

        for (let d = 0; d < DELS_PER_ENTITY && delCount < TARGET; d++) {
          const mo  = String((d % 12) + 1).padStart(2, '0')
          const dy  = String((d * 3 % 28) + 1).padStart(2, '0')
          const msd = String(((d * 5) % 28) + 1).padStart(2, '0')
          const primaryStaff = rand(STAFF_COUNT) + 1
          let reviewer = rand(STAFF_COUNT) + 1
          if (reviewer === primaryStaff) reviewer = (reviewer % STAFF_COUNT) + 1

          insD.run(
            eId, cId,
            dTypes[d % dTypes.length],
            periods[d % periods.length],
            statuses[rand(statuses.length)],
            `2025-${mo}-${dy}`,
            `2025-${mo}-${msd}`,
            rand(SL_COUNT) + 1,
            primaryStaff,
            reviewer,
            complexities[rand(complexities.length)],
            eFileStats[rand(eFileStats.length)],
            rand(5) === 0 ? 1 : 0,   // ~20% farmer
            rand(4) === 0 ? 1 : 0    // ~25% US resident
          )
          delCount++
        }
      }
    }
  })()

  console.log(`Seeded: ${delCount.toLocaleString()} deliverables · ${CLIENTS} clients · ${CLIENTS * ENTITIES_PER_CLI} entities`)
  console.log(`Reference: ${SL_COUNT} service lines · ${STAFF_COUNT} staff`)
}

seed()

// ── Middleware ────────────────────────────────────────────────────
app.use(express.json())
app.use(express.static(path.join(__dirname, 'public')))
app.get('/', (req, res) => res.redirect('/engagement.html'))

// ── Simulation constants ───────────────────────────────────────────
// Models production: 6 concurrent API calls, ~2000ms per call
// Demo runs at 30ms/batch (~67× compressed) → ~11s total
const sleep = ms => new Promise(r => setTimeout(r, ms))
const CONCURRENCY  = 6
const DEMO_CALL_MS = 30

// ── API: Waterfall stream (SSE — N+1 in 3 visible phases) ─────────
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

  // Phase 2 — entities (1 query per client, batched at CONCURRENCY)
  const entityBatches = Math.ceil(clients.length / CONCURRENCY)
  await sleep(entityBatches * DEMO_CALL_MS)
  const entities = db.prepare('SELECT * FROM entities').all()
  qCount += clients.length
  send({ type: 'entities', data: entities, queryCount: qCount, entityBatches, concurrency: CONCURRENCY, elapsed: Date.now() - start })

  // Phase 3 — deliverables (1 query per entity, batched at CONCURRENCY)
  const delivBatches = Math.ceil(entities.length / CONCURRENCY)
  await sleep(delivBatches * DEMO_CALL_MS)
  const deliverables = flatQuery.all()
  qCount += entities.length
  const prodEquivalentMs = (entityBatches + delivBatches) * 2000
  send({ type: 'deliverables', data: deliverables, queryCount: qCount, delivBatches, concurrency: CONCURRENCY, prodEquivalentMs, totalTime: Date.now() - start })

  res.end()
})

// ── API: Waterfall (stats only — batched concurrency simulation) ───
app.get('/api/waterfall', async (req, res) => {
  const start = Date.now()
  let queryCount = 0

  const clients = db.prepare('SELECT id FROM clients').all()
  queryCount++

  const entBatches = Math.ceil(clients.length / CONCURRENCY)
  for (let i = 0; i < entBatches; i++) {
    await sleep(DEMO_CALL_MS)
    const batch = clients.slice(i * CONCURRENCY, (i + 1) * CONCURRENCY)
    for (const c of batch) {
      db.prepare('SELECT id FROM entities WHERE client_id = ?').all(c.id)
      queryCount++
    }
  }

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
  res.json({ queryCount, totalTime: Date.now() - start, rowCount: 100000, prodEquivalentMs })
})

// ── API: Flat query (1 query, 5 JOINs) ───────────────────────────
const flatQuery = db.prepare(`
  SELECT
    d.id,                    d.type,              d.period,
    d.status,                d.due_date,          d.milestone_start_date,
    d.complexity,            d.e_file_status,
    d.farmer,                d.us_resident,
    e.id   AS entity_id,     e.name AS entity_name,   e.entity_type,
    c.id   AS client_id,     c.name AS client_name,   c.industry,    c.region,
    sl.name AS service_line,
    ps.name AS assigned_to,  ps.role AS assigned_role,
    rv.name AS reviewer,     rv.role AS reviewer_role
  FROM deliverables  d
  JOIN entities      e  ON d.entity_id        = e.id
  JOIN clients       c  ON e.client_id        = c.id
  JOIN service_lines sl ON d.service_line_id  = sl.id
  JOIN staff         ps ON d.primary_staff_id = ps.id
  JOIN staff         rv ON d.reviewer_id      = rv.id
  ORDER BY c.name, e.name, d.type
`)

app.get('/api/flat', (req, res) => {
  const start = Date.now()
  const data = flatQuery.all()
  res.json({ queryCount: 1, totalTime: Date.now() - start, rowCount: data.length, data })
})

// ── API: Flat stream (SSE — same 1 query, rows piped in batches) ──
// DB cursor reads rows as they're ready; client renders first batch
// in ~100ms instead of waiting for full 100k-row JSON transfer.
const STREAM_BATCH = 5000
app.get('/api/flat-stream', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream')
  res.setHeader('Cache-Control', 'no-cache')
  res.setHeader('Connection', 'keep-alive')
  res.flushHeaders()

  const start = Date.now()
  let rowCount = 0, batch = []

  for (const row of flatQuery.iterate()) {
    batch.push(row)
    rowCount++
    if (batch.length >= STREAM_BATCH) {
      res.write(`data: ${JSON.stringify({ rows: batch, rowCount })}\n\n`)
      batch = []
    }
  }

  res.write(`data: ${JSON.stringify({ rows: batch, rowCount, done: true, totalTime: Date.now() - start })}\n\n`)
  res.end()
})

// ── WebSocket: push live status updates ───────────────────────────
const STATUSES  = ['not_started', 'in_progress', 'review', 'filed', 'complete']
const maxId     = db.prepare('SELECT MAX(id) as m FROM deliverables').get().m
const updateStmt = db.prepare('UPDATE deliverables SET status = ? WHERE id = ?')

setInterval(() => {
  const id     = Math.floor(Math.random() * maxId) + 1
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
