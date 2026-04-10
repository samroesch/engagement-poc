require('dotenv').config()
const express = require('express')
const Database = require('better-sqlite3')
const WebSocket = require('ws')
const http = require('http')
const path = require('path')
const { Pool } = require('pg')
const Cursor = require('pg-cursor')

const app = express()
const server = http.createServer(app)
const wss = new WebSocket.Server({ server })

let db
try {
  db = new Database(path.join(__dirname, 'engagement.db'))
} catch (e) {
  console.warn('better-sqlite3 binary not found — SQLite routes disabled. Run `npm install` with Python + node-gyp to restore.')
  db = null
}

// ── SQLite schema + routes (disabled if binary missing) ───────────
if (db) {

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
  CREATE INDEX IF NOT EXISTS idx_del_due     ON deliverables(due_date);
  CREATE INDEX IF NOT EXISTS idx_del_msd     ON deliverables(milestone_start_date);
  CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
`)

// ── Seed ──────────────────────────────────────────────────────────
function seed() {
  // v3: dates vary per entity so rollup sort visibly reorders groups
  const seedVer = db.prepare("SELECT value FROM meta WHERE key='seed_version'").get()
  if (seedVer?.value === 'v3') {
    const total = db.prepare('SELECT COUNT(*) as c FROM deliverables').get().c
    console.log(`Database ready — ${total.toLocaleString()} deliverables`)
    return
  }
  const clearMsg = seedVer ? `Reseeding (was ${seedVer.value} → v3)` : 'Seeding database (v3)'
  console.log(`${clearMsg} — 100,000 deliverables with varied date distribution…`)
  db.exec('DELETE FROM deliverables; DELETE FROM entities; DELETE FROM clients; DELETE FROM service_lines; DELETE FROM staff')

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
          // Each client lands in a distinct month band (Jan–Dec cycling every 12 clients).
          // Each entity within a client is staggered by 2 days so entity-level sort
          // visibly reorders rows. Milestone date is a few days before due date.
          const clientBand   = c % 12                         // 0–11 → month 1–12
          const entityDayBase= e * 2                          // 0,2,4…18 per entity
          const dueDayNum    = (entityDayBase + d % 10) % 28 + 1
          const mo  = String(clientBand + 1).padStart(2, '0')
          const dy  = String(dueDayNum).padStart(2, '0')
          const msd = String(Math.max(1, dueDayNum - (e % 5 + 1))).padStart(2, '0')
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

  db.prepare("INSERT OR REPLACE INTO meta (key, value) VALUES ('seed_version', 'v3')").run()
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
  const deliverables = buildFlatQuery().all()
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

// ── Flat query builder (dynamic ORDER BY) ────────────────────────
// Prepared statements can't vary ORDER BY, so we build on demand.
// SQLite prepares in <1ms so this is fine per-request.
const SORT_COLS = {
  due_date:             'd.due_date',
  milestone_start_date: 'd.milestone_start_date',
  client_name:          'c.name',
  service_line:         'sl.name',
  assigned_to:          'ps.name',
  period:               'd.period',
  type:                 'd.type',
  complexity:           'd.complexity',
  status:               'd.status',
}
const FLAT_SELECT = `
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
`
function buildFlatQuery(sort, dir) {
  const col    = SORT_COLS[sort]
  const dirSql = dir === 'desc' ? 'DESC' : 'ASC'
  // Default: group-friendly sort. Custom: global sort + id tiebreaker for stability.
  const order  = col ? `${col} ${dirSql}, d.id` : 'c.name, e.name, d.type'
  return db.prepare(FLAT_SELECT + ` ORDER BY ${order}`)
}

app.get('/api/flat', (req, res) => {
  const start = Date.now()
  const data  = buildFlatQuery(req.query.sort, req.query.dir).all()
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
  const stmt = buildFlatQuery(req.query.sort, req.query.dir)

  for (const row of stmt.iterate()) {
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

} // end if (db)

// ── PostgreSQL pool ───────────────────────────────────────────────
const pgPool = new Pool({ connectionString: process.env.DATABASE_URL })
pgPool.on('error', err => console.error('PG pool error (non-fatal):', err.message))
process.on('unhandledRejection', err => console.error('Unhandled rejection (non-fatal):', err?.message || err))
process.on('uncaughtException', err => console.error('Uncaught exception (non-fatal):', err?.message || err))

// ── PG schema + seed ──────────────────────────────────────────────
const PG_SCHEMA = `
CREATE TABLE IF NOT EXISTS meta (
  key VARCHAR(50) PRIMARY KEY,
  value TEXT
);

CREATE TABLE IF NOT EXISTS engagement (
  id SERIAL PRIMARY KEY,
  engagement_name VARCHAR(255),
  client_id INT,
  entity_id INT,
  tax_year INT,
  service_type VARCHAR(100),
  status_id INT,
  due_date DATE,
  delivery_date DATE,
  tax_year_end DATE,
  complexity VARCHAR(50),
  created_by INT,
  updated_by INT,
  created_datetime TIMESTAMPTZ,
  updated_datetime TIMESTAMPTZ,
  is_active BOOLEAN,
  ibs_engagement_code VARCHAR(100),
  caseware_filename VARCHAR(255),
  estimated_hours DECIMAL(10,2),
  taxprep_filename VARCHAR(255),
  san VARCHAR(100),
  san_activation_date DATE,
  san_expiry_date DATE,
  client_delivery VARCHAR(100),
  letter_expiry_date DATE,
  restrict_access BOOLEAN,
  is_automatically_send_questionnaire BOOLEAN,
  is_high_level_reviewer_required BOOLEAN,
  is_activated BOOLEAN,
  office VARCHAR(100),
  send_to_crc BOOLEAN,
  crc_duedate DATE,
  is_office_of_partner BOOLEAN,
  dms_py_site_location VARCHAR(500),
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  client_name VARCHAR(255),
  ifirm_url VARCHAR(500),
  client_code VARCHAR(50),
  tax_return_processing VARCHAR(100),
  allow_client_service_assessment BOOLEAN,
  is_deliverable BOOLEAN,
  filing_type VARCHAR(50),
  preferred_name VARCHAR(100),
  country VARCHAR(100),
  description TEXT,
  entity_type VARCHAR(100),
  is_extension BOOLEAN,
  client_milestone VARCHAR(100),
  internal_milestone VARCHAR(100),
  is_retention_policy BOOLEAN,
  milestone_completed_date DATE,
  milestone_start_date DATE,
  engagement_hold BOOLEAN,
  is_amended BOOLEAN,
  is_exception_approved BOOLEAN,
  is_form_missing BOOLEAN,
  efilers TEXT,
  hl_reviewers TEXT,
  company_users TEXT,
  partners TEXT,
  preparers TEXT,
  processors TEXT,
  reviewers TEXT,
  service_teams TEXT,
  govt_delivery_method VARCHAR(100),
  parent_engagement_id INT,
  partner_names TEXT,
  preparer_names TEXT,
  processor_names TEXT,
  reviewer_names TEXT,
  company_user_names TEXT,
  hl_reviewer_names TEXT,
  efiler_names TEXT,
  has_ifirm_url BOOLEAN,
  efile_auto_failed_reason TEXT,
  engagement_government_delivery_status VARCHAR(100),
  print_auto_success BOOLEAN,
  send_to_client_auto_success BOOLEAN,
  contact_guid_id UUID,
  is_deceased BOOLEAN,
  is_farmer BOOLEAN,
  is_head_of_household BOOLEAN,
  is_paper_doc BOOLEAN,
  extension_duedate DATE,
  is_eng_name_same_as_entity_name BOOLEAN,
  legal_entity_name VARCHAR(255),
  is_milestone_email_notification_sent BOOLEAN,
  is_roll_forwarded BOOLEAN,
  san_updated_datetime TIMESTAMPTZ,
  is_gen_ai_enabled BOOLEAN,
  is_lgd BOOLEAN,
  is_lgd_linked BOOLEAN,
  is_lgd_frontload BOOLEAN,
  is_incognito BOOLEAN,
  is_deliverable_na BOOLEAN,
  slip_volume INT,
  is_prep_automated BOOLEAN,
  consent_7216 BOOLEAN,
  sin_itn VARCHAR(50),
  ssn_itin VARCHAR(50),
  is_automation_intake BOOLEAN,
  lead_partners TEXT,
  is_in_rollforward_queue BOOLEAN,
  is_bulk_milestone_moving BOOLEAN,
  business_number VARCHAR(50),
  has_tax_dashboard BOOLEAN
);

CREATE INDEX IF NOT EXISTS idx_eng_client_id ON engagement(client_id);
CREATE INDEX IF NOT EXISTS idx_eng_entity_id ON engagement(entity_id);
CREATE INDEX IF NOT EXISTS idx_eng_due_date ON engagement(due_date);
CREATE INDEX IF NOT EXISTS idx_eng_status_id ON engagement(status_id);
CREATE INDEX IF NOT EXISTS idx_eng_office ON engagement(office);
CREATE INDEX IF NOT EXISTS idx_eng_tax_year ON engagement(tax_year);
CREATE INDEX IF NOT EXISTS idx_eng_service_type ON engagement(service_type);
CREATE INDEX IF NOT EXISTS idx_eng_is_active ON engagement(is_active);
CREATE INDEX IF NOT EXISTS idx_eng_milestone_start ON engagement(milestone_start_date);
`

async function pgSeedIfNeeded() {
  const client = await pgPool.connect()
  try {
    await client.query(PG_SCHEMA)

    const versionRow = await client.query("SELECT value FROM meta WHERE key='seed_version'")
    if (versionRow.rows[0]?.value === 'v2') {
      const countRow = await client.query('SELECT COUNT(*) AS c FROM engagement')
      console.log(`PG database ready — ${parseInt(countRow.rows[0].c).toLocaleString()} engagements`)
      return
    }

    console.log('PG: Clearing any partial data and seeding 100,000 engagement rows…')
    await client.query('TRUNCATE TABLE engagement RESTART IDENTITY')
    await client.query("DELETE FROM meta WHERE key='seed_version'")

    // Reference data pools
    const offices = [
      'Toronto', 'Vancouver', 'Calgary', 'Montreal', 'Ottawa', 'Edmonton',
      'Halifax', 'Winnipeg', 'Regina', 'Victoria', 'Saskatoon', 'London',
      'Kitchener', 'Mississauga', 'Brampton', 'Hamilton', 'Windsor',
      'Oakville', 'Burlington', 'Barrie', 'Sudbury', 'Thunder Bay',
      'Kingston', 'Fredericton', 'Moncton', 'Saint John', 'Charlottetown',
      'St. John\'s', 'Yellowknife', 'Whitehorse', 'Iqaluit',
      'Kelowna', 'Kamloops', 'Nanaimo', 'Abbotsford', 'Surrey',
      'Burnaby', 'Richmond', 'Langley', 'Coquitlam', 'Delta',
      'Lethbridge', 'Red Deer', 'Medicine Hat', 'Fort McMurray',
      'Grande Prairie', 'Lloydminster', 'Prince Albert', 'Moose Jaw',
      'Brandon', 'Portage la Prairie'
    ]
    const serviceTypes = ['T1', 'T2', 'T3', 'T4', 'GST', 'HST', 'PST', 'Corporate', 'Trust', 'Partnership']
    const complexities = ['Low', 'Medium', 'High', 'Very High']
    const filingTypes = ['Electronic', 'Paper', 'Both']
    const countries = ['Canada', 'Canada', 'Canada', 'Canada', 'Canada', 'Canada', 'Canada', 'Canada', 'USA', 'UK']
    const entityTypes = ['Individual', 'Corporation', 'Trust', 'Partnership', 'Estate']
    const milestones = ['Intake', 'In Progress', 'Manager Review', 'Partner Review', 'Filed', 'Complete', null, null]
    const deliveryMethods = ['Electronic', 'Paper', 'Both', null]
    const clientDeliveries = ['Email', 'Portal', 'Mail', null]
    const firstNames = [
      'James', 'Mary', 'Robert', 'Patricia', 'John', 'Jennifer', 'Michael', 'Linda',
      'William', 'Barbara', 'David', 'Elizabeth', 'Richard', 'Susan', 'Joseph',
      'Jessica', 'Thomas', 'Sarah', 'Charles', 'Karen', 'Christopher', 'Lisa',
      'Daniel', 'Nancy', 'Matthew', 'Betty', 'Anthony', 'Margaret', 'Mark', 'Sandra',
      'Donald', 'Ashley', 'Steven', 'Dorothy', 'Paul', 'Kimberly', 'Andrew', 'Emily',
      'Kenneth', 'Donna', 'Joshua', 'Michelle', 'Kevin', 'Carol', 'Brian', 'Amanda',
      'George', 'Melissa', 'Timothy', 'Deborah'
    ]
    const lastNames = [
      'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
      'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson',
      'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Perez', 'Thompson',
      'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson',
      'Walker', 'Young', 'Allen', 'King', 'Wright', 'Scott', 'Torres', 'Nguyen',
      'Hill', 'Flores', 'Green', 'Adams', 'Nelson', 'Baker', 'Hall', 'Rivera',
      'Campbell', 'Mitchell', 'Carter', 'Roberts'
    ]
    const staffNames = [
      'John Smith', 'Jane Doe', 'Robert Chen', 'Sarah Lee', 'Michael Brown',
      'Emily Davis', 'David Wilson', 'Lisa Anderson', 'James Taylor', 'Amy Martin'
    ]

    // Seeded LCG RNG
    let rng = 123456789
    function rand(n) { rng = (rng * 1664525 + 1013904223) & 0x7fffffff; return rng % n }
    function randBool(truePct) { return rand(100) < truePct }
    function pick(arr) { return arr[rand(arr.length)] }
    function maybeNull(val, nullPct = 30) { return rand(100) < nullPct ? null : val }

    // Date helpers
    function randomDate(start, end) {
      const s = new Date(start).getTime(), e = new Date(end).getTime()
      return new Date(s + rand(Math.floor((e - s) / 86400000)) * 86400000).toISOString().slice(0, 10)
    }
    function addDays(dateStr, days) {
      if (!dateStr) return null
      const d = new Date(dateStr); d.setDate(d.getDate() + days)
      return d.toISOString().slice(0, 10)
    }

    const TARGET = 100000
    const BATCH_SIZE = 250
    let inserted = 0

    while (inserted < TARGET) {
      const batchRows = []
      const batchSize = Math.min(BATCH_SIZE, TARGET - inserted)
      for (let i = 0; i < batchSize; i++) {
        const clientId = rand(200) + 1
        const entityId = rand(1000) + 1
        const taxYear = 2021 + rand(5)
        const office = pick(offices)
        const serviceType = pick(serviceTypes)
        const statusId = rand(12) + 1
        const complexity = pick(complexities)
        const filingType = pick(filingTypes)
        const country = pick(countries)
        const entityType = pick(entityTypes)
        const firstName = pick(firstNames)
        const lastName = pick(lastNames)
        const clientName = `Client Corp ${clientId}`
        const dueDate = randomDate('2023-01-01', '2026-12-31')
        const milestoneStartDate = maybeNull(addDays(dueDate, -rand(60) - 1))
        const deliveryDate = maybeNull(addDays(dueDate, rand(30)))
        const taxYearEnd = `${taxYear}-12-31`
        const sanActivationDate = maybeNull(randomDate('2022-01-01', '2024-12-31'))
        const sanExpiryDate = sanActivationDate ? addDays(sanActivationDate, 365) : null
        const createdDatetime = randomDate('2022-01-01', '2024-12-31') + 'T00:00:00Z'
        const updatedDatetime = addDays(createdDatetime.slice(0, 10), rand(180)) + 'T00:00:00Z'
        const p1 = pick(staffNames), p2 = pick(staffNames)
        const partnerNames = maybeNull(`${p1}, ${p2}`)
        const preparerNames = maybeNull(pick(staffNames))
        const reviewerNames = maybeNull(`${pick(staffNames)}`)
        const hl = maybeNull(`["${pick(staffNames).replace(' ', '.').toLowerCase()}@firm.com"]`, 60)
        const efilers = maybeNull(`["${pick(staffNames).replace(' ', '.').toLowerCase()}@firm.com"]`, 60)
        const engagementName = `${clientName} ${serviceType} ${taxYear}`

        batchRows.push([
          engagementName, clientId, entityId, taxYear, serviceType, statusId,
          dueDate, maybeNull(deliveryDate), taxYearEnd, complexity,
          rand(50) + 1, rand(50) + 1,
          createdDatetime, updatedDatetime,
          randBool(90), // is_active
          maybeNull(`ENG-${clientId}-${entityId}-${taxYear}`), // ibs_engagement_code
          maybeNull(`CW_${clientId}_${entityId}.ac`), // caseware_filename
          (1 + rand(199) + rand(100) * 0.01).toFixed(2), // estimated_hours
          maybeNull(`TP_${clientId}_${entityId}.tax`), // taxprep_filename
          maybeNull(`SAN-${rand(99999).toString().padStart(5, '0')}`), // san
          maybeNull(sanActivationDate), // san_activation_date
          maybeNull(sanExpiryDate), // san_expiry_date
          maybeNull(pick(clientDeliveries)), // client_delivery
          maybeNull(addDays(dueDate, 90)), // letter_expiry_date
          randBool(5), // restrict_access
          randBool(40), // is_automatically_send_questionnaire
          randBool(30), // is_high_level_reviewer_required
          randBool(80), // is_activated
          office,
          randBool(20), // send_to_crc
          maybeNull(randBool(20) ? addDays(dueDate, -5) : null, 70), // crc_duedate
          randBool(60), // is_office_of_partner
          maybeNull(`//dms/client/${clientId}/${entityId}`), // dms_py_site_location
          firstName, lastName, clientName,
          maybeNull(`https://ifirm.ca/client/${clientId}`), // ifirm_url
          `C${clientId.toString().padStart(5, '0')}`, // client_code
          maybeNull(pick(['Standard', 'Rush', 'Extended', null]), 20), // tax_return_processing
          randBool(70), // allow_client_service_assessment
          randBool(85), // is_deliverable
          filingType, maybeNull(firstName), country,
          maybeNull(`Tax return for ${taxYear} ${serviceType}`), // description
          entityType,
          randBool(10), // is_extension
          maybeNull(pick(milestones)), // client_milestone
          maybeNull(pick(milestones)), // internal_milestone
          randBool(50), // is_retention_policy
          maybeNull(addDays(dueDate, rand(30))), // milestone_completed_date
          milestoneStartDate,
          randBool(5), // engagement_hold
          randBool(5), // is_amended
          randBool(10), // is_exception_approved
          randBool(8), // is_form_missing
          efilers,
          hl, // hl_reviewers
          maybeNull(`["user${rand(50)}@firm.com"]`, 60), // company_users
          maybeNull(`["${pick(staffNames).replace(' ', '.').toLowerCase()}@firm.com"]`, 60), // partners
          maybeNull(`["${pick(staffNames).replace(' ', '.').toLowerCase()}@firm.com"]`, 60), // preparers
          maybeNull(`["${pick(staffNames).replace(' ', '.').toLowerCase()}@firm.com"]`, 60), // processors
          maybeNull(`["${pick(staffNames).replace(' ', '.').toLowerCase()}@firm.com"]`, 60), // reviewers
          maybeNull(`["service_team_${rand(10)}"]`, 60), // service_teams
          maybeNull(pick(deliveryMethods)), // govt_delivery_method
          maybeNull(rand(100) < 20 ? rand(inserted + 1) + 1 : null, 0), // parent_engagement_id
          partnerNames,
          preparerNames,
          maybeNull(pick(staffNames)), // processor_names
          reviewerNames,
          maybeNull(pick(staffNames)), // company_user_names
          maybeNull(pick(staffNames)), // hl_reviewer_names
          maybeNull(`${pick(firstNames).toLowerCase()}.${pick(lastNames).toLowerCase()}@firm.com`), // efiler_names
          randBool(60), // has_ifirm_url
          maybeNull(rand(100) < 3 ? 'Efile transmission failed' : null, 0), // efile_auto_failed_reason
          maybeNull(pick(['Accepted', 'Rejected', 'Pending', 'Not Filed', null])), // engagement_government_delivery_status
          randBool(70), // print_auto_success
          randBool(65), // send_to_client_auto_success
          null, // contact_guid_id (UUID - leave null for simplicity)
          randBool(2), // is_deceased
          randBool(15), // is_farmer
          randBool(10), // is_head_of_household
          randBool(5), // is_paper_doc
          maybeNull(randBool(10) ? addDays(dueDate, 60) : null, 0), // extension_duedate
          randBool(80), // is_eng_name_same_as_entity_name
          maybeNull(`${firstName} ${lastName} ${entityType}`), // legal_entity_name
          randBool(40), // is_milestone_email_notification_sent
          randBool(30), // is_roll_forwarded
          null, // san_updated_datetime
          randBool(20), // is_gen_ai_enabled
          randBool(10), // is_lgd
          randBool(8), // is_lgd_linked
          randBool(5), // is_lgd_frontload
          randBool(3), // is_incognito
          randBool(5), // is_deliverable_na
          maybeNull(rand(100) < 50 ? rand(500) + 1 : null, 0), // slip_volume
          randBool(20), // is_prep_automated
          maybeNull(randBool(60) ? true : false, 20), // consent_7216
          maybeNull(`${rand(899999999) + 100000000}`), // sin_itn
          maybeNull(`${rand(899999999) + 100000000}`), // ssn_itin
          randBool(15), // is_automation_intake
          maybeNull(partnerNames), // lead_partners
          randBool(5), // is_in_rollforward_queue
          randBool(3), // is_bulk_milestone_moving
          maybeNull(`BN${rand(899999999) + 100000000}`), // business_number
          randBool(40) // has_tax_dashboard
        ])
      }

      // Build parameterized INSERT for this batch
      const cols = [
        'engagement_name', 'client_id', 'entity_id', 'tax_year', 'service_type', 'status_id',
        'due_date', 'delivery_date', 'tax_year_end', 'complexity', 'created_by', 'updated_by',
        'created_datetime', 'updated_datetime', 'is_active', 'ibs_engagement_code',
        'caseware_filename', 'estimated_hours', 'taxprep_filename', 'san',
        'san_activation_date', 'san_expiry_date', 'client_delivery', 'letter_expiry_date',
        'restrict_access', 'is_automatically_send_questionnaire', 'is_high_level_reviewer_required',
        'is_activated', 'office', 'send_to_crc', 'crc_duedate', 'is_office_of_partner',
        'dms_py_site_location', 'first_name', 'last_name', 'client_name', 'ifirm_url',
        'client_code', 'tax_return_processing', 'allow_client_service_assessment', 'is_deliverable',
        'filing_type', 'preferred_name', 'country', 'description', 'entity_type', 'is_extension',
        'client_milestone', 'internal_milestone', 'is_retention_policy', 'milestone_completed_date',
        'milestone_start_date', 'engagement_hold', 'is_amended', 'is_exception_approved',
        'is_form_missing', 'efilers', 'hl_reviewers', 'company_users', 'partners', 'preparers',
        'processors', 'reviewers', 'service_teams', 'govt_delivery_method', 'parent_engagement_id',
        'partner_names', 'preparer_names', 'processor_names', 'reviewer_names',
        'company_user_names', 'hl_reviewer_names', 'efiler_names', 'has_ifirm_url',
        'efile_auto_failed_reason', 'engagement_government_delivery_status',
        'print_auto_success', 'send_to_client_auto_success', 'contact_guid_id',
        'is_deceased', 'is_farmer', 'is_head_of_household', 'is_paper_doc', 'extension_duedate',
        'is_eng_name_same_as_entity_name', 'legal_entity_name',
        'is_milestone_email_notification_sent', 'is_roll_forwarded', 'san_updated_datetime',
        'is_gen_ai_enabled', 'is_lgd', 'is_lgd_linked', 'is_lgd_frontload', 'is_incognito',
        'is_deliverable_na', 'slip_volume', 'is_prep_automated', 'consent_7216',
        'sin_itn', 'ssn_itin', 'is_automation_intake', 'lead_partners',
        'is_in_rollforward_queue', 'is_bulk_milestone_moving', 'business_number', 'has_tax_dashboard'
      ]
      const numCols = cols.length
      const valuePlaceholders = batchRows.map((_, ri) =>
        '(' + Array.from({ length: numCols }, (_, ci) => `$${ri * numCols + ci + 1}`).join(', ') + ')'
      ).join(', ')
      const flatValues = batchRows.flat()
      const sql = `INSERT INTO engagement (${cols.join(', ')}) VALUES ${valuePlaceholders}`
      await client.query(sql, flatValues)

      inserted += batchRows.length
      if (inserted % 10000 === 0 || inserted === TARGET) {
        console.log(`PG seed progress: ${inserted.toLocaleString()} / ${TARGET.toLocaleString()} rows`)
      }
    }

    await client.query("INSERT INTO meta (key, value) VALUES ('seed_version', 'v2') ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value")
    console.log(`PG seed complete — ${TARGET.toLocaleString()} engagement rows inserted`)
  } finally {
    client.release()
  }
}

// Run PG seed with retry — waits for DB to finish recovering before seeding
async function pgSeedWithRetry(attemptsLeft = 30, delayMs = 5000) {
  try {
    await pgSeedIfNeeded()
  } catch (err) {
    if (attemptsLeft > 0) {
      console.log(`PG not ready (${err.message?.slice(0, 80) ?? err}) — retrying in ${delayMs / 1000}s… (${attemptsLeft} left)`)
      setTimeout(() => pgSeedWithRetry(attemptsLeft - 1, delayMs), delayMs)
    } else {
      console.error('PG seed failed after all retries:', err.message)
    }
  }
}
pgSeedWithRetry()


// ── PG API: valid columns whitelist ──────────────────────────────
const VALID_COLUMNS = new Set([
  'id', 'engagement_name', 'client_id', 'entity_id', 'tax_year', 'service_type', 'status_id',
  'due_date', 'delivery_date', 'tax_year_end', 'complexity', 'created_by', 'updated_by',
  'created_datetime', 'updated_datetime', 'is_active', 'ibs_engagement_code',
  'caseware_filename', 'estimated_hours', 'taxprep_filename', 'san',
  'san_activation_date', 'san_expiry_date', 'client_delivery', 'letter_expiry_date',
  'restrict_access', 'is_automatically_send_questionnaire', 'is_high_level_reviewer_required',
  'is_activated', 'office', 'send_to_crc', 'crc_duedate', 'is_office_of_partner',
  'dms_py_site_location', 'first_name', 'last_name', 'client_name', 'ifirm_url',
  'client_code', 'tax_return_processing', 'allow_client_service_assessment', 'is_deliverable',
  'filing_type', 'preferred_name', 'country', 'description', 'entity_type', 'is_extension',
  'client_milestone', 'internal_milestone', 'is_retention_policy', 'milestone_completed_date',
  'milestone_start_date', 'engagement_hold', 'is_amended', 'is_exception_approved',
  'is_form_missing', 'efilers', 'hl_reviewers', 'company_users', 'partners', 'preparers',
  'processors', 'reviewers', 'service_teams', 'govt_delivery_method', 'parent_engagement_id',
  'partner_names', 'preparer_names', 'processor_names', 'reviewer_names',
  'company_user_names', 'hl_reviewer_names', 'efiler_names', 'has_ifirm_url',
  'efile_auto_failed_reason', 'engagement_government_delivery_status',
  'print_auto_success', 'send_to_client_auto_success', 'contact_guid_id',
  'is_deceased', 'is_farmer', 'is_head_of_household', 'is_paper_doc', 'extension_duedate',
  'is_eng_name_same_as_entity_name', 'legal_entity_name',
  'is_milestone_email_notification_sent', 'is_roll_forwarded', 'san_updated_datetime',
  'is_gen_ai_enabled', 'is_lgd', 'is_lgd_linked', 'is_lgd_frontload', 'is_incognito',
  'is_deliverable_na', 'slip_volume', 'is_prep_automated', 'consent_7216',
  'sin_itn', 'ssn_itin', 'is_automation_intake', 'lead_partners',
  'is_in_rollforward_queue', 'is_bulk_milestone_moving', 'business_number', 'has_tax_dashboard'
])

const PG_SORT_COLS = {
  due_date: 'due_date',
  milestone_start_date: 'milestone_start_date',
  engagement_name: 'engagement_name',
  client_name: 'client_name',
  office: 'office',
  tax_year: 'tax_year',
  service_type: 'service_type',
  status_id: 'status_id',
  complexity: 'complexity',
  estimated_hours: 'estimated_hours',
}

// ── PG API: count ─────────────────────────────────────────────────
app.get('/api/pg/engagements/count', async (req, res) => {
  try {
    const result = await pgPool.query('SELECT COUNT(*) AS count FROM engagement')
    res.json({ count: parseInt(result.rows[0].count) })
  } catch (err) {
    console.error('PG count error:', err)
    res.status(500).json({ error: err.message })
  }
})

// ── PG API: stream (SSE, cursor-based pagination) ─────────────────
app.get('/api/pg/engagements/stream', async (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream')
  res.setHeader('Cache-Control', 'no-cache')
  res.setHeader('Connection', 'keep-alive')
  res.flushHeaders()

  const send = data => res.write(`data: ${JSON.stringify(data)}\n\n`)

  // Parse and validate columns
  const rawCols = req.query.columns ? req.query.columns.split(',').map(c => c.trim()).filter(Boolean) : []
  const selectedCols = rawCols.filter(c => VALID_COLUMNS.has(c))
  // Always include id
  const colSet = new Set(['id', ...selectedCols])
  const colList = [...colSet].join(', ')

  // Sort
  const sortKey = req.query.sort && PG_SORT_COLS[req.query.sort] ? req.query.sort : null
  const sortCol = sortKey ? PG_SORT_COLS[sortKey] : null
  const dir = req.query.dir === 'desc' ? 'desc' : 'asc'
  const dirOp = dir === 'asc' ? '>' : '<'
  const dirSql = dir === 'asc' ? 'ASC' : 'DESC'

  // Limit
  const limit = Math.min(parseInt(req.query.limit) || 1000, 2000)

  // Parse cursor
  let afterId = 0
  let afterVal = null
  if (req.query.after) {
    try {
      const decoded = JSON.parse(Buffer.from(req.query.after, 'base64').toString('utf8'))
      afterId = decoded.id || 0
      afterVal = decoded.val !== undefined ? decoded.val : null
    } catch (_) {
      // ignore bad cursor
    }
  }

  const pgClient = await pgPool.connect()
  try {
    let sql, params
    if (!sortCol) {
      // Default: sort by id
      sql = `SELECT ${colList} FROM engagement WHERE id > $1 ORDER BY id ASC LIMIT $2`
      params = [afterId, limit + 1]
    } else {
      if (afterVal === null && afterId === 0) {
        // First page
        sql = `SELECT ${colList} FROM engagement ORDER BY ${sortCol} ${dirSql}, id ${dirSql} LIMIT $1`
        params = [limit + 1]
      } else {
        // Cursor continuation using row value comparison
        sql = `SELECT ${colList} FROM engagement WHERE (${sortCol}, id) ${dirOp} ($1, $2) ORDER BY ${sortCol} ${dirSql}, id ${dirSql} LIMIT $3`
        params = [afterVal, afterId, limit + 1]
      }
    }

    const cursor = pgClient.query(new Cursor(sql, params))
    const rows = await new Promise((resolve, reject) => {
      cursor.read(limit + 1, (err, rows) => {
        if (err) reject(err); else resolve(rows)
      })
    })
    cursor.close(() => {})

    const hasMore = rows.length > limit
    const pageRows = hasMore ? rows.slice(0, limit) : rows

    let nextCursor = null
    if (hasMore && pageRows.length > 0) {
      const last = pageRows[pageRows.length - 1]
      const cursorObj = { id: last.id, val: sortCol ? last[sortCol] : last.id }
      nextCursor = Buffer.from(JSON.stringify(cursorObj)).toString('base64')
    }

    send({ rows: pageRows, hasMore, cursor: nextCursor, total: null })
    if (!hasMore) {
      send({ rows: [], hasMore: false, cursor: null })
    }
  } catch (err) {
    console.error('PG stream error:', err)
    send({ error: err.message })
  } finally {
    pgClient.release()
    res.end()
  }
})

// ── Start ─────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3001
server.listen(PORT, () => {
  console.log(`Engagement POC → http://localhost:${PORT}/engagement.html`)
})
