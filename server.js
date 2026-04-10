require('dotenv').config()
const express = require('express')
const http    = require('http')
const path    = require('path')
const { Pool, Client } = require('pg')

// ── Keep process alive through any error ─────────────────────────
process.on('uncaughtException',   err => console.error('[uncaughtException]',   err.message))
process.on('unhandledRejection',  err => console.error('[unhandledRejection]',  err?.message ?? err))

// ── App ───────────────────────────────────────────────────────────
const app    = express()
const server = http.createServer(app)
app.use(express.static(path.join(__dirname, 'public')))
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')))

const PORT = process.env.PORT || 3001
server.listen(PORT, () => console.log(`Server → http://localhost:${PORT}`))

// ── PG pool (for API requests) ────────────────────────────────────
const pool = new Pool({ connectionString: process.env.DATABASE_URL })
pool.on('error', err => console.error('[pool error]', err.message))

// ── Schema ────────────────────────────────────────────────────────
const SCHEMA = `
CREATE TABLE IF NOT EXISTS meta (
  key   VARCHAR(50) PRIMARY KEY,
  value TEXT
);
CREATE TABLE IF NOT EXISTS engagement (
  id                                  SERIAL PRIMARY KEY,
  engagement_name                     VARCHAR(255),
  client_id                           INT,
  entity_id                           INT,
  entity_name                         VARCHAR(255),
  tax_year                            INT,
  service_type                        VARCHAR(100),
  status_id                           INT,
  due_date                            DATE,
  delivery_date                       DATE,
  tax_year_end                        DATE,
  complexity                          VARCHAR(50),
  created_by                          INT,
  updated_by                          INT,
  created_datetime                    TIMESTAMPTZ,
  updated_datetime                    TIMESTAMPTZ,
  is_active                           BOOLEAN,
  ibs_engagement_code                 VARCHAR(100),
  caseware_filename                   VARCHAR(255),
  estimated_hours                     DECIMAL(10,2),
  taxprep_filename                    VARCHAR(255),
  san                                 VARCHAR(100),
  san_activation_date                 DATE,
  san_expiry_date                     DATE,
  client_delivery                     VARCHAR(100),
  letter_expiry_date                  DATE,
  restrict_access                     BOOLEAN,
  is_automatically_send_questionnaire BOOLEAN,
  is_high_level_reviewer_required     BOOLEAN,
  is_activated                        BOOLEAN,
  office                              VARCHAR(100),
  send_to_crc                         BOOLEAN,
  crc_duedate                         DATE,
  is_office_of_partner                BOOLEAN,
  dms_py_site_location                VARCHAR(500),
  first_name                          VARCHAR(100),
  last_name                           VARCHAR(100),
  client_name                         VARCHAR(255),
  ifirm_url                           VARCHAR(500),
  client_code                         VARCHAR(50),
  tax_return_processing               VARCHAR(100),
  allow_client_service_assessment     BOOLEAN,
  is_deliverable                      BOOLEAN,
  filing_type                         VARCHAR(50),
  preferred_name                      VARCHAR(100),
  country                             VARCHAR(100),
  description                         TEXT,
  entity_type                         VARCHAR(100),
  is_extension                        BOOLEAN,
  client_milestone                    VARCHAR(100),
  internal_milestone                  VARCHAR(100),
  is_retention_policy                 BOOLEAN,
  milestone_completed_date            DATE,
  milestone_start_date                DATE,
  engagement_hold                     BOOLEAN,
  is_amended                          BOOLEAN,
  is_exception_approved               BOOLEAN,
  is_form_missing                     BOOLEAN,
  efilers                             TEXT,
  hl_reviewers                        TEXT,
  company_users                       TEXT,
  partners                            TEXT,
  preparers                           TEXT,
  processors                          TEXT,
  reviewers                           TEXT,
  service_teams                       TEXT,
  govt_delivery_method                VARCHAR(100),
  parent_engagement_id                INT,
  partner_names                       TEXT,
  preparer_names                      TEXT,
  processor_names                     TEXT,
  reviewer_names                      TEXT,
  company_user_names                  TEXT,
  hl_reviewer_names                   TEXT,
  efiler_names                        TEXT,
  has_ifirm_url                       BOOLEAN,
  efile_auto_failed_reason            TEXT,
  engagement_government_delivery_status VARCHAR(100),
  print_auto_success                  BOOLEAN,
  send_to_client_auto_success         BOOLEAN,
  contact_guid_id                     UUID,
  is_deceased                         BOOLEAN,
  is_farmer                           BOOLEAN,
  is_head_of_household                BOOLEAN,
  is_paper_doc                        BOOLEAN,
  extension_duedate                   DATE,
  is_eng_name_same_as_entity_name     BOOLEAN,
  legal_entity_name                   VARCHAR(255),
  is_milestone_email_notification_sent BOOLEAN,
  is_roll_forwarded                   BOOLEAN,
  san_updated_datetime                TIMESTAMPTZ,
  is_gen_ai_enabled                   BOOLEAN,
  is_lgd                              BOOLEAN,
  is_lgd_linked                       BOOLEAN,
  is_lgd_frontload                    BOOLEAN,
  is_incognito                        BOOLEAN,
  is_deliverable_na                   BOOLEAN,
  slip_volume                         INT,
  is_prep_automated                   BOOLEAN,
  consent_7216                        BOOLEAN,
  sin_itn                             VARCHAR(50),
  ssn_itin                            VARCHAR(50),
  is_automation_intake                BOOLEAN,
  lead_partners                       TEXT,
  is_in_rollforward_queue             BOOLEAN,
  is_bulk_milestone_moving            BOOLEAN,
  business_number                     VARCHAR(50),
  has_tax_dashboard                   BOOLEAN
);
ALTER TABLE engagement ADD COLUMN IF NOT EXISTS entity_name VARCHAR(255);

-- Single-column indexes: equality filters and basic sorts
CREATE INDEX IF NOT EXISTS idx_eng_client_id   ON engagement(client_id);
CREATE INDEX IF NOT EXISTS idx_eng_entity_id   ON engagement(entity_id);
CREATE INDEX IF NOT EXISTS idx_eng_entity_name ON engagement(entity_name);
CREATE INDEX IF NOT EXISTS idx_eng_status_id   ON engagement(status_id);
CREATE INDEX IF NOT EXISTS idx_eng_office      ON engagement(office);
CREATE INDEX IF NOT EXISTS idx_eng_tax_year    ON engagement(tax_year);
CREATE INDEX IF NOT EXISTS idx_eng_svc_type    ON engagement(service_type);
CREATE INDEX IF NOT EXISTS idx_eng_filing_type ON engagement(filing_type);
CREATE INDEX IF NOT EXISTS idx_eng_is_active   ON engagement(is_active);
CREATE INDEX IF NOT EXISTS idx_eng_msd         ON engagement(milestone_start_date);

-- Composite indexes: cover the cursor pagination tiebreak (sort_col, id)
-- so the keyset cursor condition (sort_col, id) > (val, id) is a pure index scan
CREATE INDEX IF NOT EXISTS idx_eng_due_date_id ON engagement(due_date NULLS LAST, id);

-- Composite indexes: filter column leading, sort column trailing
-- These allow PostgreSQL to satisfy WHERE filter + ORDER BY sort with one index scan
CREATE INDEX IF NOT EXISTS idx_eng_status_due   ON engagement(status_id,   due_date NULLS LAST, id);
CREATE INDEX IF NOT EXISTS idx_eng_office_due   ON engagement(office,      due_date NULLS LAST, id);
CREATE INDEX IF NOT EXISTS idx_eng_svctype_due  ON engagement(service_type,due_date NULLS LAST, id);
CREATE INDEX IF NOT EXISTS idx_eng_taxyear_due  ON engagement(tax_year,    due_date NULLS LAST, id);
CREATE INDEX IF NOT EXISTS idx_eng_filing_due   ON engagement(filing_type, due_date NULLS LAST, id);
`

// ── Seed data helpers ─────────────────────────────────────────────
const OFFICES = [
  'Toronto','Vancouver','Calgary','Montreal','Ottawa','Edmonton','Halifax',
  'Winnipeg','Regina','Victoria','Saskatoon','London','Kitchener','Mississauga',
  'Brampton','Hamilton','Windsor','Oakville','Burlington','Barrie','Sudbury',
  'Thunder Bay','Kingston','Fredericton','Moncton','Saint John','Charlottetown',
  "St. John's",'Yellowknife','Whitehorse','Iqaluit','Kelowna','Kamloops',
  'Nanaimo','Abbotsford','Surrey','Burnaby','Richmond','Langley','Coquitlam',
  'Delta','Lethbridge','Red Deer','Medicine Hat','Fort McMurray','Grande Prairie',
  'Lloydminster','Prince Albert','Moose Jaw','Brandon'
]
const SVC_TYPES    = ['T1','T2','T3','T4','GST','HST','PST','Corporate','Trust','Partnership']
const COMPLEXITIES = ['Low','Medium','High','Very High']
const FILING_TYPES = ['Electronic','Paper','Both']
const COUNTRIES    = ['Canada','Canada','Canada','Canada','Canada','Canada','Canada','Canada','USA','UK']
const ENTITY_TYPES = ['Individual','Corporation','Trust','Partnership','Estate']
const MILESTONES   = ['Intake','In Progress','Manager Review','Partner Review','Filed','Complete',null,null]
const DELIVERY_M   = ['Electronic','Paper','Both',null]
const CLIENT_DEL   = ['Email','Portal','Mail',null]
const FIRST_NAMES  = ['James','Mary','Robert','Patricia','John','Jennifer','Michael','Linda',
  'William','Barbara','David','Elizabeth','Richard','Susan','Joseph','Jessica',
  'Thomas','Sarah','Charles','Karen','Christopher','Lisa','Daniel','Nancy',
  'Matthew','Betty','Anthony','Margaret','Mark','Sandra','Donald','Ashley',
  'Steven','Dorothy','Paul','Kimberly','Andrew','Emily','Kenneth','Donna',
  'Joshua','Michelle','Kevin','Carol','Brian','Amanda','George','Melissa','Timothy','Deborah']
const LAST_NAMES   = ['Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis',
  'Rodriguez','Martinez','Hernandez','Lopez','Gonzalez','Wilson','Anderson',
  'Thomas','Taylor','Moore','Jackson','Martin','Lee','Perez','Thompson',
  'White','Harris','Sanchez','Clark','Ramirez','Lewis','Robinson',
  'Walker','Young','Allen','King','Wright','Scott','Torres','Nguyen',
  'Hill','Flores','Green','Adams','Nelson','Baker','Hall','Rivera',
  'Campbell','Mitchell','Carter','Roberts']
const STAFF_NAMES  = ['John Smith','Jane Doe','Robert Chen','Sarah Lee','Michael Brown',
  'Emily Davis','David Wilson','Lisa Anderson','James Taylor','Amy Martin']

// Simple seeded LCG — deterministic, no Math.random()
let rng = 123456789
function rand(n)           { rng = (rng * 1664525 + 1013904223) & 0x7fffffff; return rng % n }
function pick(arr)         { return arr[rand(arr.length)] }
function bool(truePct)     { return rand(100) < truePct }
function maybe(v, nullPct) { return rand(100) < (nullPct ?? 30) ? null : v }
function randDate(s, e)    {
  const ms = new Date(s).getTime(), me = new Date(e).getTime()
  return new Date(ms + rand(Math.floor((me - ms) / 86400000)) * 86400000).toISOString().slice(0, 10)
}
function addDays(d, n) {
  if (!d) return null
  const dt = new Date(d); dt.setDate(dt.getDate() + n); return dt.toISOString().slice(0, 10)
}
function email(name) { return name.replace(' ', '.').toLowerCase() + '@firm.com' }
function jsonArr(name) { return `["${email(name)}"]` }

function makeEntityName(entityType, firstName, lastName) {
  switch (entityType) {
    case 'Individual':
      return `${firstName} ${lastName}`
    case 'Corporation':
      return `${lastName} ${pick(['Holdings','Group','Enterprises','Industries','Solutions','Capital','Partners'])} Inc.`
    case 'Trust':
      return rand(2) === 0
        ? `${lastName} Family Trust`
        : `${firstName} ${lastName} Trust`
    case 'Partnership':
      return `${lastName} & ${pick(LAST_NAMES)} ${pick(['LLP','LP','Partnership'])}`
    case 'Estate':
      return `Estate of ${firstName} ${lastName}`
    default:
      return `${firstName} ${lastName}`
  }
}

function makeRow() {
  const clientId   = rand(200) + 1
  const entityId   = rand(1000) + 1
  const taxYear    = 2021 + rand(5)
  const office     = pick(OFFICES)
  const svcType    = pick(SVC_TYPES)
  const statusId   = rand(12) + 1
  const complexity = pick(COMPLEXITIES)
  const filingType = pick(FILING_TYPES)
  const country    = pick(COUNTRIES)
  const entityType = pick(ENTITY_TYPES)
  const firstName  = pick(FIRST_NAMES)
  const lastName   = pick(LAST_NAMES)
  const clientName = `Client Corp ${clientId}`
  const entityName = makeEntityName(entityType, firstName, lastName)
  const dueDate    = randDate('2023-01-01', '2026-12-31')
  const msd        = maybe(addDays(dueDate, -(rand(60) + 1)))
  const delivDate  = maybe(addDays(dueDate, rand(30)))
  const sanAct     = maybe(randDate('2022-01-01', '2024-12-31'))
  const created    = randDate('2022-01-01', '2024-12-31') + 'T00:00:00Z'
  const p1 = pick(STAFF_NAMES), p2 = pick(STAFF_NAMES)

  return [
    `${clientName} ${svcType} ${taxYear}`,   // engagement_name
    clientId, entityId,
    entityName,                               // entity_name
    taxYear, svcType, statusId,
    dueDate,
    maybe(delivDate),                         // delivery_date
    `${taxYear}-12-31`,                       // tax_year_end
    complexity,
    rand(50) + 1, rand(50) + 1,              // created_by, updated_by
    created,
    addDays(created.slice(0,10), rand(180)) + 'T00:00:00Z', // updated_datetime
    bool(90),                                 // is_active
    maybe(`ENG-${clientId}-${entityId}-${taxYear}`),
    maybe(`CW_${clientId}_${entityId}.ac`),
    +(1 + rand(199) + rand(100) * 0.01).toFixed(2),
    maybe(`TP_${clientId}_${entityId}.tax`),
    maybe(`SAN-${String(rand(99999)).padStart(5,'0')}`),
    maybe(sanAct),
    maybe(sanAct ? addDays(sanAct, 365) : null),
    maybe(pick(CLIENT_DEL)),
    maybe(addDays(dueDate, 90)),
    bool(5), bool(40), bool(30), bool(80),   // restrict, auto_q, hlr, activated
    office,
    bool(20),                                 // send_to_crc
    maybe(bool(20) ? addDays(dueDate, -5) : null, 70),
    bool(60),                                 // is_office_of_partner
    maybe(`//dms/client/${clientId}/${entityId}`),
    firstName, lastName, clientName,
    maybe(`https://ifirm.ca/client/${clientId}`),
    `C${String(clientId).padStart(5,'0')}`,
    maybe(pick(['Standard','Rush','Extended',null]), 20),
    bool(70), bool(85),                       // allow_csa, is_deliverable
    filingType, maybe(firstName), country,
    maybe(`Tax return for ${taxYear} ${svcType}`),
    entityType, bool(10),
    maybe(pick(MILESTONES)), maybe(pick(MILESTONES)),
    bool(50),                                 // is_retention_policy
    maybe(addDays(dueDate, rand(30))),
    msd,
    bool(5), bool(5), bool(10), bool(8),     // hold, amended, exception, form_missing
    maybe(jsonArr(pick(STAFF_NAMES)), 60),   // efilers
    maybe(jsonArr(pick(STAFF_NAMES)), 60),   // hl_reviewers
    maybe(jsonArr(pick(STAFF_NAMES)), 60),   // company_users
    maybe(jsonArr(pick(STAFF_NAMES)), 60),   // partners
    maybe(jsonArr(pick(STAFF_NAMES)), 60),   // preparers
    maybe(jsonArr(pick(STAFF_NAMES)), 60),   // processors
    maybe(jsonArr(pick(STAFF_NAMES)), 60),   // reviewers
    maybe(`["service_team_${rand(10)}"]`, 60),
    maybe(pick(DELIVERY_M)),
    null,                                     // parent_engagement_id
    maybe(`${p1}, ${p2}`),                   // partner_names
    maybe(pick(STAFF_NAMES)),                // preparer_names
    maybe(pick(STAFF_NAMES)),                // processor_names
    maybe(pick(STAFF_NAMES)),                // reviewer_names
    maybe(pick(STAFF_NAMES)),                // company_user_names
    maybe(pick(STAFF_NAMES)),                // hl_reviewer_names
    maybe(email(pick(STAFF_NAMES))),         // efiler_names
    bool(60),                                 // has_ifirm_url
    null,                                     // efile_auto_failed_reason
    maybe(pick(['Accepted','Rejected','Pending','Not Filed',null])),
    bool(70), bool(65),                       // print_auto, send_to_client_auto
    null,                                     // contact_guid_id
    bool(2), bool(15), bool(10), bool(5),    // deceased, farmer, hoh, paper_doc
    maybe(bool(10) ? addDays(dueDate, 60) : null, 0),
    bool(80),                                 // is_eng_name_same
    maybe(`${firstName} ${lastName} ${entityType}`),
    bool(40), bool(30),                       // milestone_email, roll_forwarded
    null,                                     // san_updated_datetime
    bool(20), bool(10), bool(8), bool(5),    // gen_ai, lgd, lgd_linked, lgd_frontload
    bool(3), bool(5),                         // incognito, deliverable_na
    maybe(rand(500) + 1, 50),               // slip_volume
    bool(20),                                 // is_prep_automated
    maybe(bool(60)),                          // consent_7216
    maybe(String(rand(899999999) + 100000000)),
    maybe(String(rand(899999999) + 100000000)),
    bool(15),                                 // is_automation_intake
    maybe(`${p1}, ${p2}`),                   // lead_partners
    bool(5), bool(3),                         // in_rollforward_queue, bulk_milestone_moving
    maybe(`BN${rand(899999999) + 100000000}`),
    bool(40)                                  // has_tax_dashboard
  ]
}

// ── Seed ──────────────────────────────────────────────────────────
const COLS = [
  'engagement_name','client_id','entity_id','entity_name','tax_year','service_type','status_id',
  'due_date','delivery_date','tax_year_end','complexity','created_by','updated_by',
  'created_datetime','updated_datetime','is_active','ibs_engagement_code',
  'caseware_filename','estimated_hours','taxprep_filename','san',
  'san_activation_date','san_expiry_date','client_delivery','letter_expiry_date',
  'restrict_access','is_automatically_send_questionnaire','is_high_level_reviewer_required',
  'is_activated','office','send_to_crc','crc_duedate','is_office_of_partner',
  'dms_py_site_location','first_name','last_name','client_name','ifirm_url',
  'client_code','tax_return_processing','allow_client_service_assessment','is_deliverable',
  'filing_type','preferred_name','country','description','entity_type','is_extension',
  'client_milestone','internal_milestone','is_retention_policy','milestone_completed_date',
  'milestone_start_date','engagement_hold','is_amended','is_exception_approved',
  'is_form_missing','efilers','hl_reviewers','company_users','partners','preparers',
  'processors','reviewers','service_teams','govt_delivery_method','parent_engagement_id',
  'partner_names','preparer_names','processor_names','reviewer_names',
  'company_user_names','hl_reviewer_names','efiler_names','has_ifirm_url',
  'efile_auto_failed_reason','engagement_government_delivery_status',
  'print_auto_success','send_to_client_auto_success','contact_guid_id',
  'is_deceased','is_farmer','is_head_of_household','is_paper_doc','extension_duedate',
  'is_eng_name_same_as_entity_name','legal_entity_name',
  'is_milestone_email_notification_sent','is_roll_forwarded','san_updated_datetime',
  'is_gen_ai_enabled','is_lgd','is_lgd_linked','is_lgd_frontload','is_incognito',
  'is_deliverable_na','slip_volume','is_prep_automated','consent_7216',
  'sin_itn','ssn_itin','is_automation_intake','lead_partners',
  'is_in_rollforward_queue','is_bulk_milestone_moving','business_number','has_tax_dashboard'
]
const NUM_COLS   = COLS.length
const MAX_BATCH  = Math.floor(65535 / NUM_COLS)  // PostgreSQL hard limit: 65535 params per query
const TARGET     = parseInt(process.env.SEED_TARGET) || 100000
const BATCH_SIZE = Math.min(parseInt(process.env.SEED_BATCH_SIZE) || 200, MAX_BATCH)

async function seed() {
  // Use a dedicated client so pool errors don't interfere
  const client = new Client({ connectionString: process.env.DATABASE_URL })
  await client.connect()
  try {
    await client.query(SCHEMA)

    // Skip re-seed if already seeded to (at least) the configured target
    const ver = await client.query("SELECT value FROM meta WHERE key='seed_version'")
    if (ver.rows[0]?.value === 'v2') {
      const cnt = await client.query('SELECT COUNT(*) AS c FROM engagement')
      const existing = Number(cnt.rows[0].c)
      console.log(`DB ready — ${existing.toLocaleString()} engagements`)
      if (existing >= TARGET) return
      console.log(`Target is ${TARGET.toLocaleString()} but only ${existing.toLocaleString()} rows exist — reseeding…`)
    }

    console.log('Truncating any partial data…')
    await client.query('TRUNCATE TABLE engagement RESTART IDENTITY')
    await client.query("DELETE FROM meta WHERE key='seed_version'")

    // Reduce WAL pressure: skip fsync per commit (data still safe on checkpoint)
    await client.query('SET synchronous_commit = OFF')

    console.log(`Seeding ${TARGET.toLocaleString()} engagement rows…`)
    let inserted = 0
    const CHECKPOINT_INTERVAL = 50000  // flush WAL every 50k rows to avoid disk fill
    while (inserted < TARGET) {
      const size = Math.min(BATCH_SIZE, TARGET - inserted)
      const rows = Array.from({ length: size }, makeRow)
      const placeholders = rows.map((_, ri) =>
        '(' + Array.from({ length: NUM_COLS }, (_, ci) => `$${ri * NUM_COLS + ci + 1}`).join(',') + ')'
      ).join(',')
      await client.query(
        `INSERT INTO engagement (${COLS.join(',')}) VALUES ${placeholders}`,
        rows.flat()
      )
      inserted += size
      if (inserted % 10000 === 0 || inserted === TARGET)
        console.log(`  ${inserted.toLocaleString()} / ${TARGET.toLocaleString()}`)
      // Force checkpoint to flush WAL and reclaim disk space
      if (inserted % CHECKPOINT_INTERVAL === 0) {
        process.stdout.write('  [checkpoint] ')
        await client.query('CHECKPOINT')
        console.log('done')
      }
    }

    await client.query("INSERT INTO meta VALUES ('seed_version','v2') ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value")
    console.log('Seed complete.')
  } finally {
    await client.end()
  }
}

async function seedWithRetry(attempts = 30, delay = 5000) {
  try {
    await seed()
  } catch (err) {
    if (attempts > 0) {
      console.log(`DB not ready (${err.message?.slice(0, 70)}) — retry in ${delay/1000}s (${attempts} left)`)
      setTimeout(() => seedWithRetry(attempts - 1, delay), delay)
    } else {
      console.error('Seed failed after all retries:', err.message)
    }
  }
}
seedWithRetry()

// ── Column whitelist ──────────────────────────────────────────────
const VALID_COLS = new Set(COLS.concat(['id']))

const SORT_COLS = {
  due_date: 'due_date', milestone_start_date: 'milestone_start_date',
  engagement_name: 'engagement_name', client_name: 'client_name',
  office: 'office', tax_year: 'tax_year', service_type: 'service_type',
  status_id: 'status_id', complexity: 'complexity', estimated_hours: 'estimated_hours'
}

// ── API: count ────────────────────────────────────────────────────
app.get('/api/engagements/count', async (req, res) => {
  try {
    const r = await pool.query('SELECT COUNT(*) AS c FROM engagement')
    res.json({ count: Number(r.rows[0].c) })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ── API: stream (SSE, cursor pagination) ─────────────────────────
app.get('/api/engagements/stream', async (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream')
  res.setHeader('Cache-Control', 'no-cache')
  res.setHeader('Connection', 'keep-alive')

  function send(obj) { res.write(`data: ${JSON.stringify(obj)}\n\n`) }

  const requestedCols = (req.query.columns || '')
    .split(',').map(c => c.trim()).filter(c => VALID_COLS.has(c))
  const selectCols = ['id', ...requestedCols.filter(c => c !== 'id')]
  const selectSQL  = selectCols.join(', ')

  const sortKey = SORT_COLS[req.query.sort]
  const sortDir = req.query.dir === 'desc' ? 'DESC' : 'ASC'
  const limit   = Math.min(parseInt(req.query.limit) || 1000, 2000)

  // Decode cursor
  let cursorVal = null, cursorId = 0
  if (req.query.after) {
    try {
      const parsed = JSON.parse(Buffer.from(req.query.after, 'base64').toString())
      cursorId  = parsed.id  ?? 0
      cursorVal = parsed.val ?? null
    } catch (_) {}
  }

  // Build filter conditions and params
  const filterConditions = []
  const filterParams = []

  if (req.query.office) {
    filterParams.push(req.query.office)
    filterConditions.push(`office = $${filterParams.length}`)
  }
  if (req.query.tax_year) {
    filterParams.push(req.query.tax_year)
    filterConditions.push(`tax_year = $${filterParams.length}`)
  }
  if (req.query.service_type) {
    filterParams.push(req.query.service_type)
    filterConditions.push(`service_type = $${filterParams.length}`)
  }
  if (req.query.status_id) {
    filterParams.push(req.query.status_id)
    filterConditions.push(`status_id = $${filterParams.length}`)
  }
  if (req.query.complexity) {
    filterParams.push(req.query.complexity)
    filterConditions.push(`complexity = $${filterParams.length}`)
  }
  if (req.query.filing_type) {
    filterParams.push(req.query.filing_type)
    filterConditions.push(`filing_type = $${filterParams.length}`)
  }
  if (req.query.entity_type) {
    filterParams.push(req.query.entity_type)
    filterConditions.push(`entity_type = $${filterParams.length}`)
  }
  if (req.query.is_active !== undefined && req.query.is_active !== '') {
    filterParams.push(req.query.is_active === 'true')
    filterConditions.push(`is_active = $${filterParams.length}`)
  }

  const filterPrefix = filterConditions.length > 0
    ? filterConditions.join(' AND ') + ' AND '
    : ''

  const client = new Client({ connectionString: process.env.DATABASE_URL })
  try {
    await client.connect()

    let sql, params
    const fLen = filterParams.length
    const hasCursor = req.query.after != null
    const filterWhere = filterConditions.length > 0 ? filterConditions.join(' AND ') : null

    if (!sortKey) {
      // Default: sort by id
      params = [...filterParams, cursorId, limit + 1]
      const where = [filterWhere, `id > $${fLen + 1}`].filter(Boolean).join(' AND ')
      sql = `SELECT ${selectSQL} FROM engagement WHERE ${where} ORDER BY id ASC LIMIT $${fLen + 2}`
    } else if (!hasCursor) {
      // First page — no cursor condition, just filters + ORDER BY
      params = [...filterParams, limit + 1]
      const where = filterWhere ? `WHERE ${filterWhere}` : ''
      sql = `SELECT ${selectSQL} FROM engagement ${where} ORDER BY ${sortKey} ${sortDir} NULLS LAST, id ${sortDir} LIMIT $${fLen + 1}`
    } else if (sortDir === 'ASC') {
      params = [...filterParams, cursorVal, cursorId, limit + 1]
      const cursorCond = `(${sortKey} > $${fLen + 1} OR (${sortKey} = $${fLen + 1} AND id > $${fLen + 2}) OR (${sortKey} IS NULL AND id > $${fLen + 2}))`
      const where = [filterWhere, cursorCond].filter(Boolean).join(' AND ')
      sql = `SELECT ${selectSQL} FROM engagement WHERE ${where} ORDER BY ${sortKey} ASC NULLS LAST, id ASC LIMIT $${fLen + 3}`
    } else {
      params = [...filterParams, cursorVal, cursorId, limit + 1]
      const cursorCond = `(${sortKey} < $${fLen + 1} OR (${sortKey} = $${fLen + 1} AND id < $${fLen + 2}))`
      const where = [filterWhere, cursorCond].filter(Boolean).join(' AND ')
      sql = `SELECT ${selectSQL} FROM engagement WHERE ${where} ORDER BY ${sortKey} DESC NULLS LAST, id DESC LIMIT $${fLen + 3}`
    }

    const result = await client.query(sql, params)
    const hasMore = result.rows.length > limit
    const rows    = hasMore ? result.rows.slice(0, limit) : result.rows

    let nextCursor = null
    if (hasMore && rows.length > 0) {
      const last = rows[rows.length - 1]
      nextCursor = Buffer.from(JSON.stringify({ id: last.id, val: sortKey ? last[sortKey] : last.id })).toString('base64')
    }

    send({ rows, hasMore, cursor: nextCursor })
    if (!hasMore) send({ rows: [], hasMore: false, cursor: null })
  } catch (err) {
    console.error('[stream error]', err.message)
    send({ error: err.message })
  } finally {
    await client.end().catch(() => {})
    res.end()
  }
})
