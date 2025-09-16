// server/index.js
require('dotenv').config();
const express = require('express');
const multer = require('multer');
const csv = require('csv-parser');
const XLSX = require('xlsx');
const fs = require('fs');
const path = require('path');
const nodemailer = require('nodemailer');
const cors = require('cors');

const app = express();
app.use(express.json());
app.use(cors());

// ---------- Paths ----------
const ROOT = __dirname;
const UPLOAD_DIR = path.join(ROOT, 'uploads');
const LOG_DIR = path.join(ROOT, 'logs');
const ERROR_LOG = path.join(LOG_DIR, 'error.log');
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });
if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR, { recursive: true });

// ---------- Simple logger helpers ----------
function timestamp() {
  return new Date().toISOString();
}
function log(...args) {
  console.log(timestamp(), '-', ...args);
}
function logErr(...args) {
  console.error(timestamp(), '-', ...args);
}
function appendErrorLog(entry) {
  const line = `[${timestamp()}] ${entry}\n\n`;
  fs.appendFile(ERROR_LOG, line, (err) => {
    if (err) console.error('Failed to append to error log:', err);
  });
}
function logError(err, context = '') {
  try {
    const message = (err && err.stack) ? err.stack : String(err);
    logErr(context, '\n', message);
    appendErrorLog(`${context}\n${message}`);
  } catch (e) {
    console.error('Error while logging error:', e);
  }
}

// ---------- Request logging middleware (lightweight) ----------
app.use((req, res, next) => {
  log(`${req.method} ${req.originalUrl} from ${req.ip}`);
  next();
});

// Static frontend (if present)
app.use(express.static(path.join(ROOT, 'public')));

// ---------- Config ----------
const PORT = process.env.PORT ? Number(process.env.PORT) : 7000;
const allowedExt = ['.csv', '.xls', '.xlsx', '.txt'];

// ---------- Multer ----------
const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, UPLOAD_DIR),
  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname) || '';
    const name = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}${ext}`;
    cb(null, name);
  },
});
const upload = multer({
  storage,
  limits: { fileSize: 10 * 1024 * 1024 }, // 10MB
  fileFilter: (req, file, cb) => {
    const ext = path.extname(file.originalname).toLowerCase();
    if (!allowedExt.includes(ext)) {
      return cb(new Error(`Only ${allowedExt.join(', ')} files are allowed`));
    }
    cb(null, true);
  },
});

// ---------- Nodemailer ----------
const transporter = nodemailer.createTransport({
  service: 'gmail',
  auth: {
    user: process.env.EMAIL_USER,
    pass: process.env.EMAIL_PASS,
  },
  pool: true,
  connectionTimeout: 30 * 1000,
});

// Verify transporter at startup and log result
transporter.verify()
  .then(() => log('Mailer verification: OK'))
  .catch((err) => {
    logErr('Mailer verification failed:', err && err.message ? err.message : err);
    appendErrorLog(`Mailer verify failed:\n${err && err.stack ? err.stack : String(err)}`);
  });

// ---------- Parsers ----------
function parseCsvFile(filepath) {
  return new Promise((resolve, reject) => {
    const emails = [];
    fs.createReadStream(filepath)
      .pipe(csv())
      .on('data', (row) => {
        try {
          const keys = Object.keys(row || {});
          const emailKey = keys.find((k) => k && k.toLowerCase() === 'email');
          if (emailKey && row[emailKey]) {
            emails.push(String(row[emailKey]).trim());
          } else {
            for (const k of keys) {
              const v = String(row[k] || '').trim();
              if (v && /.+@.+\..+/.test(v)) {
                emails.push(v);
                break;
              }
            }
          }
        } catch (e) {
          // row-level parsing error — log small context but continue
          logErr('Row parse error:', e.message || e);
        }
      })
      .on('end', () => resolve(emails))
      .on('error', (err) => reject(err));
  });
}

function parseExcelFile(filepath) {
  return new Promise((resolve, reject) => {
    try {
      const workbook = XLSX.readFile(filepath);
      const sheetName = workbook.SheetNames[0];
      if (!sheetName) return resolve([]);
      const worksheet = workbook.Sheets[sheetName];
      const json = XLSX.utils.sheet_to_json(worksheet, { defval: '' });
      const emails = [];
      if (!json.length) return resolve(emails);

      const headerKeys = Object.keys(json[0] || {});
      const emailKey = headerKeys.find((k) => k && k.toLowerCase() === 'email');

      if (emailKey) {
        for (const row of json) {
          const v = String(row[emailKey] || '').trim();
          if (v) emails.push(v);
        }
        return resolve(emails);
      }

      for (const row of json) {
        for (const k of Object.keys(row || {})) {
          const v = String(row[k] || '').trim();
          if (v && /.+@.+\..+/.test(v)) {
            emails.push(v);
            break;
          }
        }
      }
      resolve(emails);
    } catch (err) {
      reject(err);
    }
  });
}

function parseTextFile(filepath) {
  return new Promise((resolve, reject) => {
    fs.readFile(filepath, 'utf8', (err, raw) => {
      if (err) return reject(err);
      const lines = raw.split(/\r?\n/);
      const emails = [];
      for (const line of lines) {
        const v = String(line || '').trim();
        if (v && /.+@.+\..+/.test(v)) emails.push(v);
      }
      resolve(emails);
    });
  });
}

async function parseUploadedFile(filepath, originalName) {
  const ext = path.extname(originalName || '').toLowerCase();
  log('Parsing uploaded file', { originalName, ext, filepath });
  if (ext === '.csv') return await parseCsvFile(filepath);
  if (ext === '.xls' || ext === '.xlsx') return await parseExcelFile(filepath);
  if (ext === '.txt') return await parseTextFile(filepath);

  // Fallback attempts
  try {
    const csvEmails = await parseCsvFile(filepath);
    if (csvEmails && csvEmails.length) return csvEmails;
  } catch (e) {
    logErr('CSV fallback parse failed:', e && e.message ? e.message : e);
  }
  try {
    const excelEmails = await parseExcelFile(filepath);
    if (excelEmails && excelEmails.length) return excelEmails;
  } catch (e) {
    logErr('Excel fallback parse failed:', e && e.message ? e.message : e);
  }
  try {
    const txtEmails = await parseTextFile(filepath);
    if (txtEmails && txtEmails.length) return txtEmails;
  } catch (e) {
    logErr('Text fallback parse failed:', e && e.message ? e.message : e);
  }
  return [];
}

// ---------- Utilities ----------
const delay = (ms) => new Promise((r) => setTimeout(r, ms));
const chunk = (arr, size) => {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
};
const isValidEmail = (s) => typeof s === 'string' && /.+@.+\..+/.test(s);
function safeUnlink(filepath) {
  fs.unlink(filepath, (err) => {
    if (err) logErr('Failed to unlink temp file', filepath, err.message || err);
  });
}

// ---------- Routes ----------

// Health/status
app.get('/status', async (req, res) => {
  try {
    let mailerOk = false;
    try { await transporter.verify(); mailerOk = true; } catch (e) { mailerOk = false; }
    return res.json({ ok: true, port: PORT, mailer: mailerOk });
  } catch (err) {
    logError(err, '/status error');
    return res.status(500).json({ ok: false, error: err.message || String(err) });
  }
});

// Test send
app.post('/test', express.json(), async (req, res) => {
  try {
    const to = process.env.SENDER_EMAIL || process.env.EMAIL_USER;
    if (!to) return res.status(500).json({ error: 'No sender/test recipient configured in env' });

    const subject = (req.body && req.body.subject) ? String(req.body.subject) : 'Test email from app';
    const text = (req.body && req.body.body) ? String(req.body.body) : 'This is a test email';
    const from = `${process.env.SENDER_NAME || 'Sender'} <${process.env.SENDER_EMAIL || process.env.EMAIL_USER}>`;

    const info = await transporter.sendMail({ from, to, subject, text });
    log('Test email sent:', info && info.messageId);
    return res.json({ ok: true, info });
  } catch (err) {
    logError(err, '/test send error');
    return res.status(500).json({ ok: false, error: err.message || String(err) });
  }
});

// Main send endpoint
app.post('/send', upload.single('file'), async (req, res) => {
  let filepath = null;
  try {
    if (!req.file) {
      return res.status(400).json({ error: "File required (field name: 'file')" });
    }

    filepath = req.file.path;
    const originalName = req.file.originalname || req.file.filename;
    log('Upload received', { originalName, size: req.file.size, path: filepath });

    const {
      previewOnly = 'false',
      mode = 'bcc-batched',
      subject = '',
      text = '',
      html = '',
      groupSize = '100',
      batchDelayMs = '120000',
      concurrency = '5',
      individualDelayMs = '1000',
    } = req.body || {};

    // parse
    let emails = await parseUploadedFile(filepath, originalName);
    log('Parser returned count=', emails.length);

    // sanitize/dedupe
    emails = (emails || []).map((e) => (e ? String(e).trim() : '')).filter((e) => e.length > 0 && isValidEmail(e));
    emails = Array.from(new Set(emails));
    log('After sanitize/dedupe valid emails count=', emails.length);

    // cleanup upload
    safeUnlink(filepath);
    filepath = null;

    if (!emails.length) {
      return res.status(400).json({ error: 'No valid emails found in the uploaded file.' });
    }

    if (String(previewOnly) === 'true') {
      return res.json({ count: emails.length, emails: emails.slice(0, 200) });
    }

    const from = `${process.env.SENDER_NAME || 'Sender'} <${process.env.SENDER_EMAIL || process.env.EMAIL_USER}>`;

    // BCC batched
    if (mode === 'bcc-batched') {
      const size = Math.min(100, Math.max(1, parseInt(groupSize, 10) || 100));
      const waitMs = Math.max(0, parseInt(batchDelayMs, 10) || 0);
      const groups = chunk(emails, size);

      log(`Starting BCC batched send: recipients=${emails.length}, groups=${groups.length}, groupSize=${size}, delay=${waitMs}ms`);
      const results = [];

      for (let gi = 0; gi < groups.length; gi++) {
        const bccList = groups[gi];
        log(`Sending batch ${gi + 1}/${groups.length} count=${bccList.length}`);

        try {
          const info = await transporter.sendMail({
            from,
            to: process.env.SENDER_EMAIL || process.env.EMAIL_USER,
            bcc: bccList,
            subject: String(subject || ''),
            text: String(text || ''),
            html: String(html || ''),
          });
          log(`Batch ${gi + 1} sent messageId=${info && info.messageId}`);
          results.push({ batch: gi + 1, success: true, count: bccList.length, accepted: info.accepted || null, rejected: info.rejected || null });
        } catch (err) {
          logError(err, `Batch ${gi + 1} send error`);
          results.push({ batch: gi + 1, success: false, count: bccList.length, error: err.message || String(err) });
        }

        if (gi < groups.length - 1 && waitMs > 0) {
          log(`Waiting ${waitMs}ms before next batch`);
          await delay(waitMs);
        }
      }

      const summary = {
        totalRecipients: emails.length,
        batches: results.length,
        successBatches: results.filter((r) => r.success).length,
        failedBatches: results.filter((r) => !r.success).length,
      };
      log('BCC batched done', summary);
      return res.json({ mode: 'bcc-batched', summary, results });
    }

    // Individual
    const batchSize = Math.max(1, parseInt(concurrency, 10) || 5);
    const waitMs = Math.max(0, parseInt(individualDelayMs, 10) || 0);
    log(`Starting individual send: total=${emails.length}, batchSize=${batchSize}, waitMs=${waitMs}`);

    const results = [];
    const sendSingle = (to) =>
      transporter
        .sendMail({ from, to, subject: String(subject || ''), text: String(text || ''), html: String(html || '') })
        .then((info) => ({ success: true, email: to, accepted: info.accepted || null, messageId: info.messageId || null }))
        .catch((err) => ({ success: false, email: to, error: err.message || String(err) }));

    for (let i = 0; i < emails.length; i += batchSize) {
      const batch = emails.slice(i, i + batchSize);
      log(`Sending individual batch index=${i} size=${batch.length}`);
      const settled = await Promise.all(batch.map(sendSingle));
      results.push(...settled);
      if (i + batchSize < emails.length && waitMs > 0) {
        log(`Waiting ${waitMs}ms before next individual batch`);
        await delay(waitMs);
      }
    }

    const summary = { total: emails.length, success: results.filter((r) => r.success).length, failed: results.filter((r) => !r.success).length };
    log('Individual send done', summary);
    return res.json({ mode: 'individual', summary, results });
  } catch (err) {
    logError(err, 'Server error in /send');
    if (filepath) safeUnlink(filepath);
    return res.status(500).json({ error: err.message || String(err) });
  }
});

// Express error handler (falls back for uncaught route errors etc.)
app.use((err, req, res, next) => {
  logError(err, `Express error handler for ${req.method} ${req.originalUrl}`);
  res.status(500).json({ error: err.message || 'Internal server error' });
});

// Fallback serve index.html if present
app.get('*', (_req, res) => {
  const indexPath = path.join(ROOT, 'public', 'index.html');
  if (fs.existsSync(indexPath)) return res.sendFile(indexPath);
  return res.status(404).send('Not found');
});

// Start server
const server = app.listen(PORT, () => {
  log(`Server listening at http://localhost:${PORT}`);
});

server.on('error', (err) => {
  logError(err, 'Server error (listen)');
  if (err.code === 'EADDRINUSE') {
    logErr(`Port ${PORT} already in use — choose a different PORT or stop the process using it.`);
  }
  process.exit(1);
});

// Graceful shutdown & global error hooks
process.on('SIGINT', () => {
  log('SIGINT received, shutting down gracefully...');
  server.close(() => {
    log('Server closed');
    process.exit(0);
  });
});
process.on('uncaughtException', (err) => {
  logError(err, 'uncaughtException');
  process.exit(1);
});
process.on('unhandledRejection', (err) => {
  logError(err, 'unhandledRejection');
});
