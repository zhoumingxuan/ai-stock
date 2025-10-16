const path = require('path');
const sqlite3 = require('sqlite3');
const { open } = require('sqlite');

const ROOT_DIR = path.resolve(__dirname, '..');
const DB_PATH = path.join(ROOT_DIR, 'stock-data.sqlite');

const INSERT_COLUMNS = [
  'ts_code',
  'trade_date',
  'end_date',
  'freq',
  'open',
  'high',
  'low',
  'close',
  'pre_close',
  'open_qfq',
  'high_qfq',
  'low_qfq',
  'close_qfq',
  'open_hfq',
  'high_hfq',
  'low_hfq',
  'close_hfq',
  'vol',
  'amount',
  'change',
  'pct_chg',
  'is_suspension_fill',
];

async function main() {
  const db = await open({
    filename: DB_PATH,
    driver: sqlite3.Database,
  });

  try {
    await db.exec('PRAGMA journal_mode = WAL;');
    const allTradeDates = await fetchAllTradeDates(db);
    if (allTradeDates.length === 0) {
      console.log('No trade dates found in weekly_qfq; nothing to fill.');
      return;
    }

    const tradeDateIndex = buildTradeDateIndex(allTradeDates);
    const tsCodes = await fetchAllTsCodes(db);
    if (tsCodes.length === 0) {
      console.log('No ts_code entries found in weekly_qfq; nothing to fill.');
      return;
    }

    const insertStmt = await prepareInsertStatement(db);
    let totalInserted = 0;

    try {
      for (const tsCode of tsCodes) {
        const inserted = await fillForSymbol({
          db,
          insertStmt,
          tsCode,
          allTradeDates,
          tradeDateIndex,
        });
        if (inserted > 0) {
          console.log(`Filled ${inserted} suspension rows for ${tsCode}.`);
        }
        totalInserted += inserted;
      }
    } finally {
      await insertStmt.finalize();
    }

    console.log(`Suspension fill complete. Total new rows: ${totalInserted}.`);
  } finally {
    await db.close();
  }
}

async function fetchAllTradeDates(db) {
  const rows = await db.all('SELECT DISTINCT trade_date FROM weekly_qfq ORDER BY trade_date ASC;');
  return rows.map((row) => row.trade_date);
}

async function fetchAllTsCodes(db) {
  const rows = await db.all('SELECT DISTINCT ts_code FROM weekly_qfq ORDER BY ts_code ASC;');
  return rows.map((row) => row.ts_code);
}

function buildTradeDateIndex(tradeDates) {
  const map = new Map();
  for (let i = 0; i < tradeDates.length; i += 1) {
    map.set(tradeDates[i], i);
  }
  return map;
}

async function prepareInsertStatement(db) {
  const columnsSql = INSERT_COLUMNS.join(', ');
  const placeholders = INSERT_COLUMNS.map(() => '?').join(', ');
  const sql = `INSERT OR REPLACE INTO weekly_qfq (${columnsSql}) VALUES (${placeholders});`;
  return db.prepare(sql);
}

function normalizeSuspensionRow(row) {
  const normalized = { ...row };
  const close = normalized.close;
  if (close !== null && close !== undefined) {
    normalized.open = close;
    normalized.high = close;
    normalized.low = close;
    normalized.pre_close = close;
  }

  const closeQfq = normalized.close_qfq;
  if (closeQfq !== null && closeQfq !== undefined) {
    normalized.open_qfq = closeQfq;
    normalized.high_qfq = closeQfq;
    normalized.low_qfq = closeQfq;
    normalized.close_qfq = closeQfq;
  }

  const closeHfq = normalized.close_hfq;
  if (closeHfq !== null && closeHfq !== undefined) {
    normalized.open_hfq = closeHfq;
    normalized.high_hfq = closeHfq;
    normalized.low_hfq = closeHfq;
    normalized.close_hfq = closeHfq;
  }

  normalized.amount=0;
  normalized.vol=0;
  normalized.change=0;
  normalized.pct_chg=0;

  normalized.is_suspension_fill = 1;
  return normalized;
}

async function fillForSymbol({ db, insertStmt, tsCode, allTradeDates, tradeDateIndex }) {
  const rows = await db.all(
    'SELECT * FROM weekly_qfq WHERE ts_code = ? ORDER BY trade_date ASC;',
    tsCode,
  );

  if (rows.length === 0) {
    return 0;
  }

  const firstDate = rows[0].trade_date;
  const lastDate = rows[rows.length - 1].trade_date;
  const startIndex = tradeDateIndex.get(firstDate);
  const endIndex = tradeDateIndex.get(lastDate);

  if (startIndex === undefined || endIndex === undefined) {
    console.warn(`Trade date range missing in global index for ${tsCode}; skipping.`);
    return 0;
  }

  if (startIndex >= endIndex) {
    return 0;
  }

  const existingByDate = new Map(rows.map((row) => [row.trade_date, row]));
  let lastRow = null;
  let inserted = 0;

  await db.exec('BEGIN TRANSACTION;');
  try {
    for (let idx = startIndex; idx <= endIndex; idx += 1) {
      const tradeDate = allTradeDates[idx];
      const existing = existingByDate.get(tradeDate);

      if (existing) {
        if (existing.is_suspension_fill) {
          const normalized = normalizeSuspensionRow(existing);
          const values = INSERT_COLUMNS.map((column) => normalized[column] ?? null);
          await insertStmt.run(values);
          existingByDate.set(tradeDate, normalized);
          lastRow = normalized;
        } else {
          lastRow = existing;
        }
        continue;
      }

      if (!lastRow) {
        console.warn(`Missing baseline row before ${tradeDate} for ${tsCode}; skipping fill.`);
        continue;
      }

      let newRow = { ...lastRow };
      newRow.trade_date = tradeDate;
      if (newRow.end_date !== null && newRow.end_date !== undefined) {
        newRow.end_date = tradeDate;
      }

      newRow = normalizeSuspensionRow(newRow);

      const values = INSERT_COLUMNS.map((column) => newRow[column] ?? null);
      await insertStmt.run(values);

      lastRow = newRow;
      existingByDate.set(tradeDate, newRow);
      inserted += 1;
    }

    await db.exec('COMMIT;');
  } catch (error) {
    await db.exec('ROLLBACK;');
    throw error;
  }

  return inserted;
}

if (require.main === module) {
  main().catch((error) => {
    console.error('Suspension fill failed:', error);
    process.exitCode = 1;
  });
}
