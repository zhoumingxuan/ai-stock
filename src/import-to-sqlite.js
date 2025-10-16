// 将行情与基础信息 CSV 数据导入到 SQLite。脚本假设所有文件以 UTF-8 存储。
const fs = require('fs');
const fsp = fs.promises;
const path = require('path');
const { parse } = require('csv-parse');
const sqlite3 = require('sqlite3');
const { open } = require('sqlite');

const ROOT_DIR = path.resolve(__dirname, '..');
const DATA_DIR = path.join(ROOT_DIR, 'data');
const WEEKLY_DIR = path.join(DATA_DIR, 'data_weekly_qfq');
const STOCK_INFO_DIR = path.join(DATA_DIR, 'stock_info');
const DB_PATH = path.join(ROOT_DIR, 'stock-data.sqlite');

const WEEKLY_COLUMNS = [
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
];

const WEEKLY_INSERT_COLUMNS = [...WEEKLY_COLUMNS, 'is_suspension_fill'];

const WEEKLY_NUMERIC_COLUMNS = new Set([
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
]);

const STOCK_INFO_COLUMNS = [
  'ts_code',
  'symbol',
  'name',
  'area',
  'industry',
  'cnspell',
  'market',
  'list_date',
  'act_name',
  'act_ent_type',
];

async function main() {
  const db = await open({
    filename: DB_PATH,
    driver: sqlite3.Database,
  });

  try {
    await db.exec('PRAGMA foreign_keys = ON;');
    await db.exec('PRAGMA journal_mode = WAL;');
    await createTables(db);
    await importWeeklyData(db);
    await importStockInfo(db);
  } finally {
    await db.close();
  }
}

async function createTables(db) {
  const weeklyColumnsSql = `
    CREATE TABLE IF NOT EXISTS weekly_qfq (
      ts_code TEXT NOT NULL, -- 股票代码
      trade_date TEXT NOT NULL, -- 交易日期（每周五或当月末）
      end_date TEXT, -- 计算截至日期
      freq TEXT, -- 频率（week/month）
      open REAL, -- 开盘价
      high REAL, -- 最高价
      low REAL, -- 最低价
      close REAL, -- 收盘价
      pre_close REAL, -- 上期收盘价（除权/前复权）
      open_qfq REAL, -- 前复权开盘价
      high_qfq REAL, -- 前复权最高价
      low_qfq REAL, -- 前复权最低价
      close_qfq REAL, -- 前复权收盘价
      open_hfq REAL, -- 后复权开盘价
      high_hfq REAL, -- 后复权最高价
      low_hfq REAL, -- 后复权最低价
      close_hfq REAL, -- 后复权收盘价
      vol REAL, -- 成交量
      amount REAL, -- 成交金额
      change REAL, -- 涨跌额
      pct_chg REAL, -- 涨跌幅（%）
      is_suspension_fill INTEGER NOT NULL DEFAULT 0, -- suspension fill flag
      PRIMARY KEY (ts_code, trade_date)
    );
  `;

  const stockInfoSql = `
    CREATE TABLE IF NOT EXISTS stock_info (
      ts_code TEXT PRIMARY KEY, -- TS代码
      symbol TEXT, -- 股票代码
      name TEXT, -- 股票名称
      area TEXT, -- 所属地区
      industry TEXT, -- 所属行业
      cnspell TEXT, -- 拼音缩写
      market TEXT, -- 市场类型（主板/创业板等）
      list_date TEXT, -- 上市日期
      act_name TEXT, -- 实控人名称
      act_ent_type TEXT -- 实控人企业性质
    );
  `;

  await db.exec(weeklyColumnsSql);
  await db.exec(stockInfoSql);
}

async function importWeeklyData(db) {
  const files = await listCsvFiles(WEEKLY_DIR);
  if (files.length === 0) {
    console.warn(`No weekly data files found in ${WEEKLY_DIR}`);
    return;
  }

  const columnsSql = WEEKLY_INSERT_COLUMNS.map((column) => `"${column}"`).join(', ');
  const placeholders = WEEKLY_INSERT_COLUMNS.map((column) => `@${column}`).join(', ');
  const insertSql = `INSERT OR REPLACE INTO weekly_qfq (${columnsSql}) VALUES (${placeholders});`;
  const statement = await db.prepare(insertSql);

  try {
    for (const file of files) {
      // 每个文件使用单独事务，确保失败时可以完整回滚。
      const filePath = path.join(WEEKLY_DIR, file);
      console.log(`Importing weekly data from ${file}...`);
      await db.exec('BEGIN TRANSACTION;');
      try {
        await streamCsv(filePath, (row) => {
          if (!row.ts_code || !row.trade_date) {
            return Promise.resolve();
          }

          const params = mapWeeklyRow(row);
          return statement.run(params);
        });
        await db.exec('COMMIT;');
      } catch (error) {
        await db.exec('ROLLBACK;');
        throw error;
      }
    }
  } finally {
    await statement.finalize();
  }
}

async function importStockInfo(db) {
  const files = await listCsvFiles(STOCK_INFO_DIR);
  if (files.length === 0) {
    console.warn(`No stock info files found in ${STOCK_INFO_DIR}`);
    return;
  }

  const columnsSql = STOCK_INFO_COLUMNS.map((column) => `"${column}"`).join(', ');
  const placeholders = STOCK_INFO_COLUMNS.map((column) => `@${column}`).join(', ');
  const insertSql = `INSERT OR REPLACE INTO stock_info (${columnsSql}) VALUES (${placeholders});`;
  const statement = await db.prepare(insertSql);

  try {
    for (const file of files) {
      // 依次导入每个基础信息文件，使用事务保证原子性。
      const filePath = path.join(STOCK_INFO_DIR, file);
      console.log(`Importing stock info from ${file}...`);
      await db.exec('BEGIN TRANSACTION;');
      try {
        await streamCsv(filePath, (row) => {
          if (!row.ts_code) {
            return Promise.resolve();
          }

          const params = mapStockInfoRow(row);
          return statement.run(params);
        });
        await db.exec('COMMIT;');
      } catch (error) {
        await db.exec('ROLLBACK;');
        throw error;
      }
    }
  } finally {
    await statement.finalize();
  }
}

async function listCsvFiles(directory) {
  try {
    const entries = await fsp.readdir(directory, { withFileTypes: true });
    return entries
      .filter((entry) => entry.isFile() && entry.name.toLowerCase().endsWith('.csv'))
      .map((entry) => entry.name)
      .sort();
  } catch (error) {
    if (error.code === 'ENOENT') {
      return [];
    }
    throw error;
  }
}

function mapWeeklyRow(row) {
  const params = {};
  for (const column of WEEKLY_INSERT_COLUMNS) {
    if (column === 'is_suspension_fill') {
      params[`@${column}`] = 0;
      continue;
    }
    const value = row[column];
    if (WEEKLY_NUMERIC_COLUMNS.has(column)) {
      params[`@${column}`] = toNumber(value);
    } else {
      params[`@${column}`] = sanitizeString(value);
    }
  }
  return params;
}

function mapStockInfoRow(row) {
  const params = {};
  for (const column of STOCK_INFO_COLUMNS) {
    params[`@${column}`] = sanitizeString(row[column]);
  }
  return params;
}

function sanitizeString(value) {
  if (value === undefined || value === null) {
    return null;
  }
  const trimmed = String(value).trim();
  return trimmed === '' ? null : trimmed;
}

function toNumber(value) {
  if (value === undefined || value === null) {
    return null;
  }
  const trimmed = String(value).trim();
  if (trimmed === '') {
    return null;
  }
  const number = Number(trimmed);
  return Number.isNaN(number) ? null : number;
}

function streamCsv(filePath, onRow) {
  return new Promise((resolve, reject) => {
    // 强制使用 UTF-8 读取以兼容中文字段，同时启用 BOM 处理。
    const stream = fs.createReadStream(filePath, { encoding: 'utf8' });
    const parser = parse({
      columns: true,
      trim: true,
      skip_empty_lines: true,
      bom: true,
    });

    let finished = false;

    const fail = (error) => {
      if (!finished) {
        finished = true;
        stream.destroy();
        reject(error);
      }
    };

    parser.on('data', (row) => {
      parser.pause();
      Promise.resolve(onRow(row))
        .then(() => {
          if (!finished) {
            parser.resume();
          }
        })
        .catch(fail);
    });

    parser.on('end', () => {
      if (!finished) {
        finished = true;
        resolve();
      }
    });

    parser.on('error', fail);
    stream.on('error', fail);

    stream.pipe(parser);
  });
}

if (require.main === module) {
  main().catch((error) => {
    console.error('Import failed:', error);
    process.exitCode = 1;
  });
}
