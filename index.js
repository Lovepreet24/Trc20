
import pg from "pg";

const { Pool } = pg;

const API_BASE = "https://apilist.tronscanapi.com";
const USDT_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t";
const USDT_DECIMALS = 6;

const config = {
  apiKey: process.env.TRONSCAN_API_KEY,
  botToken: process.env.TELEGRAM_BOT_TOKEN,
  chatId: process.env.TELEGRAM_CHAT_ID,
  databaseUrl: process.env.DATABASE_URL,

  min: BigInt(process.env.MIN_USDT || "30"),
  max: BigInt(process.env.MAX_USDT || "350"),

  pollSeconds: Math.max(15, Number(process.env.POLL_SECONDS || 20)),
  initialLookbackMinutes: Math.max(
    1,
    Number(process.env.INITIAL_LOOKBACK_MINUTES || 10)
  ),
  windowSeconds: Math.max(10, Number(process.env.WINDOW_SECONDS || 60)),
  pageSize: Math.min(50, Math.max(1, Number(process.env.PAGE_SIZE || 50))),
  maxPages: Math.max(
    1,
    Number(process.env.MAX_PAGES_PER_WINDOW || 200)
  ),

  excludedAddresses: new Set(
    (process.env.EXCLUDED_ADDRESSES || "")
      .split(",")
      .map((s) => s.trim().toLowerCase())
      .filter(Boolean)
  )
};

const pool = new Pool({
  connectionString: config.databaseUrl,
  ssl: { rejectUnauthorized: false },
  connectionTimeoutMillis: 15000,
  idleTimeoutMillis: 30000,
  max: 5
});

function log(level, message, extra = {}) {
  console.log(JSON.stringify({
    time: new Date().toISOString(),
    level,
    message,
    ...extra
  }));
}

function nowMs() {
  return Date.now();
}

function isTrue(value) {
  return value === true || value === 1 || value === "true";
}

function getAmountWhole(tx) {
  const token = tx?.tokenInfo || {};

  const decimals = Number(
    token.tokenDecimal ??
    token.token_decimal ??
    tx.tokenDecimal ??
    USDT_DECIMALS
  );

  if (decimals !== USDT_DECIMALS) return null;

  const rawValue = tx?.quant ?? tx?.amount_str ?? tx?.amount;
  if (rawValue === undefined || rawValue === null) return null;

  let raw;

  try {
    raw = BigInt(String(rawValue));
  } catch {
    return null;
  }

  if (raw < 0n) return null;

  const scale = 10n ** BigInt(decimals);

  // Only exact whole-number USDT transfers.
  if (raw % scale !== 0n) return null;

  const whole = raw / scale;

  if (whole < config.min || whole > config.max) return null;

  return whole;
}

function transferFields(tx) {
  return {
    txid: tx?.transaction_id || tx?.transactionId || tx?.hash || "",
    from: tx?.from_address || tx?.fromAddress || "",
    to: tx?.to_address || tx?.toAddress || "",
    timestamp: Number(
      tx?.block_ts ??
      tx?.block_timestamp ??
      tx?.timestamp ??
      0
    ),
    confirmed: isTrue(tx?.confirmed),
    contractRet: String(tx?.contractRet || "").toUpperCase(),
    finalResult: String(tx?.finalResult || "").toUpperCase(),
    reverted: isTrue(tx?.revert),
    contract:
      tx?.contract_address ||
      tx?.contractAddress ||
      tx?.tokenInfo?.tokenId ||
      "",
    eventType: String(tx?.event_type || tx?.eventType || "Transfer")
  };
}

function eligibleTransfer(tx) {
  const f = transferFields(tx);
  const amount = getAmountWhole(tx);

  if (!f.txid || !f.from || !f.to || amount === null) return null;

  if (f.contract && f.contract !== USDT_CONTRACT) return null;

  if (f.eventType.toLowerCase() !== "transfer") return null;

  // Never post unconfirmed or failed transactions.
  if (!f.confirmed || f.reverted) return null;
  if (f.contractRet !== "SUCCESS") return null;
  if (f.finalResult !== "SUCCESS") return null;

  if (
    config.excludedAddresses.has(f.from.toLowerCase()) ||
    config.excludedAddresses.has(f.to.toLowerCase())
  ) {
    return null;
  }

  return { ...f, amount };
}

async function requestJson(url, headers = {}, attempts = 5) {
  let lastError;

  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      const response = await fetch(url, {
        headers,
        signal: AbortSignal.timeout(20000)
      });

      const body = await response.text();

      if (!response.ok) {
        const detail = body.slice(0, 500);

        const error = new Error(
          `HTTP ${response.status}: ${detail}`
        );

        error.status = response.status;

        // Don't retry permanent authentication or bad-request errors.
        if ([400, 401, 403, 404].includes(response.status)) {
          throw error;
        }

        throw error;
      }

      try {
        return JSON.parse(body);
      } catch {
        throw new Error(`Invalid JSON response: ${body.slice(0, 300)}`);
      }
    } catch (error) {
      lastError = error;

      log("warn", "Request attempt failed", {
        attempt,
        maxAttempts: attempts,
        status: error.status || null,
        error: String(error.message || error).slice(0, 500)
      });

      if (
        error.status &&
        [400, 401, 403, 404].includes(error.status)
      ) {
        break;
      }

      if (attempt < attempts) {
        await new Promise((resolve) =>
          setTimeout(resolve, Math.min(1000 * 2 ** (attempt - 1), 10000))
        );
      }
    }
  }

  throw lastError || new Error("Request failed");
}

async function fetchWindowPage(startMs, endMs, offset) {
  const url = new URL("/api/token_trc20/transfers", API_BASE);

  url.searchParams.set("contract_address", USDT_CONTRACT);

  // Request confirmed transfers.
  url.searchParams.set("confirm", "1");

  url.searchParams.set("start_timestamp", String(startMs));
  url.searchParams.set("end_timestamp", String(endMs));
  url.searchParams.set("start", String(offset));
  url.searchParams.set("limit", String(config.pageSize));

  const headers = {};

  if (config.apiKey) {
    headers["TRON-PRO-API-KEY"] = config.apiKey;
  }

  return requestJson(url, headers);
}

async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS scanner_state (
      id TEXT PRIMARY KEY,
      checkpoint_ms BIGINT NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS scanner_transfers (
      txid TEXT PRIMARY KEY,
      amount_usdt BIGINT NOT NULL,
      from_address TEXT NOT NULL,
      to_address TEXT NOT NULL,
      block_timestamp BIGINT,
      posted_at TIMESTAMPTZ,
      attempts INTEGER NOT NULL DEFAULT 0,
      last_error TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS scanner_transfers_pending_idx
      ON scanner_transfers (created_at)
      WHERE posted_at IS NULL;
  `);

  const result = await pool.query(
    "SELECT checkpoint_ms FROM scanner_state WHERE id = $1",
    ["trc20-usdt"]
  );

  if (result.rowCount === 0) {
    const initial =
      nowMs() - config.initialLookbackMinutes * 60_000;

    await pool.query(
      `INSERT INTO scanner_state (id, checkpoint_ms)
       VALUES ($1, $2)
       ON CONFLICT (id) DO NOTHING`,
      ["trc20-usdt", String(initial)]
    );
  }
}

async function getCheckpoint() {
  const result = await pool.query(
    "SELECT checkpoint_ms FROM scanner_state WHERE id = $1",
    ["trc20-usdt"]
  );

  if (result.rowCount === 0) {
    throw new Error("Scanner checkpoint is missing");
  }

  return Number(result.rows[0].checkpoint_ms);
}

async function saveCheckpoint(checkpoint) {
  await pool.query(
    `UPDATE scanner_state
     SET checkpoint_ms = $2, updated_at = NOW()
     WHERE id = $1`,
    ["trc20-usdt", String(checkpoint)]
  );
}

async function scanWindow(startMs, endMs) {
  let offset = 0;
  let pages = 0;
  let fetched = 0;
  let candidates = 0;

  while (pages < config.maxPages) {
    const data = await fetchWindowPage(startMs, endMs, offset);

    const rows = Array.isArray(data.token_transfers)
      ? data.token_transfers
      : Array.isArray(data.data)
        ? data.data
        : [];

    pages++;
    fetched += rows.length;

    for (const tx of rows) {
      const candidate = eligibleTransfer(tx);

      if (!candidate) continue;

      const inserted = await pool.query(
        `INSERT INTO scanner_transfers
          (txid, amount_usdt, from_address, to_address, block_timestamp)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (txid) DO NOTHING`,
        [
          candidate.txid,
          String(candidate.amount),
          candidate.from,
          candidate.to,
          candidate.timestamp || null
        ]
      );

      candidates += inserted.rowCount;
    }

    if (rows.length < config.pageSize) {
      log("info", "Scanned time window", {
        start: new Date(startMs).toISOString(),
        end: new Date(endMs).toISOString(),
        pages,
        fetched,
        candidates,
        complete: true
      });

      return { complete: true };
    }

    offset += rows.length;

    // Avoid silently skipping a window that has too many results.
    if (offset >= 10000) break;
  }

  log("error", "Scan window incomplete; checkpoint not advanced", {
    start: new Date(startMs).toISOString(),
    end: new Date(endMs).toISOString(),
    pages,
    fetched,
    candidates,
    complete: false
  });

  return { complete: false };
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;"
  })[char]);
}

function postText(row) {
  const txid = escapeHtml(row.txid);
  const from = escapeHtml(row.from_address);
  const to = escapeHtml(row.to_address);
  const amount = escapeHtml(String(row.amount_usdt));

  return [
    "🟢 <b>New USDT Transfer</b>",
    "",
    `💵 Amount: <b>${amount} USDT</b>`,
    "🌐 Network: TRON (TRC20)",
    `📤 From: <code>${from}</code>`,
    `📥 To: <code>${to}</code>`,
    `🔗 <a href="https://tronscan.org/#/transaction/${txid}">View transaction</a>`
  ].join("\n");
}

async function sendTelegram(text) {
  if (!config.botToken || !config.chatId) {
    throw new Error("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is missing");
  }

  const url =
    `https://api.telegram.org/bot${config.botToken}/sendMessage`;

  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      chat_id: config.chatId,
      text,
      parse_mode: "HTML",
      disable_web_page_preview: true
    }),
    signal: AbortSignal.timeout(20000)
  });

  const body = await response.json().catch(() => ({}));

  if (!response.ok || body.ok !== true) {
    throw new Error(
      `Telegram send failed: HTTP ${response.status} ` +
      JSON.stringify(body).slice(0, 500)
    );
  }
}

async function flushPending() {
  const result = await pool.query(
    `SELECT txid, amount_usdt, from_address, to_address
     FROM scanner_transfers
     WHERE posted_at IS NULL
     ORDER BY created_at ASC
     LIMIT 20`
  );

  for (const row of result.rows) {
    try {
      await sendTelegram(postText(row));

      await pool.query(
        `UPDATE scanner_transfers
         SET posted_at = NOW(), last_error = NULL
         WHERE txid = $1 AND posted_at IS NULL`,
        [row.txid]
      );

      log("info", "Telegram post sent", {
        txid: row.txid,
        amount: row.amount_usdt
      });
    } catch (error) {
      await pool.query(
        `UPDATE scanner_transfers
         SET attempts = attempts + 1, last_error = $2
         WHERE txid = $1`,
        [row.txid, String(error.message || error).slice(0, 1000)]
      );

      log("error", "Telegram post failed", {
        txid: row.txid,
        error: String(error.message || error).slice(0, 500)
      });

      // Avoid hammering Telegram if it is unavailable.
      break;
    }
  }
}

async function runCycle() {
  await flushPending();

  let checkpoint = await getCheckpoint();
  const safeEnd = nowMs() - 30_000;

  while (checkpoint < safeEnd) {
    const end = Math.min(
      safeEnd,
      checkpoint + config.windowSeconds * 1000
    );

    const result = await scanWindow(checkpoint, end);

    if (!result.complete) {
      // Retry this window next cycle instead of skipping it.
      return;
    }

    await saveCheckpoint(end);
    checkpoint = end;
  }

  await flushPending();
}

async function main() {
  if (!config.databaseUrl) {
    throw new Error("DATABASE_URL is missing");
  }

  if (config.min < 0n || config.max < config.min) {
    throw new Error("Invalid MIN_USDT / MAX_USDT configuration");
  }

  await pool.query("SELECT 1");
  await initDb();

  log("info", "Scanner started", {
    min: String(config.min),
    max: String(config.max),
    pollSeconds: config.pollSeconds,
    windowSeconds: config.windowSeconds,
    database: "connected"
  });

  while (true) {
    try {
      await runCycle();
    } catch (error) {
      log("error", "Cycle failed", {
        error: String(error.stack || error).slice(0, 1500)
      });
    }

    await new Promise((resolve) =>
      setTimeout(resolve, config.pollSeconds * 1000)
    );
  }
}

process.on("SIGTERM", async () => {
  log("info", "Received SIGTERM; shutting down");
  await pool.end().catch(() => {});
  process.exit(0);
});

process.on("SIGINT", async () => {
  log("info", "Received SIGINT; shutting down");
  await pool.end().catch(() => {});
  process.exit(0);
});

main().catch(async (error) => {
  log("error", "Fatal startup error", {
    error: String(error.stack || error).slice(0, 1500)
  });

  await pool.end().catch(() => {});
  process.exit(1);
});
