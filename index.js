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
  pollSeconds: Math.max(10, Number(process.env.POLL_SECONDS || 15)),
  initialLookbackMinutes: Math.max(1, Number(process.env.INITIAL_LOOKBACK_MINUTES || 10)),
  windowSeconds: Math.max(10, Number(process.env.WINDOW_SECONDS || 60)),
  pageSize: Math.min(50, Math.max(1, Number(process.env.PAGE_SIZE || 50))),
  maxPages: Math.max(1, Number(process.env.MAX_PAGES_PER_WINDOW || 200)),
  title: process.env.TITLE || "New USDT Transfer",
  excludedAddresses: new Set(
    (process.env.EXCLUDED_ADDRESSES || "")
      .split(",").map(s => s.trim()).filter(Boolean)
  )
};

if (!config.apiKey || !config.botToken || !config.chatId || !config.databaseUrl) {
  console.error("Missing required environment variables. See .env.example");
  process.exit(1);
}
if (config.min < 0n || config.max < config.min) {
  console.error("Invalid MIN_USDT/MAX_USDT");
  process.exit(1);
}

const pool = new Pool({
  connectionString: config.databaseUrl,
  ssl: process.env.PGSSLMODE === "disable" ? false : { rejectUnauthorized: false },
  max: 3,
  connectionTimeoutMillis: 10000,
  idleTimeoutMillis: 30000
});

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const nowMs = () => Date.now();
const iso = ms => new Date(ms).toISOString();
function log(level, message, extra = {}) {
  console.log(JSON.stringify({ time: new Date().toISOString(), level, message, ...extra }));
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
      ON scanner_transfers (created_at) WHERE posted_at IS NULL;
  `);

  const result = await pool.query("SELECT checkpoint_ms FROM scanner_state WHERE id = $1", ["trc20-usdt"]);
  if (result.rowCount === 0) {
    const initial = nowMs() - config.initialLookbackMinutes * 60_000;
    await pool.query(
      "INSERT INTO scanner_state (id, checkpoint_ms) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING",
      ["trc20-usdt", String(initial)]
    );
    log("info", "Initialized scan checkpoint", { checkpoint: iso(initial) });
  }
}

function getAmountWhole(tx) {
  const token = tx?.tokenInfo || {};
  const decimals = Number(token.tokenDecimal ?? token.token_decimal ?? tx.tokenDecimal ?? USDT_DECIMALS);
  if (decimals !== USDT_DECIMALS) return null;

  const rawValue = tx?.quant ?? tx?.amount_str ?? tx?.amount;
  if (rawValue === undefined || rawValue === null) return null;

  let raw;
  try { raw = BigInt(String(rawValue)); } catch { return null; }
  if (raw < 0n) return null;
  const scale = 10n ** BigInt(decimals);
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
    timestamp: Number(tx?.block_ts ?? tx?.block_timestamp ?? tx?.timestamp ?? 0),
    // The request itself uses TronScan confirm=0 (confirmed-only); do not depend on an optional response boolean.\n    confirmed: tx?.revert !== true,
    successful: (tx?.contractRet ?? tx?.contract_ret ?? tx?.result ?? "SUCCESS") === "SUCCESS",
    contract: tx?.contract_address || tx?.contractAddress || tx?.tokenInfo?.tokenId || "",
    eventType: tx?.event_type || tx?.eventType || "Transfer"
  };
}

function eligibleTransfer(tx) {
  const f = transferFields(tx);
  const amount = getAmountWhole(tx);
  if (!f.txid || !f.from || !f.to || amount === null) return null;
  if (f.contract && f.contract !== USDT_CONTRACT) return null;
  if (f.eventType.toLowerCase() !== "transfer") return null;
  if (!f.confirmed || !f.successful) return null;
  if (config.excludedAddresses.has(f.from) || config.excludedAddresses.has(f.to)) return null;
  return { ...f, amount };
}

async function fetchWindowPage(startMs, endMs, offset) {
  const url = new URL("/api/token_trc20/transfers", API_BASE);
  url.searchParams.set("contract_address", USDT_CONTRACT);
  url.searchParams.set("confirm", "0");
  url.searchParams.set("start_timestamp", String(startMs));
  url.searchParams.set("end_timestamp", String(endMs));
  url.searchParams.set("start", String(offset));
  url.searchParams.set("limit", String(config.pageSize));

  for (let attempt = 0; attempt < 5; attempt++) {
    try {
      const response = await fetch(url, {
        headers: { "TRON-PRO-API-KEY": config.apiKey },
        signal: AbortSignal.timeout(20000)
      });
      if (response.status === 429 || response.status >= 500) {
        const retryAfter = Number(response.headers.get("retry-after") || 0);
        await sleep(retryAfter > 0 ? retryAfter * 1000 : Math.min(30000, 1000 * (2 ** attempt)));
        continue;
      }
      if (!response.ok) {
        const body = (await response.text()).slice(0, 300);
        throw new Error(`TronScan HTTP ${response.status}: ${body}`);
      }
      return await response.json();
    } catch (error) {
      if (attempt === 4) throw error;
      await sleep(Math.min(30000, 1000 * (2 ** attempt)));
    }
  }
  throw new Error("TronScan request failed after retries");
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
      : Array.isArray(data.data) ? data.data : [];
    fetched += rows.length;
    pages++;

    for (const tx of rows) {
      const candidate = eligibleTransfer(tx);
      if (!candidate) continue;
      const inserted = await pool.query(
        `INSERT INTO scanner_transfers (txid, amount_usdt, from_address, to_address, block_timestamp)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (txid) DO NOTHING`,
        [candidate.txid, candidate.amount.toString(), candidate.from, candidate.to,
         candidate.timestamp || null]
      );
      candidates += inserted.rowCount;
    }

    if (rows.length < config.pageSize) {
      return { pages, fetched, candidates, complete: true };
    }
    offset += rows.length;
    if (offset >= 10000) break;
  }

  // Do not advance checkpoint if the window was not fully paginated.
  return { pages, fetched, candidates, complete: false };
}

function escapeHtml(value) {
  return String(value).replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
}

function postText(row) {
  const txid = row.txid;
  return [
    `🟢 <b>${escapeHtml(config.title)}</b>`,
    "",
    `<b>Amount:</b> ${row.amount_usdt} USDT`,
    "<b>Network:</b> TRON (TRC20)",
    `<b>TXID:</b> <code>${escapeHtml(txid)}</code>`,
    "",
    `<a href="https://tronscan.org/#/transaction/${encodeURIComponent(txid)}">View Transaction on TronScan</a>`
  ].join("\n");
}

async function sendTelegram(text) {
  const response = await fetch(`https://api.telegram.org/bot${config.botToken}/sendMessage`, {
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
  const data = await response.json().catch(() => ({}));
  if (!response.ok || !data.ok) {
    const retryAfter = data?.parameters?.retry_after;
    const error = new Error(`Telegram API error ${response.status}: ${data?.description || "unknown error"}`);
    error.retryAfter = retryAfter;
    throw error;
  }
}

async function flushPending() {
  const pending = await pool.query(
    `SELECT txid, amount_usdt, from_address, to_address, block_timestamp, attempts
     FROM scanner_transfers WHERE posted_at IS NULL
     ORDER BY block_timestamp NULLS FIRST, created_at ASC LIMIT 25`
  );

  for (const row of pending.rows) {
    try {
      await sendTelegram(postText(row));
      await pool.query(
        "UPDATE scanner_transfers SET posted_at = NOW(), last_error = NULL WHERE txid = $1",
        [row.txid]
      );
      log("info", "Posted transfer", { txid: row.txid, amount: row.amount_usdt });
    } catch (error) {
      await pool.query(
        "UPDATE scanner_transfers SET attempts = attempts + 1, last_error = $2 WHERE txid = $1",
        [row.txid, String(error?.message || error).slice(0, 500)]
      );
      log("error", "Could not post transfer; will retry", { txid: row.txid, error: error.message });
      if (error.retryAfter) await sleep(error.retryAfter * 1000);
      else await sleep(1500);
      break;
    }
  }
}

async function runCycle() {
  // Deliver queued messages first, so a Telegram outage doesn't block scanning.
  await flushPending();

  const state = await pool.query("SELECT checkpoint_ms FROM scanner_state WHERE id = $1", ["trc20-usdt"]);
  let checkpoint = Number(state.rows[0].checkpoint_ms);
  const now = nowMs();
  const endSafe = now - 15_000; // small safety lag for indexer/confirmation delay
  if (checkpoint >= endSafe) return;

  while (checkpoint < endSafe) {
    const end = Math.min(endSafe, checkpoint + config.windowSeconds * 1000);
    const result = await scanWindow(checkpoint, end);

    if (!result.complete) {
      log("error", "Window pagination limit reached; checkpoint not advanced to avoid silent skips", {
        start: iso(checkpoint), end: iso(end), pages: result.pages, fetched: result.fetched
      });
      return;
    }

    await pool.query(
      "UPDATE scanner_state SET checkpoint_ms = $2, updated_at = NOW() WHERE id = $1",
      ["trc20-usdt", String(end)]
    );
    checkpoint = end;
    log("info", "Scanned time window", {
      start: iso(result.startMs ?? (end - config.windowSeconds * 1000)),
      end: iso(end),
      pages: result.pages,
      fetched: result.fetched,
      candidates: result.candidates
    });
  }

  await flushPending();
}

let stopping = false;
async function shutdown(signal) {
  if (stopping) return;
  stopping = true;
  log("info", `Received ${signal}; shutting down`);
  try { await pool.end(); } catch {}
  process.exit(0);
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

await initDb();
log("info", "Scanner started", {
  min: config.min.toString(),
  max: config.max.toString(),
  pollSeconds: config.pollSeconds,
  windowSeconds: config.windowSeconds,
  database: "connected"
});

while (!stopping) {
  try {
    await runCycle();
  } catch (error) {
    log("error", "Cycle failed", { error: String(error?.stack || error).slice(0, 1200) });
  }
  await sleep(config.pollSeconds * 1000);
}
