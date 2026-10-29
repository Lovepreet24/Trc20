import pg from "pg";

const { Pool } = pg;

const API_BASE = "https://apilist.tronscanapi.com";
const USDT_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t";
const USDT_DECIMALS = 6;
const ADMIN_ID = 7982111846;
const MAX_PAYMENT_AGE_MS = 60_000;

const config = {
  apiKey: process.env.TRONSCAN_API_KEY,
  botToken: process.env.TELEGRAM_BOT_TOKEN,
  chatId: process.env.TELEGRAM_CHAT_ID,
  databaseUrl: process.env.DATABASE_URL,
  pollSeconds: Math.max(15, Number(process.env.POLL_SECONDS || 20)),
  initialLookbackMinutes: Math.max(1, Number(process.env.INITIAL_LOOKBACK_MINUTES || 10)),
  windowSeconds: Math.max(10, Number(process.env.WINDOW_SECONDS || 60)),
  pageSize: Math.min(50, Math.max(1, Number(process.env.PAGE_SIZE || 50))),
  maxPages: Math.max(1, Number(process.env.MAX_PAGES_PER_WINDOW || 200)),
  excludedAddresses: new Set(
    (process.env.EXCLUDED_ADDRESSES || "")
      .split(",").map(s => s.trim().toLowerCase()).filter(Boolean)
  )
};

if (!config.databaseUrl || !config.botToken) {
  throw new Error("DATABASE_URL and TELEGRAM_BOT_TOKEN are required");
}

const pool = new Pool({
  connectionString: config.databaseUrl,
  ssl: { rejectUnauthorized: false },
  connectionTimeoutMillis: 15000,
  idleTimeoutMillis: 30000,
  max: 5
});

const defaults = {
  channel: config.chatId || "",
  min: Number(process.env.MIN_USDT || 30),
  max: Number(process.env.MAX_USDT || 350),
  hourlyTarget: 31,
  minDelay: 75,
  maxDelay: 150,
  maxAgeSeconds: 60,
  buttonText: "View Transaction",
  buttonUrl: "",
  paused: false
};

let settings = { ...defaults };
let adminOffset = 0;
let adminInput = new Map();
let postingWorkerStarted = false;
let lastApiRequest = 0;

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

function log(level, message, extra = {}) {
  console.log(JSON.stringify({
    time: new Date().toISOString(), level, message, ...extra
  }));
}

function isTrue(value) {
  return value === true || value === 1 || value === "true";
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, c => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;",
    '"': "&quot;", "'": "&#39;"
  })[c]);
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

    CREATE TABLE IF NOT EXISTS scanner_hourly_limits (
      hour_start TIMESTAMPTZ PRIMARY KEY,
      target_count INTEGER NOT NULL CHECK (target_count BETWEEN 1 AND 100)
    );

    CREATE TABLE IF NOT EXISTS scanner_settings (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      settings JSONB NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(
    `INSERT INTO scanner_settings (id, settings)
     VALUES (1, $1::jsonb)
     ON CONFLICT (id) DO NOTHING`,
    [JSON.stringify(defaults)]
  );

  const r = await pool.query(
    "SELECT settings FROM scanner_settings WHERE id = 1"
  );
  settings = { ...defaults, ...r.rows[0].settings };

  // Do not post anything left pending from before this deployment.
  await pool.query(`
    UPDATE scanner_transfers
    SET posted_at = NOW(),
        last_error = 'Skipped: latest-only deployment'
    WHERE posted_at IS NULL
  `);

  // Start near the present instead of backfilling historical transfers.
  await pool.query(
    `INSERT INTO scanner_state (id, checkpoint_ms)
     VALUES ($1, $2)
     ON CONFLICT (id) DO UPDATE
       SET checkpoint_ms = EXCLUDED.checkpoint_ms,
           updated_at = NOW()`,
    ["trc20-usdt", String(Date.now() - 30000)]
  );

  log("info", "Database ready; old pending transfers skipped");
}

async function saveSettings() {
  await pool.query(
    `UPDATE scanner_settings
     SET settings = $1::jsonb, updated_at = NOW()
     WHERE id = 1`,
    [JSON.stringify(settings)]
  );
}

async function tg(method, body = {}) {
  const response = await fetch(
    `https://api.telegram.org/bot${config.botToken}/${method}`,
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(35000)
    }
  );

  const data = await response.json().catch(() => ({}));

  if (!response.ok || data.ok !== true) {
    const error = new Error(
      `Telegram ${method}: HTTP ${response.status} ${JSON.stringify(data).slice(0, 400)}`
    );
    error.retryAfter = Number(data?.parameters?.retry_after || 0);
    throw error;
  }

  return data.result;
}

async function adminMessage(chatId, text, keyboard) {
  const body = { chat_id: chatId, text };
  if (keyboard) body.reply_markup = { inline_keyboard: keyboard };
  return tg("sendMessage", body);
}

function adminKeyboard() {
  return [
    [
      { text: "📢 Channel", callback_data: "channel" },
      { text: "💵 Amount", callback_data: "amount" }
    ],
    [
      { text: "🎯 Hourly Target", callback_data: "target" },
      { text: "⏱ Delay", callback_data: "delay" }
    ],
    [
      { text: "🔘 Button Text", callback_data: "button" },
      { text: "🔗 Button URL", callback_data: "buttonurl" }
    ],
    [
      { text: settings.paused ? "▶️ Resume" : "⏸ Pause", callback_data: "pause" },
      { text: "📊 Hourly Report", callback_data: "report" }
    ],
    [{ text: "💾 Save Settings", callback_data: "save" }]
  ];
}

async function getHourlyReport() {
  const r = await pool.query(`
    WITH h AS (
      SELECT date_trunc('hour', NOW()) AS start_time
    )
    SELECT
      (SELECT start_time FROM h) AS hour_start,
      (
        SELECT COUNT(*)::int FROM scanner_transfers t, h
        WHERE t.posted_at >= h.start_time
          AND t.posted_at < h.start_time + INTERVAL '1 hour'
      ) AS successful_posts,
      (
        SELECT COUNT(*)::int FROM scanner_transfers t, h
        WHERE t.block_timestamp >= EXTRACT(EPOCH FROM h.start_time) * 1000
          AND t.block_timestamp < EXTRACT(EPOCH FROM (h.start_time + INTERVAL '1 hour')) * 1000
          AND t.amount_usdt BETWEEN $1 AND $2
      ) AS eligible_transfers,
      (
        SELECT COALESCE(SUM(t.amount_usdt), 0)::numeric
        FROM scanner_transfers t, h
        WHERE t.block_timestamp >= EXTRACT(EPOCH FROM h.start_time) * 1000
          AND t.block_timestamp < EXTRACT(EPOCH FROM (h.start_time + INTERVAL '1 hour')) * 1000
          AND t.amount_usdt BETWEEN $1 AND $2
      ) AS total_usdt
  `, [settings.min, settings.max]);

  return r.rows[0];
}

async function adminPanelText() {
  const r = await getHourlyReport();

  return [
    "🛠 TRC20 SCANNER ADMIN",
    "",
    `Channel: ${settings.channel}`,
    `Amount: ${settings.min}–${settings.max} USDT`,
    `Hourly target: ${settings.hourlyTarget} posts`,
    `Delay: ${settings.minDelay}–${settings.maxDelay} sec`,
    `Maximum payment age: ${settings.maxAgeSeconds} sec`,
    `Button: ${settings.buttonText || "(disabled)"}`,
    `Button URL: ${settings.buttonUrl || "(not set)"}`,
    `Posting: ${settings.paused ? "PAUSED" : "ACTIVE"}`,
    "",
    "📊 CURRENT HOUR",
    `Successful posts: ${r.successful_posts}`,
    `Eligible transfers: ${r.eligible_transfers}`,
    `Total eligible USDT: ${r.total_usdt}`,
    `Hour: ${new Date(r.hour_start).toISOString()}`
  ].join("\n");
}

async function openAdmin(chatId) {
  await adminMessage(chatId, await adminPanelText(), adminKeyboard());
}

async function handleAdminText(message) {
  if (Number(message?.from?.id) !== ADMIN_ID) return;
  if (message.chat?.type !== "private") return;

  const chatId = message.chat.id;
  const text = String(message.text || "").trim();

  if (text === "/start" || text === "/admin") {
    adminInput.delete(chatId);
    await openAdmin(chatId);
    return;
  }

  const field = adminInput.get(chatId);
  if (!field || !text) return;
  adminInput.delete(chatId);

  if (field === "channel") {
    if (!/^(@[A-Za-z0-9_]{5,}|-100\d+)$/.test(text)) {
      await adminMessage(chatId, "Invalid channel. Use @channelusername or numeric channel ID.");
      return;
    }
    settings.channel = text;
  } else if (field === "amount") {
    const m = text.match(/^(\d+)\s+(\d+)$/);
    if (!m) {
      await adminMessage(chatId, "Send min and max, e.g. 30 350");
      return;
    }
    const min = Number(m[1]);
    const max = Number(m[2]);
    if (min < 0 || max < min || max > 1000000000) {
      await adminMessage(chatId, "Invalid range.");
      return;
    }
    settings.min = min;
    settings.max = max;
  } else if (field === "target") {
    const n = Number(text);
    if (!Number.isInteger(n) || n < 1 || n > 100) {
      await adminMessage(chatId, "Target must be 1–100.");
      return;
    }
    settings.hourlyTarget = n;
  } else if (field === "delay") {
    const m = text.match(/^(\d+)\s+(\d+)$/);
    if (!m) {
      await adminMessage(chatId, "Send min and max seconds, e.g. 75 150");
      return;
    }
    const min = Number(m[1]);
    const max = Number(m[2]);
    if (min < 5 || max < min || max > 3600) {
      await adminMessage(chatId, "Delay must be 5–3600 seconds.");
      return;
    }
    settings.minDelay = min;
    settings.maxDelay = max;
  } else if (field === "button") {
    if (text.length > 50) {
      await adminMessage(chatId, "Button text maximum is 50 characters.");
      return;
    }
    settings.buttonText = text;
  } else if (field === "buttonurl") {
    try {
      const u = new URL(text);
      if (!["https:", "http:"].includes(u.protocol)) throw new Error();
    } catch {
      await adminMessage(chatId, "Enter a valid http:// or https:// URL.");
      return;
    }
    settings.buttonUrl = text;
  }

  await saveSettings();
  await openAdmin(chatId);
}

async function handleCallback(q) {
  if (Number(q?.from?.id) !== ADMIN_ID || q.message?.chat?.type !== "private") {
    await tg("answerCallbackQuery", {
      callback_query_id: q.id,
      text: "Not authorized",
      show_alert: true
    }).catch(() => {});
    return;
  }

  await tg("answerCallbackQuery", { callback_query_id: q.id }).catch(() => {});

  const chatId = q.message.chat.id;
  const action = q.data;

  if (action === "pause") {
    settings.paused = !settings.paused;
    await saveSettings();
    await openAdmin(chatId);
    return;
  }

  if (action === "save") {
    await saveSettings();
    await adminMessage(chatId, "✅ Settings saved in PostgreSQL.");
    await openAdmin(chatId);
    return;
  }

  if (action === "report") {
    const r = await getHourlyReport();
    await adminMessage(chatId, [
      "📊 HOURLY REPORT",
      "",
      `Successful posts: ${r.successful_posts}`,
      `Eligible transfers: ${r.eligible_transfers}`,
      `Total eligible USDT: ${r.total_usdt}`,
      `Hour: ${new Date(r.hour_start).toISOString()}`
    ].join("\n"), adminKeyboard());
    return;
  }

  const prompts = {
    channel: ["channel", "Send channel username, e.g. @mychannel"],
    amount: ["amount", "Send min and max USDT, e.g. 30 350"],
    target: ["target", "Send hourly post target from 1 to 100"],
    delay: ["delay", "Send min and max delay in seconds, e.g. 75 150"],
    button: ["button", "Send the inline button text"],
    buttonurl: ["buttonurl", "Send button URL beginning with https://"]
  };

  if (prompts[action]) {
    adminInput.set(chatId, prompts[action][0]);
    await adminMessage(chatId, prompts[action][1]);
  }
}

async function adminLoop() {
  // This bot token must not be used by another active getUpdates poller.
  while (true) {
    try {
      const updates = await tg("getUpdates", {
        offset: adminOffset,
        timeout: 25,
        allowed_updates: ["message", "callback_query"]
      });

      for (const u of updates || []) {
        adminOffset = u.update_id + 1;
        if (u.callback_query) {
          await handleCallback(u.callback_query).catch(e =>
            log("warn", "Admin callback error", { error: String(e.message || e) })
          );
        } else if (u.message) {
          await handleAdminText(u.message).catch(e =>
            log("warn", "Admin message error", { error: String(e.message || e) })
          );
        }
      }
    } catch (e) {
      log("warn", "Admin polling retry", { error: String(e.message || e).slice(0, 300) });
      await sleep(e.retryAfter ? e.retryAfter * 1000 + 1000 : 5000);
    }
  }
}

function getAmountWhole(tx) {
  const token = tx?.tokenInfo || {};
  const decimals = Number(
    token.tokenDecimal ?? token.token_decimal ?? tx.tokenDecimal ?? USDT_DECIMALS
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
  if (raw % scale !== 0n) return null;

  const whole = raw / scale;
  if (whole < BigInt(settings.min) || whole > BigInt(settings.max)) return null;
  return whole;
}

function transferFields(tx) {
  return {
    txid: tx?.transaction_id || tx?.transactionId || tx?.hash || "",
    from: tx?.from_address || tx?.fromAddress || "",
    to: tx?.to_address || tx?.toAddress || "",
    timestamp: Number(tx?.block_ts ?? tx?.block_timestamp ?? tx?.timestamp ?? 0),
    confirmed: isTrue(tx?.confirmed),
    contractRet: String(tx?.contractRet || "").toUpperCase(),
    finalResult: String(tx?.finalResult || "").toUpperCase(),
    reverted: isTrue(tx?.revert),
    contract: tx?.contract_address || tx?.contractAddress || tx?.tokenInfo?.tokenId || "",
    eventType: String(tx?.event_type || tx?.eventType || "Transfer")
  };
}

function eligibleTransfer(tx) {
  const f = transferFields(tx);
  const amount = getAmountWhole(tx);

  if (!f.txid || !f.from || !f.to || amount === null) return null;
  if (f.contract && f.contract !== USDT_CONTRACT) return null;
  if (f.eventType.toLowerCase() !== "transfer") return null;
  if (!f.confirmed || f.reverted) return null;
  if (f.contractRet !== "SUCCESS" || f.finalResult !== "SUCCESS") return null;

  if (
    config.excludedAddresses.has(f.from.toLowerCase()) ||
    config.excludedAddresses.has(f.to.toLowerCase())
  ) return null;

  // Ignore old, future-dated or malformed timestamps.
  const age = Date.now() - f.timestamp;
  if (!f.timestamp || age < 0 || age > settings.maxAgeSeconds * 1000) return null;

  return { ...f, amount };
}

async function requestJson(url, headers = {}, attempts = 5) {
  let lastError;

  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      const wait = Math.max(0, 1100 - (Date.now() - lastApiRequest));
      if (wait) await sleep(wait);
      lastApiRequest = Date.now();

      const response = await fetch(url, {
        headers,
        signal: AbortSignal.timeout(20000)
      });
      const body = await response.text();

      if (!response.ok) {
        const error = new Error(`HTTP ${response.status}: ${body.slice(0, 400)}`);
        error.status = response.status;
        if ([400, 401, 403, 404].includes(response.status)) throw error;
        throw error;
      }

      return JSON.parse(body);
    } catch (error) {
      lastError = error;
      log("warn", "TronScan request failed", {
        attempt, status: error.status || null,
        error: String(error.message || error).slice(0, 300)
      });

      if (error.status && [400, 401, 403, 404].includes(error.status)) break;
      if (attempt < attempts) {
        await sleep(Math.min(1500 * 2 ** (attempt - 1), 15000));
      }
    }
  }

  throw lastError || new Error("TronScan request failed");
}

async function fetchWindowPage(startMs, endMs, offset) {
  const url = new URL("/api/token_trc20/transfers", API_BASE);
  url.searchParams.set("contract_address", USDT_CONTRACT);
  url.searchParams.set("confirm", "0");
  url.searchParams.set("start_timestamp", String(startMs));
  url.searchParams.set("end_timestamp", String(endMs));
  url.searchParams.set("start", String(offset));
  url.searchParams.set("limit", String(config.pageSize));

  const headers = {};
  if (config.apiKey) headers["TRON-PRO-API-KEY"] = config.apiKey;
  return requestJson(url, headers);
}

async function getCheckpoint() {
  const r = await pool.query(
    "SELECT checkpoint_ms FROM scanner_state WHERE id = $1",
    ["trc20-usdt"]
  );
  if (!r.rowCount) throw new Error("Scanner checkpoint missing");
  return Number(r.rows[0].checkpoint_ms);
}

async function saveCheckpoint(value) {
  await pool.query(
    `UPDATE scanner_state SET checkpoint_ms = $2, updated_at = NOW()
     WHERE id = $1`,
    ["trc20-usdt", String(value)]
  );
}

async function scanWindow(startMs, endMs) {
  let offset = 0;
  let pages = 0;

  while (pages < config.maxPages) {
    const data = await fetchWindowPage(startMs, endMs, offset);
    const rows = Array.isArray(data.token_transfers)
      ? data.token_transfers
      : Array.isArray(data.data) ? data.data : [];

    pages++;

    for (const tx of rows) {
      const candidate = eligibleTransfer(tx);
      if (!candidate) continue;

      await pool.query(
        `INSERT INTO scanner_transfers
          (txid, amount_usdt, from_address, to_address, block_timestamp)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (txid) DO NOTHING`,
        [
          candidate.txid, String(candidate.amount), candidate.from,
          candidate.to, candidate.timestamp
        ]
      );
    }

    if (rows.length < config.pageSize) {
      log("info", "Scanned time window", {
        start: new Date(startMs).toISOString(),
        end: new Date(endMs).toISOString(),
        pages, fetched: offset + rows.length, complete: true
      });
      return true;
    }

    offset += rows.length;
    if (offset >= 10000) break;
  }

  log("error", "Scan window incomplete", {
    start: new Date(startMs).toISOString(),
    end: new Date(endMs).toISOString()
  });
  return false;
}

function postText(row) {
  return [
    "🟢 <b>New USDT Transfer</b>",
    "",
    `💵 Amount: <b>${escapeHtml(String(row.amount_usdt))} USDT</b>`,
    "🌐 Network: TRON (TRC20)",
    `📤 From: <code>${escapeHtml(row.from_address)}</code>`,
    `📥 To: <code>${escapeHtml(row.to_address)}</code>`,
    `🔗 <a href="https://tronscan.org/#/transaction/${escapeHtml(row.txid)}">View transaction</a>`
  ].join("\n");
}

async function sendTransfer(row) {
  if (!settings.channel) throw new Error("Channel is not configured");

  const body = {
    chat_id: settings.channel,
    text: postText(row),
    parse_mode: "HTML",
    disable_web_page_preview: true
  };

  if (settings.buttonText && settings.buttonUrl) {
    body.reply_markup = {
      inline_keyboard: [[{
        text: settings.buttonText,
        url: settings.buttonUrl
      }]]
    };
  }

  await tg("sendMessage", body);
}

async function postingWorker() {
  while (true) {
    try {
      if (settings.paused) {
        await sleep(3000);
        continue;
      }

      const hour = await pool.query(
        "SELECT date_trunc('hour', NOW()) AS hour_start"
      );
      const hourStart = hour.rows[0].hour_start;

      await pool.query(
        `INSERT INTO scanner_hourly_limits (hour_start, target_count)
         VALUES ($1, $2) ON CONFLICT (hour_start) DO NOTHING`,
        [hourStart, settings.hourlyTarget]
      );

      const count = await pool.query(
        `SELECT COUNT(*)::int AS sent
         FROM scanner_transfers
         WHERE posted_at >= $1
           AND posted_at < $1::timestamptz + INTERVAL '1 hour'`,
        [hourStart]
      );

      if (count.rows[0].sent >= settings.hourlyTarget) {
        const nextHour = new Date(new Date(hourStart).getTime() + 3600000);
        await sleep(Math.max(1000, nextHour.getTime() - Date.now() + 1000));
        continue;
      }

      // Do not send any payment that has aged beyond the configured limit.
      await pool.query(
        `UPDATE scanner_transfers
         SET posted_at = NOW(),
             last_error = 'Skipped: payment older than latest-only limit'
         WHERE posted_at IS NULL
           AND (
             block_timestamp IS NULL
             OR block_timestamp < $1
             OR block_timestamp > $2
           )`,
        [
          Date.now() - settings.maxAgeSeconds * 1000,
          Date.now()
        ]
      );

      const pending = await pool.query(
        `SELECT txid, amount_usdt, from_address, to_address, block_timestamp
         FROM scanner_transfers
         WHERE posted_at IS NULL
           AND block_timestamp >= $1
           AND block_timestamp <= $2
           AND amount_usdt BETWEEN $3 AND $4
         ORDER BY block_timestamp DESC, created_at DESC
         LIMIT 1`,
        [
          Date.now() - settings.maxAgeSeconds * 1000,
          Date.now(),
          settings.min,
          settings.max
        ]
      );

      if (!pending.rows.length) {
        await sleep(2000);
        continue;
      }

      const row = pending.rows[0];

      // Re-check age immediately before sending.
      if (Date.now() - Number(row.block_timestamp) > settings.maxAgeSeconds * 1000) {
        await pool.query(
          `UPDATE scanner_transfers
           SET posted_at = NOW(),
               last_error = 'Skipped: expired before send'
           WHERE txid = $1 AND posted_at IS NULL`,
          [row.txid]
        );
        continue;
      }

      await sendTransfer(row);

      await pool.query(
        `UPDATE scanner_transfers
         SET posted_at = NOW(), last_error = NULL
         WHERE txid = $1 AND posted_at IS NULL`,
        [row.txid]
      );

      log("info", "Telegram post sent", {
        txid: row.txid,
        amount: row.amount_usdt,
        hourlyTarget: settings.hourlyTarget,
        hourlySent: count.rows[0].sent + 1
      });

      const delay = settings.minDelay +
        Math.floor(Math.random() * (settings.maxDelay - settings.minDelay + 1));
      await sleep(delay * 1000);
    } catch (error) {
      const retry = Number(error?.retryAfter || 0);
      log("warn", "Posting worker retry", {
        error: String(error.message || error).slice(0, 400),
        retryAfter: retry
      });
      await sleep(retry ? retry * 1000 + 1000 : 10000);
    }
  }
}

async function runCycle() {
  if (!postingWorkerStarted) {
    postingWorkerStarted = true;
    void postingWorker();
  }

  let checkpoint = await getCheckpoint();
  const safeEnd = Date.now() - 30000;

  while (checkpoint < safeEnd) {
    const end = Math.min(
      safeEnd,
      checkpoint + config.windowSeconds * 1000
    );

    const complete = await scanWindow(checkpoint, end);
    if (!complete) return;

    await saveCheckpoint(end);
    checkpoint = end;
  }
}

async function main() {
  await pool.query("SELECT 1");
  await initDb();

  log("info", "Scanner starting", {
    min: settings.min,
    max: settings.max,
    hourlyTarget: settings.hourlyTarget,
    maxAgeSeconds: settings.maxAgeSeconds
  });

  void adminLoop();

  while (true) {
    try {
      await runCycle();
    } catch (error) {
      log("error", "Scanner cycle failed", {
        error: String(error.stack || error).slice(0, 1000)
      });
    }

    await sleep(config.pollSeconds * 1000);
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

main().catch(async error => {
  log("error", "Fatal startup error", {
    error: String(error.stack || error).slice(0, 1200)
  });
  await pool.end().catch(() => {});
  process.exit(1);
});
