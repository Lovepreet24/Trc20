import test from "node:test";
import assert from "node:assert/strict";

// Pure test helper matching scanner's integer-amount rule.
function wholeUsdt(raw, decimals = 6, min = 30n, max = 350n) {
  if (decimals !== 6) return null;
  let value;
  try { value = BigInt(String(raw)); } catch { return null; }
  const scale = 10n ** BigInt(decimals);
  if (value < 0n || value % scale !== 0n) return null;
  const whole = value / scale;
  return whole >= min && whole <= max ? whole.toString() : null;
}

test("accepts min/max whole USDT", () => {
  assert.equal(wholeUsdt("30000000"), "30");
  assert.equal(wholeUsdt("350000000"), "350");
});

test("rejects decimals and out-of-range amounts", () => {
  assert.equal(wholeUsdt("30500000"), null);
  assert.equal(wholeUsdt("29999999"), null);
  assert.equal(wholeUsdt("350000001"), null);
});
