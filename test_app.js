const assert = require('node:assert/strict');
const {
  recalcKR,
  impliedCagrKR,
  normalizeMarketCagrOverrides,
  removeMarketCagrOverride,
  resolveMarketCagrKR,
} = require('./app.js');

const today = new Date(2026, 7, 3);
const fixture = {
  price: 108700,
  equity: 2580,
  shares: 22177360,
  baseDate: '2025.12',
  requiredReturnPct: 10,
};

const marketCagr = impliedCagrKR(
  fixture.price,
  fixture.equity,
  fixture.shares,
  fixture.baseDate,
  fixture.requiredReturnPct,
  today,
);
const marketCalc = recalcKR(
  fixture.price,
  fixture.equity,
  marketCagr,
  fixture.shares,
  fixture.baseDate,
  fixture.requiredReturnPct,
  today,
);
assert.ok(marketCalc);
assert.ok(Math.abs(marketCalc.pbgr - 1) < 1e-10);
assert.equal(marketCalc.fair_price, fixture.price);

const actualCalc = recalcKR(
  fixture.price,
  fixture.equity,
  7.51,
  fixture.shares,
  fixture.baseDate,
  fixture.requiredReturnPct,
  today,
);
const expectedCalc = recalcKR(
  fixture.price,
  fixture.equity,
  7.18,
  fixture.shares,
  fixture.baseDate,
  fixture.requiredReturnPct,
  today,
);
assert.ok(actualCalc);
assert.ok(expectedCalc);
assert.notEqual(actualCalc.fair_price, expectedCalc.fair_price);

const asset = {
  ticker: '058610',
  price: fixture.price,
  equity_y0_100m: fixture.equity,
  shares: fixture.shares,
  base_date: fixture.baseDate,
};
const noOverrides = normalizeMarketCagrOverrides({});
const defaultMarketCagr = resolveMarketCagrKR(
  asset,
  fixture.requiredReturnPct,
  noOverrides,
  today,
);
assert.ok(Math.abs(defaultMarketCagr - marketCagr) < 1e-10);
const defaultCalc = recalcKR(
  asset.price,
  asset.equity_y0_100m,
  defaultMarketCagr,
  asset.shares,
  asset.base_date,
  fixture.requiredReturnPct,
  today,
);
assert.ok(Math.abs(defaultCalc.pbgr - 1) < 1e-10);

const editedOverrides = normalizeMarketCagrOverrides({
  '058610': '15.25',
  invalid: 'not-a-number',
  impossible: -100,
});
assert.deepEqual(editedOverrides, { '058610': 15.25 });
const resetOverrides = removeMarketCagrOverride(
  { '058610': 15.25, '005930': 9.4 },
  '058610',
);
assert.deepEqual(resetOverrides, { '005930': 9.4 });
assert.equal(
  resolveMarketCagrKR(asset, fixture.requiredReturnPct, resetOverrides, today),
  defaultMarketCagr,
);
const editedMarketCagr = resolveMarketCagrKR(
  asset,
  fixture.requiredReturnPct,
  editedOverrides,
  today,
);
assert.equal(editedMarketCagr, 15.25);
const editedCalc = recalcKR(
  asset.price,
  asset.equity_y0_100m,
  editedMarketCagr,
  asset.shares,
  asset.base_date,
  fixture.requiredReturnPct,
  today,
);
assert.ok(editedCalc);
assert.notEqual(editedCalc.pbgr, defaultCalc.pbgr);
assert.notEqual((1 / editedCalc.pbgr) - 1, 0);

console.log(JSON.stringify({ marketCagr, actualCalc, expectedCalc, defaultCalc, editedCalc }, null, 2));
