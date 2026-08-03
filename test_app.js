const assert = require('node:assert/strict');
const { recalcKR, impliedCagrKR } = require('./app.js');

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

console.log(JSON.stringify({ marketCagr, actualCalc, expectedCalc }, null, 2));
