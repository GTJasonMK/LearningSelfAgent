import test from "node:test";
import assert from "node:assert/strict";

import { shouldFinalizePendingFinal } from "../src/renderer/pending_final.js";

test("shouldFinalizePendingFinal returns true when eval is final", () => {
  assert.equal(
    shouldFinalizePendingFinal({
      evalFinal: true,
      terminal: false,
      ageMs: 1,
      timeoutMs: 90_000,
    }),
    true
  );
});

test("shouldFinalizePendingFinal blocks non-terminal runs", () => {
  assert.equal(
    shouldFinalizePendingFinal({
      evalFinal: false,
      terminal: false,
      ageMs: 120_000,
      timeoutMs: 90_000,
    }),
    false
  );
});

test("shouldFinalizePendingFinal allows timeout fallback even when review exists", () => {
  assert.equal(
    shouldFinalizePendingFinal({
      evalFinal: false,
      hasReview: true,
      terminal: true,
      ageMs: 120_000,
      timeoutMs: 90_000,
    }),
    true
  );
});

test("shouldFinalizePendingFinal keeps waiting before timeout", () => {
  assert.equal(
    shouldFinalizePendingFinal({
      evalFinal: false,
      hasReview: true,
      terminal: true,
      ageMs: 30_000,
      timeoutMs: 90_000,
    }),
    false
  );
});

test("shouldFinalizePendingFinal finalizes immediately when no review exists", () => {
  assert.equal(
    shouldFinalizePendingFinal({
      evalFinal: false,
      hasReview: false,
      terminal: true,
      ageMs: 1,
      timeoutMs: 90_000,
    }),
    true
  );
});
