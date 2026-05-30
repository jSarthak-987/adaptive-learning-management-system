import { describe, expect, it } from "vitest";
import { processAnswer } from "../src/engine.js";
import { createInitialState } from "../src/engine.js";
import type { QuestionCandidate } from "../src/types.js";
import { productDifficultyToIrt } from "../src/types.js";

function makePool(
  count: number,
  difficulty: number,
  idPrefix = "q",
): QuestionCandidate[] {
  return Array.from({ length: count }, (_, i) => ({
    questionId: `${idPrefix}-${i}`,
    difficulty,
    status: "published" as const,
    exposureCount: 0,
    maxExposure: 100_000,
  }));
}

describe("convergence on a known ability level", () => {
  it("θ stabilizes near b when responses match Rasch expectation at that ability", () => {
    const targetB = 1.5;
    let state = createInitialState({ startingAbility: targetB });
    const pool = makePool(40, targetB);

    // At θ ≈ b, P(correct) ≈ 0.5 — alternate correct/incorrect to stay near b
    for (let i = 0; i < 40; i++) {
      const q = pool[i]!;
      const correct = i % 2 === 0;
      state = processAnswer(state, q.questionId, q.difficulty, correct, pool).state;
    }

    expect(Math.abs(state.theta - targetB)).toBeLessThan(0.75);
  });

  it("θ moves toward harder items when learner answers correctly at increasing difficulty", () => {
    const targetB = 1.5;
    let state = createInitialState();
    const pool = makePool(20, targetB);

    for (let i = 0; i < 15; i++) {
      const q = pool[i]!;
      state = processAnswer(state, q.questionId, q.difficulty, true, pool).state;
    }

    expect(state.theta).toBeGreaterThan(0.5);
    expect(state.theta).toBeLessThanOrEqual(4);
  });

  it("θ decreases toward easy items when answers are mostly incorrect at hard difficulty", () => {
    const hardB = 2.5;
    let state = createInitialState({ startingAbility: 0 });
    const pool = makePool(20, hardB);

    for (let i = 0; i < 15; i++) {
      const q = pool[i]!;
      state = processAnswer(state, q.questionId, q.difficulty, false, pool).state;
    }

    expect(state.theta).toBeLessThan(0);
  });
});

describe("product difficulty scale mapping", () => {
  it("maps difficulty 1 and 10 to ability bounds", () => {
    expect(productDifficultyToIrt(1)).toBe(-4);
    expect(productDifficultyToIrt(10)).toBe(4);
  });
});
