import { describe, expect, it } from "vitest";
import { processAnswer } from "../src/engine.js";
import { createInitialState } from "../src/engine.js";
import { selectNextQuestion } from "../src/selection.js";
import { DEFAULT_ADAPTIVE_CONFIG } from "../src/types.js";
import type { QuestionCandidate } from "../src/types.js";

const candidates: QuestionCandidate[] = [
  {
    questionId: "a",
    difficulty: 0,
    status: "published",
    exposureCount: 0,
    maxExposure: 5000,
  },
  {
    questionId: "b",
    difficulty: 0.1,
    status: "published",
    exposureCount: 0,
    maxExposure: 5000,
  },
  {
    questionId: "c",
    difficulty: 0.2,
    status: "published",
    exposureCount: 0,
    maxExposure: 5000,
  },
];

describe("no-repeat invariant", () => {
  it("never selects an already-answered question", () => {
    let state = createInitialState();
    const served = new Set<string>();

    for (let round = 0; round < 3; round++) {
      const next = selectNextQuestion(candidates, state, {
        ...DEFAULT_ADAPTIVE_CONFIG,
        randomizationN: 10,
      });
      expect(next).not.toBeNull();
      expect(served.has(next!.questionId)).toBe(false);

      served.add(next!.questionId);
      state = processAnswer(
        state,
        next!.questionId,
        next!.difficulty,
        true,
        candidates,
        DEFAULT_ADAPTIVE_CONFIG,
        () => 0,
      ).state;
    }

    expect(served.size).toBe(3);
  });

  it("rejects duplicate submission of the same questionId", () => {
    let state = createInitialState();
    state = processAnswer(state, "a", 0, true, candidates).state;

    expect(() => processAnswer(state, "a", 0, false, candidates)).toThrow(
      /already answered/i,
    );
  });

  it("returns null when all questions are exhausted", () => {
    let state = createInitialState();
    for (const q of candidates) {
      state = processAnswer(state, q.questionId, q.difficulty, true, candidates).state;
    }

    const next = selectNextQuestion(candidates, state, DEFAULT_ADAPTIVE_CONFIG);
    expect(next).toBeNull();
  });
});
