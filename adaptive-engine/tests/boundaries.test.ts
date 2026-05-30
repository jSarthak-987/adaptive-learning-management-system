import { describe, expect, it } from "vitest";
import { updateAbility } from "../src/ability.js";
import { selectNextQuestion } from "../src/selection.js";
import { productDifficultyToIrt } from "../src/types.js";
import { DEFAULT_ADAPTIVE_CONFIG } from "../src/types.js";
import type { AdaptiveState, QuestionCandidate } from "../src/types.js";

describe("boundary conditions — difficulty 1 and 10", () => {
  const bEasy = productDifficultyToIrt(1);
  const bHard = productDifficultyToIrt(10);

  it("selects easiest published item when θ is at minimum ability", () => {
    const state: AdaptiveState = { theta: -4, answered: [] };
    const candidates: QuestionCandidate[] = [
      {
        questionId: "easy",
        difficulty: bEasy,
        status: "published",
        exposureCount: 0,
        maxExposure: 5000,
      },
      {
        questionId: "hard",
        difficulty: bHard,
        status: "published",
        exposureCount: 0,
        maxExposure: 5000,
      },
    ];

    const next = selectNextQuestion(candidates, state, {
      ...DEFAULT_ADAPTIVE_CONFIG,
      randomizationN: 1,
    });
    expect(next?.questionId).toBe("easy");
  });

  it("selects hardest published item when θ is at maximum ability", () => {
    const state: AdaptiveState = { theta: 4, answered: [] };
    const candidates: QuestionCandidate[] = [
      {
        questionId: "easy",
        difficulty: bEasy,
        status: "published",
        exposureCount: 0,
        maxExposure: 5000,
      },
      {
        questionId: "hard",
        difficulty: bHard,
        status: "published",
        exposureCount: 0,
        maxExposure: 5000,
      },
    ];

    const next = selectNextQuestion(candidates, state, {
      ...DEFAULT_ADAPTIVE_CONFIG,
      randomizationN: 1,
    });
    expect(next?.questionId).toBe("hard");
  });

  it("does not exceed maxAbility after many correct answers on difficulty 10", () => {
    let theta = 3.5;
    for (let i = 0; i < 10; i++) {
      theta = updateAbility(theta, bHard, true, DEFAULT_ADAPTIVE_CONFIG).thetaAfter;
    }
    expect(theta).toBe(DEFAULT_ADAPTIVE_CONFIG.maxAbility);
  });

  it("does not go below minAbility after many incorrect answers on difficulty 1", () => {
    let theta = -3.5;
    for (let i = 0; i < 10; i++) {
      theta = updateAbility(theta, bEasy, false, DEFAULT_ADAPTIVE_CONFIG).thetaAfter;
    }
    expect(theta).toBe(DEFAULT_ADAPTIVE_CONFIG.minAbility);
  });
});
