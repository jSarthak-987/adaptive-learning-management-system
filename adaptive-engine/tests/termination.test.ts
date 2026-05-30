import { describe, expect, it } from "vitest";
import { checkTermination } from "../src/termination.js";
import { processAnswer } from "../src/engine.js";
import { createInitialState } from "../src/engine.js";
import type { AdaptiveConfig, QuestionCandidate } from "../src/types.js";

const baseConfig: AdaptiveConfig = {
  learningRate: 0.3,
  maxStepSize: 0.75,
  minAbility: -4,
  maxAbility: 4,
  minInformation: 0.05,
  terminationSeThreshold: 0.2,
  maxQuestions: 5,
  randomizationN: 3,
  startingAbility: 0,
};

function pool(n: number): QuestionCandidate[] {
  return Array.from({ length: n }, (_, i) => ({
    questionId: `q${i}`,
    difficulty: 0,
    status: "published" as const,
    exposureCount: 0,
    maxExposure: 99999,
  }));
}

describe("termination logic", () => {
  it("stops when questions_answered >= max_questions", () => {
    expect(checkTermination(5, 0.5, baseConfig)).toEqual({
      shouldTerminate: true,
      reason: "max_questions",
    });
  });

  it("does not stop on SE alone before any answers", () => {
    expect(checkTermination(0, 0.01, baseConfig)).toEqual({
      shouldTerminate: false,
      reason: null,
    });
  });

  it("stops when SE < threshold before max_questions", () => {
    const config = { ...baseConfig, maxQuestions: 40 };
    expect(checkTermination(8, 0.18, config)).toEqual({
      shouldTerminate: true,
      reason: "confidence_threshold",
    });
  });

  it("processAnswer terminates early when SE drops below threshold", () => {
    const earlyStopConfig: AdaptiveConfig = {
      ...baseConfig,
      maxQuestions: 50,
      terminationSeThreshold: 10,
    };
    const questions = pool(10);
    const result = processAnswer(
      createInitialState(earlyStopConfig),
      questions[0]!.questionId,
      0,
      true,
      questions,
      earlyStopConfig,
      () => 0,
    );

    expect(result.termination).toEqual({
      shouldTerminate: true,
      reason: "confidence_threshold",
    });
    expect(result.questionsAnswered).toBe(1);
    expect(result.nextQuestion).toBeNull();
  });

  it("processAnswer ends session at max_questions with no next question", () => {
    let state = createInitialState(baseConfig);
    const questions = pool(10);
    let result;

    for (let i = 0; i < baseConfig.maxQuestions; i++) {
      result = processAnswer(
        state,
        questions[i]!.questionId,
        0,
        true,
        questions,
        baseConfig,
        () => 0,
      );
      state = result.state;
    }

    expect(result!.termination.reason).toBe("max_questions");
    expect(result!.termination.shouldTerminate).toBe(true);
    expect(result!.nextQuestion).toBeNull();
  });
});
