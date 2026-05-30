import { updateAbility } from "./ability.js";
import { confidenceInterval95, standardError } from "./rasch.js";
import {
  selectFirstQuestion,
  selectNextQuestionWithFallback,
  type Rng,
} from "./selection.js";
import { checkTermination } from "./termination.js";
import type {
  AdaptiveConfig,
  AdaptiveState,
  AnsweredItem,
  ProcessAnswerResult,
  QuestionCandidate,
} from "./types.js";
import { DEFAULT_ADAPTIVE_CONFIG } from "./types.js";

export function createInitialState(
  config: AdaptiveConfig = DEFAULT_ADAPTIVE_CONFIG,
): AdaptiveState {
  return { theta: config.startingAbility, answered: [] };
}

/**
 * Record one answer, update θ and SE, evaluate termination, pick next item.
 * Pure function — caller persists state and serves HTTP/DB elsewhere.
 */
export function processAnswer(
  state: AdaptiveState,
  questionId: string,
  difficulty: number,
  correct: boolean,
  candidates: readonly QuestionCandidate[],
  config: AdaptiveConfig = DEFAULT_ADAPTIVE_CONFIG,
  rng?: Rng,
): ProcessAnswerResult {
  if (state.answered.some((a) => a.questionId === questionId)) {
    throw new Error(
      `Question ${questionId} was already answered (no-repeat invariant)`,
    );
  }

  const thetaBefore = state.theta;
  const { thetaAfter, predictedProbability, step } = updateAbility(
    thetaBefore,
    difficulty,
    correct,
    config,
  );

  const newItem: AnsweredItem = { questionId, difficulty, correct };
  const nextState: AdaptiveState = {
    theta: thetaAfter,
    answered: [...state.answered, newItem],
  };

  const se = standardError(nextState.answered, nextState.theta);
  const questionsAnswered = nextState.answered.length;
  const termination = checkTermination(questionsAnswered, se, config);

  let nextQuestion: QuestionCandidate | null = null;
  if (!termination.shouldTerminate) {
    nextQuestion = selectNextQuestionWithFallback(candidates, nextState, config, rng);
    if (nextQuestion === null) {
      return {
        state: nextState,
        ability: {
          thetaBefore,
          thetaAfter,
          predictedProbability,
          step,
        },
        standardError: se,
        confidenceInterval95: confidenceInterval95(nextState.theta, se),
        questionsAnswered,
        termination: { shouldTerminate: true, reason: "no_eligible_questions" },
        nextQuestion: null,
      };
    }
  }

  return {
    state: nextState,
    ability: {
      thetaBefore,
      thetaAfter,
      predictedProbability,
      step,
    },
    standardError: se,
    confidenceInterval95: confidenceInterval95(nextState.theta, se),
    questionsAnswered,
    termination,
    nextQuestion,
  };
}

export function startSession(
  candidates: readonly QuestionCandidate[],
  startingDifficulty: number,
  config: AdaptiveConfig = DEFAULT_ADAPTIVE_CONFIG,
  rng?: Rng,
): { state: AdaptiveState; firstQuestion: QuestionCandidate | null } {
  const state = createInitialState(config);
  const firstQuestion = selectFirstQuestion(
    candidates,
    startingDifficulty,
    config,
    rng,
  );
  return { state, firstQuestion };
}
