/** Per-test adaptive settings (maps to `adaptive_config` + test columns in Section 1). */
export interface AdaptiveConfig {
  /** Damping factor α (default 0.3). */
  learningRate: number;
  /** Maximum single-step ability change (default 0.75). */
  maxStepSize: number;
  minAbility: number;
  maxAbility: number;
  /** Floor on item information in the update denominator (default 0.05). */
  minInformation: number;
  /** Early stop when SE falls below this (default 0.20). */
  terminationSeThreshold: number;
  maxQuestions: number;
  /** Top-N randomization pool size. */
  randomizationN: number;
  /** Initial θ when a session starts (default 0). */
  startingAbility: number;
}

export const DEFAULT_ADAPTIVE_CONFIG: AdaptiveConfig = {
  learningRate: 0.3,
  maxStepSize: 0.75,
  minAbility: -4,
  maxAbility: 4,
  minInformation: 0.05,
  terminationSeThreshold: 0.2,
  maxQuestions: 40,
  randomizationN: 20,
  startingAbility: 0,
};

/** One recorded response used for SE / information accumulation. */
export interface AnsweredItem {
  questionId: string;
  /** Question difficulty b on the IRT ability scale. */
  difficulty: number;
  correct: boolean;
}

/** In-memory adaptive state for a test session (server-side source of truth). */
export interface AdaptiveState {
  theta: number;
  answered: AnsweredItem[];
}

export type QuestionStatus = "published" | "draft" | "retired";

/** Minimal question metadata required for selection (no DB / HTTP). */
export interface QuestionCandidate {
  questionId: string;
  difficulty: number;
  status: QuestionStatus;
  exposureCount: number;
  maxExposure: number;
}

export type TerminationReason =
  | "max_questions"
  | "confidence_threshold"
  | "no_eligible_questions"
  | null;

export interface TerminationResult {
  shouldTerminate: boolean;
  reason: TerminationReason;
}

export interface AbilityUpdateResult {
  thetaBefore: number;
  thetaAfter: number;
  predictedProbability: number;
  step: number;
}

export interface ProcessAnswerResult {
  state: AdaptiveState;
  ability: AbilityUpdateResult;
  standardError: number;
  confidenceInterval95: { lower: number; upper: number };
  questionsAnswered: number;
  termination: TerminationResult;
  nextQuestion: QuestionCandidate | null;
}

/** Product-scale difficulty (1 = easiest … 10 = hardest) → IRT b. */
export function productDifficultyToIrt(
  productDifficulty: number,
  config: Pick<AdaptiveConfig, "minAbility" | "maxAbility"> = DEFAULT_ADAPTIVE_CONFIG,
): number {
  const minProduct = 1;
  const maxProduct = 10;
  const t = (productDifficulty - minProduct) / (maxProduct - minProduct);
  return config.minAbility + t * (config.maxAbility - config.minAbility);
}

/** IRT b → product-scale difficulty (for display / bank tooling). */
export function irtToProductDifficulty(
  irtDifficulty: number,
  config: Pick<AdaptiveConfig, "minAbility" | "maxAbility"> = DEFAULT_ADAPTIVE_CONFIG,
): number {
  const t =
    (irtDifficulty - config.minAbility) / (config.maxAbility - config.minAbility);
  return 1 + t * 9;
}
