import type { AdaptiveConfig, AdaptiveState, QuestionCandidate } from "./types.js";

export type Rng = () => number;

const defaultRng: Rng = () => Math.random();

function isEligible(
  question: QuestionCandidate,
  answeredIds: ReadonlySet<string>,
): boolean {
  if (answeredIds.has(question.questionId)) return false;
  if (question.status !== "published") return false;
  if (question.exposureCount >= question.maxExposure) return false;
  return true;
}

/**
 * Maximum-information selection with Top-N randomization.
 * argmin |θ − b| among eligible items, then random pick from top N.
 */
export function selectNextQuestion(
  candidates: readonly QuestionCandidate[],
  state: AdaptiveState,
  config: AdaptiveConfig,
  rng: Rng = defaultRng,
): QuestionCandidate | null {
  const answeredIds = new Set(state.answered.map((a) => a.questionId));

  const eligible = candidates
    .filter((q) => isEligible(q, answeredIds))
    .map((q) => ({
      question: q,
      distance: Math.abs(state.theta - q.difficulty),
    }))
    .sort((a, b) => a.distance - b.distance);

  if (eligible.length === 0) return null;

  const poolSize = Math.min(config.randomizationN, eligible.length);
  const pool = eligible.slice(0, poolSize).map((e) => e.question);
  const index = Math.floor(rng() * pool.length);
  return pool[index] ?? null;
}

/** First question: closest to starting ability / configured starting difficulty. */
export function selectFirstQuestion(
  candidates: readonly QuestionCandidate[],
  targetDifficulty: number,
  config: AdaptiveConfig,
  rng: Rng = defaultRng,
): QuestionCandidate | null {
  const sorted = candidates
    .filter((q) => q.status === "published" && q.exposureCount < q.maxExposure)
    .map((q) => ({
      question: q,
      distance: Math.abs(targetDifficulty - q.difficulty),
    }))
    .sort((a, b) => a.distance - b.distance);

  if (sorted.length === 0) return null;

  const poolSize = Math.min(config.randomizationN, sorted.length);
  const pool = sorted.slice(0, poolSize).map((e) => e.question);
  const index = Math.floor(rng() * pool.length);
  return pool[index] ?? null;
}

/** Expands search when the primary pool is exhausted (README edge case). */
export function selectNextQuestionWithFallback(
  candidates: readonly QuestionCandidate[],
  state: AdaptiveState,
  config: AdaptiveConfig,
  rng: Rng = defaultRng,
): QuestionCandidate | null {
  const primary = selectNextQuestion(candidates, state, config, rng);
  if (primary !== null) return primary;

  const answeredIds = new Set(state.answered.map((a) => a.questionId));
  const remaining = candidates.filter(
    (q) =>
      !answeredIds.has(q.questionId) &&
      q.status === "published" &&
      q.exposureCount < q.maxExposure,
  );
  if (remaining.length === 0) return null;

  const index = Math.floor(rng() * remaining.length);
  return remaining[index] ?? null;
}
