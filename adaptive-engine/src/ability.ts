import type { AdaptiveConfig } from "./types.js";
import { raschProbability, itemInformation } from "./rasch.js";

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

/**
 * Production-safe damped ability update after one response.
 *
 * P = 1/(1+e^{-(θ−b)})
 * I = P(1−P)
 * raw = (u−P)/max(I, minInformation)
 * step = clamp(α·raw, −maxStep, +maxStep)
 * θ_new = clamp(θ_old + step, minAbility, maxAbility)
 */
export function updateAbility(
  theta: number,
  difficulty: number,
  correct: boolean,
  config: AdaptiveConfig,
): {
  thetaAfter: number;
  predictedProbability: number;
  step: number;
} {
  const u = correct ? 1 : 0;
  const p = raschProbability(theta, difficulty);
  const information = itemInformation(theta, difficulty);
  const safeI = Math.max(information, config.minInformation);
  const rawUpdate = (u - p) / safeI;
  const step = clamp(
    config.learningRate * rawUpdate,
    -config.maxStepSize,
    config.maxStepSize,
  );
  const thetaAfter = clamp(theta + step, config.minAbility, config.maxAbility);
  return { thetaAfter, predictedProbability: p, step };
}
