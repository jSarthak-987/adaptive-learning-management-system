/**
 * Rasch 1PL model: P(correct | θ, b) and item information I(θ).
 * @see README Section 1 — Adaptive Algorithm Design
 */

/** Probability of a correct response under the Rasch model. */
export function raschProbability(theta: number, difficulty: number): number {
  const exponent = -(theta - difficulty);
  if (exponent > 700) return 0;
  if (exponent < -700) return 1;
  return 1 / (1 + Math.exp(exponent));
}

/** Item information I(θ) = P(1 − P) at the learner's current ability. */
export function itemInformation(theta: number, difficulty: number): number {
  const p = raschProbability(theta, difficulty);
  return p * (1 - p);
}

/** Sum of item information across all answered items at current θ. */
export function totalTestInformation(
  answered: ReadonlyArray<{ difficulty: number }>,
  theta: number,
): number {
  return answered.reduce(
    (sum, item) => sum + itemInformation(theta, item.difficulty),
    0,
  );
}

/** Standard error SE(θ) = 1 / √(I_total). Returns +∞ when no information yet. */
export function standardError(
  answered: ReadonlyArray<{ difficulty: number }>,
  theta: number,
): number {
  const information = totalTestInformation(answered, theta);
  if (information <= 0) return Number.POSITIVE_INFINITY;
  return 1 / Math.sqrt(information);
}

const Z_95 = 1.96;

/** 95% confidence interval: θ ± 1.96 × SE. */
export function confidenceInterval95(
  theta: number,
  se: number,
): { lower: number; upper: number } {
  const margin = Z_95 * se;
  return { lower: theta - margin, upper: theta + margin };
}
