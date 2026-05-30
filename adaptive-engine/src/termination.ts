import type { AdaptiveConfig, TerminationResult } from "./types.js";

/**
 * Test may end when max questions reached or SE < threshold.
 * @see README — Test Termination
 */
export function checkTermination(
  questionsAnswered: number,
  se: number,
  config: AdaptiveConfig,
): TerminationResult {
  if (questionsAnswered > 0 && se < config.terminationSeThreshold) {
    return { shouldTerminate: true, reason: "confidence_threshold" };
  }
  if (questionsAnswered >= config.maxQuestions) {
    return { shouldTerminate: true, reason: "max_questions" };
  }
  return { shouldTerminate: false, reason: null };
}
