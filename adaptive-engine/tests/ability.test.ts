import { describe, expect, it } from "vitest";
import { updateAbility } from "../src/ability.js";
import { DEFAULT_ADAPTIVE_CONFIG } from "../src/types.js";

describe("updateAbility", () => {
  it("applies damping so one correct medium item does not jump to θ=2", () => {
    const { thetaAfter } = updateAbility(0, 0, true, DEFAULT_ADAPTIVE_CONFIG);
    expect(thetaAfter).toBeCloseTo(0.6, 5);
  });

  it("clamps step to maxStepSize", () => {
    const config = { ...DEFAULT_ADAPTIVE_CONFIG, learningRate: 1 };
    const { step, thetaAfter } = updateAbility(0, -4, true, config);
    expect(Math.abs(step)).toBeLessThanOrEqual(config.maxStepSize);
    expect(thetaAfter - 0).toBeLessThanOrEqual(config.maxStepSize);
  });

  it("never exceeds maxAbility", () => {
    let theta = 3.5;
    for (let i = 0; i < 30; i++) {
      theta = updateAbility(theta, 3.5, true, DEFAULT_ADAPTIVE_CONFIG).thetaAfter;
    }
    expect(theta).toBeLessThanOrEqual(DEFAULT_ADAPTIVE_CONFIG.maxAbility);
  });

  it("never goes below minAbility after sustained incorrect answers at matched difficulty", () => {
    let theta = -3.5;
    for (let i = 0; i < 30; i++) {
      theta = updateAbility(theta, -3.5, false, DEFAULT_ADAPTIVE_CONFIG).thetaAfter;
    }
    expect(theta).toBeGreaterThanOrEqual(DEFAULT_ADAPTIVE_CONFIG.minAbility);
  });
});
