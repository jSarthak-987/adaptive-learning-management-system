import { describe, expect, it } from "vitest";
import { itemInformation, raschProbability } from "../src/rasch.js";

describe("raschProbability", () => {
  it("is 50% when ability equals difficulty", () => {
    expect(raschProbability(0, 0)).toBeCloseTo(0.5, 5);
  });

  it("matches documented examples", () => {
    expect(raschProbability(2, 0)).toBeCloseTo(0.88, 2);
    expect(raschProbability(0, 2)).toBeCloseTo(0.12, 2);
  });
});

describe("itemInformation", () => {
  it("peaks when theta approximates difficulty", () => {
    const atMatch = itemInformation(1.4, 1.4);
    const easy = itemInformation(1.4, -2);
    const hard = itemInformation(1.4, 4);
    expect(atMatch).toBeGreaterThan(easy);
    expect(atMatch).toBeGreaterThan(hard);
  });
});
