import { describe, it, expect } from "vitest";
import { choiceValues } from "./choices";
import catalogues from "../data/catalogues.json";

const types = catalogues.gameTypeChoices.filter((c) => c.value !== "");

describe("choiceValues", () => {
  it("reads a multi-code value as its one choice", () => {
    expect(choiceValues("10,34", types)).toEqual(["10,34"]);
  });
  it("keeps it apart from the other choices picked, in the order picked", () => {
    expect(choiceValues("5,10,34,35", types)).toEqual(["5", "10,34", "35"]);
  });
  it("gives every game-type code a label through its choice", () => {
    for (const { value, label } of types)
      for (const code of value.split(","))
        expect(
          choiceValues(code, types).map(
            (v) => types.find((c) => c.value === v)?.label,
          ),
        ).toEqual([label]);
  });
  it("keeps an unknown code and treats an empty value as nothing picked", () => {
    expect(choiceValues("999", types)).toEqual(["999"]);
    expect(choiceValues("", types)).toEqual([]);
  });
});
