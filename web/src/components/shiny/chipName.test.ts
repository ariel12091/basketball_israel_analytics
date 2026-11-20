import { expect, it } from "vitest";
import { chipName } from "./LineupPlayers";

it("title-cases all-caps names, keeping Mc names and numerals right", () => {
  expect(chipName("KENDALE MCCULLUM")).toBe("Kendale McCullum");
  expect(chipName("JAMES MCDONALD II")).toBe("James McDonald II");
  expect(chipName("SHAQUILLE O'NEAL")).toBe("Shaquille O'Neal");
  expect(chipName("MALIK MACK")).toBe("Malik Mack");
  expect(chipName("Kyle McRae")).toBe("Kyle McRae");
});
