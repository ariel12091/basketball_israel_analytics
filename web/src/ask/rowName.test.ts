import { expect, it } from "vitest";
import { rowName } from "./rowName";

it("names a quoted row by its player, then its lineup, then its team", () => {
  const onoff = { Player: "TOMER GINAT", Team: "HAPOEL TEL AVIV", name_he: "תומר גינת", team_name_he: "הפועל תל אביב" };
  expect(rowName(onoff, "en")).toBe("TOMER GINAT");
  expect(rowName(onoff, "he")).toBe("תומר גינת");
  expect(rowName({ "First Name": "Vasilije", "Last Name": "Micic", Team: "X" }, "en")).toBe("Vasilije Micic");
  expect(rowName({ Players: "A, B, C, D, E", Team: "X" }, "en")).toBe("A, B, C, D, E");
  expect(rowName({ team_name: "M. RISHON", team_name_he: "מכבי ראשון" }, "he")).toBe("מכבי ראשון");
  expect(rowName({ off_ppp: 110 }, "en")).toBe("");
});
