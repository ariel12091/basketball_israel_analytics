// The Israeli game-type codes, from the generated catalogue: never typed here.
import catalogues from "@/data/catalogues.json";

const choices = catalogues.gameTypeChoices.filter((c) => c.value !== "");

/** "5 Regular season; 10,34 Winner Cup; ...", for the model. */
export const gameTypeList = choices.map((c) => `${c.value} ${c.label}`).join("; ");

const known = new Set(choices.flatMap((c) => c.value.split(",")));

/** The first code in a comma-separated value that the catalogue does not hold. */
export const unknownGameType = (value: string) =>
  value.split(",").find((code) => code && !known.has(code));
