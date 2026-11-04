/**
 * The choices a comma-separated URL value stands for. One choice can hold
 * several codes (Winner Cup is game types 10 and 34, value "10,34"), so splitting
 * the value on commas alone leaves codes no choice is called, which a control
 * then shows raw. A code no choice knows is kept as it is.
 */
export function choiceValues(
  csv: string,
  choices: readonly { value: string }[],
): string[] {
  const owner = new Map<string, string>();
  for (const { value } of choices)
    for (const code of value.split(",").filter(Boolean)) owner.set(code, value);
  return [
    ...new Set(
      csv
        .split(",")
        .filter(Boolean)
        .map((code) => owner.get(code) ?? code),
    ),
  ];
}
