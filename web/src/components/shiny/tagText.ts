// The On/Off row tags' explanations and worked examples, shared by the Net
// cell's tags and the legend's pills. The 3PT luck example is Shiny's own
// (shinyUi onoff popovers); Shiny has none for small sample, so that one is
// web text from the same 25-26 data.
import ui from "@/data/shinyUi.json";
import layout from "@/data/layoutUi.json";
import type { League } from "@/rules/gameLogs";
import type { TextBlock } from "./Blocks";

export type TagKind = "luck" | "sample";

export function tagText(league: League, kind: TagKind) {
  const euro = league !== "il";
  const tag = layout.tags[kind];
  if (kind === "sample")
    return {
      label: tag.label,
      text: tag.text,
      example: layout.tags.sample.example[euro ? "euro" : "il"],
    };
  const body = (euro ? ui.onoff.euro : ui.onoff.il).popovers[0]
    .body as TextBlock[];
  const para = body.find(
    (b): b is Extract<TextBlock, { p: unknown }> =>
      "p" in b && b.p.some((r) => r.t.startsWith("Example:")),
  );
  return {
    label: tag.label,
    text: tag.text,
    example: para ? para.p.map((r) => r.t).join("") : "",
  };
}
