// The app's own explanations as rows: the glossary modal, the filter
// tooltips, and each tab's explainer cards, "How to Read" examples, popovers
// and impact line, read from shinyUi.json (scripts/export_web_shiny_ui.R) and
// never retyped (inventory X-16). No database read.
import ui from "@/data/shinyUi.json";
import type { Block, Run, TextBlock } from "@/components/shiny/Blocks";
import type { League } from "@/rules/gameLogs";
import { ParamError } from "./api";

type Popover = { title: string; trigger: string; body: TextBlock[] };
type Fragment = {
  views: Record<string, Block[]>;
  after?: Record<string, Block[]>;
  popovers?: Popover[];
};
export type GuideRow = {
  topic: string;
  view: string | null;
  kind: "definition" | "filter" | "explainer" | "example" | "popover" | "impact";
  title: string;
  text: string;
};

/** Each tab's content, per league, as its page reads it. */
const TABS: Record<string, (euro: boolean) => Fragment | { blocks: Block[] } | null> = {
  onoff: (e) => ui.onoff[e ? "euro" : "il"] as unknown as Fragment,
  lineups: (e) => ui.lineups[e ? "euro" : "il"] as unknown as Fragment,
  team_ratings: (e) => ui.teamRatings[e ? "euro" : "il"] as unknown as Fragment,
  game_logs: (e) => ui[e ? "euro" : "il"] as unknown as Fragment,
  // PlayerStatsClient hides the Israeli example on EuroLeague/EuroCup.
  player_stats: (e) => ({
    blocks: (ui.playerStats.blocks as Block[]).filter((b) => !e || b.kind !== "example"),
  }),
  compare: (e) => (e ? null : { blocks: ui.compare.blocks as Block[] }),
};
export const GUIDE_TOPICS = ["glossary", "filters", ...Object.keys(TABS)];

const runs = (r: Run[]) => r.map((x) => x.t).join("").trim();
function textBlocks(blocks: TextBlock[]): string {
  return blocks
    .flatMap((b) =>
      "hr" in b ? [] : "h" in b ? [`${b.h}:`] : "ul" in b ? b.ul.map((li) => `- ${runs(li)}`) : [runs(b.p)],
    )
    .join("\n");
}

function blockRows(topic: string, view: string | null, blocks: Block[]): GuideRow[] {
  return blocks.flatMap((b): GuideRow[] => {
    if (b.kind === "explainer")
      return [{ topic, view, kind: "explainer", title: b.title, text: [runs(b.intro), ...b.bullets.map((li) => `- ${runs(li)}`)].join("\n") }];
    if (b.kind === "example")
      return [{ topic, view, kind: "example", title: b.title, text: b.paragraphs.map(runs).join("\n") }];
    if (b.kind === "impact") return [{ topic, view, kind: "impact", title: "Estimated factor impact", text: b.text }];
    return []; // legends are drawn, and the glossary's Shot Splits / ranking sections say the same
  });
}

/** Glossary sections: the heading, then its items. */
function glossaryRows(): GuideRow[] {
  const out: GuideRow[] = [];
  for (const b of ui.glossary as TextBlock[]) {
    if ("h" in b) out.push({ topic: "glossary", view: null, kind: "definition", title: b.h, text: "" });
    else if (out.length) out.at(-1)!.text = [out.at(-1)!.text, textBlocks([b])].filter(Boolean).join("\n");
  }
  return out;
}

export function guideRows(league: League, s: URLSearchParams): GuideRow[] {
  const topic = s.get("topic") ?? "";
  const view = s.get("view") || null;
  if (!GUIDE_TOPICS.includes(topic))
    throw new ParamError("topic", `topic must be one of ${GUIDE_TOPICS.join(", ")}`);
  if (topic === "glossary") return glossaryRows();
  if (topic === "filters")
    return Object.entries(ui.filterTooltips as Record<string, string>).map(([k, text]) => ({
      topic,
      view: null,
      kind: "filter",
      title: k,
      text,
    }));
  const frag = TABS[topic](league !== "il");
  if (!frag) throw new ParamError("topic", `${topic} is Israeli League only`);
  if ("blocks" in frag) return blockRows(topic, null, frag.blocks);
  const views = Object.keys(frag.views).filter((v) => frag.views[v].length);
  if (view && !views.includes(view))
    throw new ParamError("view", `view must be one of ${views.join(", ")}`);
  return [
    ...(view ? [view] : views).flatMap((v) => [
      ...blockRows(topic, v, frag.views[v]),
      ...blockRows(topic, v, frag.after?.[v] ?? []),
    ]),
    ...(frag.popovers ?? []).map(
      (p): GuideRow => ({ topic, view: null, kind: "popover", title: p.trigger || p.title, text: textBlocks(p.body) }),
    ),
  ];
}

export async function guideAnswer(league: League, s: URLSearchParams) {
  return { rows: guideRows(league, s) };
}
