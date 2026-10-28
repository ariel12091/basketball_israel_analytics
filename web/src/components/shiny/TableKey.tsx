"use client";
// The stats tabs' legend, terse, just above the table's top-left corner
// (user 2026-10-28), and the view's intro as a compact visible panel.
import { useState, type ReactNode } from "react";
import ui from "@/data/shinyUi.json";
import text from "@/data/layoutUi.json";
import { Runs, type Block } from "./Blocks";
import { useFrame } from "./TabKit";

const SWATCHES = [0, 5, 10, 15, 19].map((i) => ui.heat.grad[i]);

/** Shading scale, plus the shot-cell and on/off-detail keys when they apply. */
export default function TableLegend({
  shots = false,
  rank = false,
}: {
  shots?: boolean;
  rank?: boolean;
}) {
  const t = text.legend;
  return (
    <div className="table-legend">
      <span className="table-legend-row">
        {t.low}
        <span className="table-legend-scale" aria-hidden="true">
          {SWATCHES.map((c) => (
            <i key={c} style={{ background: c }} />
          ))}
        </span>
        {t.high}
      </span>
      {shots && (
        <span className="table-legend-row">
          {t.accuracy} · {t.frequency}
          <i className="table-legend-bar table-legend-2pt" aria-hidden="true" />
          {t.two}
          <i className="table-legend-bar table-legend-3pt" aria-hidden="true" />
          {t.three}
        </span>
      )}
      {rank && (
        <span className="table-legend-row">
          <i className="legend-icon-on" aria-hidden="true" /> {t.on}{" "}
          <i className="legend-icon-off" aria-hidden="true" /> {t.off}
        </span>
      )}
    </div>
  );
}

/** The view's "What this tab answers" explainer as one visible line (title
 * and question); More opens the bullets, the worked example and the
 * glossary link in place. `extra` sits at the end of the line. */
export function ViewIntro({
  blocks,
  extra,
}: {
  blocks: Block[];
  extra?: ReactNode;
}) {
  const { openGlossary } = useFrame();
  const [open, setOpen] = useState(false);
  const explainer = blocks.find(
    (b): b is Extract<Block, { kind: "explainer" }> => b.kind === "explainer",
  );
  const example = blocks.find(
    (b): b is Extract<Block, { kind: "example" }> => b.kind === "example",
  );
  if (!explainer)
    return extra ? <div className="view-intro">{extra}</div> : null;
  return (
    <div className="view-intro">
      <div className="view-intro-line">
        <b>{explainer.title}:</b>{" "}
        <Runs runs={explainer.intro} onGlossary={openGlossary} />{" "}
        <button
          type="button"
          className="view-intro-more"
          aria-expanded={open}
          onClick={() => setOpen((o) => !o)}
        >
          {open ? text.intro.less : text.intro.more}
        </button>
        {extra}
      </div>
      {open && (
        <div className="view-intro-body">
          <ul>
            {explainer.bullets.map((r, i) => (
              <li key={i}>
                <Runs runs={r} onGlossary={openGlossary} />
              </li>
            ))}
          </ul>
          {example && (
            <div className="view-intro-example">
              <div className="view-intro-example-text">
                <b>{example.title}</b>
                {example.paragraphs.map((p, i) => (
                  <p key={i}>
                    <Runs runs={p} onGlossary={openGlossary} />
                  </p>
                ))}
              </div>
              {/* eslint-disable-next-line @next/next/no-img-element -- a static snippet, as in Shiny */}
              <img src={example.image} alt={example.alt} />
            </div>
          )}
          <button
            type="button"
            className="btn btn-link p-0 view-intro-glossary"
            onClick={openGlossary}
          >
            Open glossary
          </button>
        </div>
      )}
    </div>
  );
}

/** Which legend parts a view needs, from its exported legend blocks. */
export function keyFromBlocks(blocks: Block[]) {
  const legend = (v: string) =>
    blocks.some((b) => b.kind === "legend" && b.variant === v);
  return { shots: legend("shot_splits"), rank: legend("rank") };
}

/** table_hint()'s sentences for a MoreInfo table: on touch, definitions open
 * from the dashed-underlined header itself, not from an info button. */
export function moreInfoHint(hint: { pointer: string; touch: string }) {
  const touch = hint.touch
    .split(" · ")
    .map((part) =>
      /info button/i.test(part) ? text.hint.touchDefinition : part,
    )
    .join(" · ");
  return { ...hint, touch };
}
