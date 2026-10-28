"use client";
// The stats tabs' legend, a compact visual key at the table's top right
// (user 2026-10-28), and the view's intro as a compact visible panel.
import { useState, type ReactNode } from "react";
import ui from "@/data/shinyUi.json";
import text from "@/data/layoutUi.json";
import { Runs, type Block } from "./Blocks";
import MoreInfo from "./MoreInfo";
import { useFrame } from "./TabKit";

const SWATCHES = [0, 5, 10, 15, 19].map((i) => ui.heat.grad[i]);

/** A compact visual key for the table's top right: the shading scale, a
 * miniature shot cell, the on/off markers and the row tags, each only when
 * the view has them. Captions are one or two words; tags explain themselves
 * in one sentence on hover or tap. */
export default function TableLegend({
  shots = false,
  rank = false,
  tags = false,
}: {
  shots?: boolean;
  rank?: boolean;
  tags?: boolean;
}) {
  const t = text.legend;
  return (
    <div className="table-legend" aria-label="Legend" role="group">
      <span className="tl-item">
        <span className="tl-scale" aria-hidden="true">
          {SWATCHES.map((c) => (
            <i key={c} style={{ background: c }} />
          ))}
        </span>
        <span className="tl-ends">
          <span>{t.worse}</span>
          <span>{t.better}</span>
        </span>
      </span>
      {shots && (
        <span className="tl-item tl-shot">
          <span className="tl-acc">
            {t.two} <span aria-hidden="true">|</span> {t.three}
          </span>
          <span className="tl-bar">
            <i className="tl-2pt" aria-hidden="true" />
            <i className="tl-3pt" aria-hidden="true" />
            <span className="tl-bar-label">{t.mix}</span>
          </span>
        </span>
      )}
      {rank && (
        // A miniature of the cells' rank bar: the whole league from the 0th
        // to the 100th percentile, with the on- and off-court dots on it.
        <span className="tl-item tl-rank">
          <span className="tl-rank-marks">
            <span>
              <i className="tl-dot-on" aria-hidden="true" /> {t.on}
            </span>
            <span>
              <i className="tl-dot-off" aria-hidden="true" /> {t.off}
            </span>
          </span>
          <span className="rank-bar-container tl-rank-bar" aria-hidden="true">
            <span
              className="range-connect"
              style={{ left: "30%", width: "45%" }}
            />
            <span className="dot-off" style={{ left: "30%" }} />
            <span className="dot-on" style={{ left: "75%" }} />
          </span>
          <span className="tl-ends tl-pct">
            <span>{t.pctLow}</span>
            <span>{t.pctMid}</span>
            <span>{t.pctHigh}</span>
          </span>
        </span>
      )}
      {tags &&
        (["luck", "sample"] as const).map((k) => (
          <MoreInfo
            key={k}
            label={text.tags[k].label}
            className="onoff-luck-tag tl-tag"
          >
            {text.tags[k].text}
          </MoreInfo>
        ))}
    </div>
  );
}

/** The view's "What this tab answers" explainer as one visible line (title
 * and question); More opens the bullets, the worked example and the
 * glossary link in place. `extra` sits at the end of the line. */
export function ViewIntro({
  blocks,
  summary,
  extra,
}: {
  blocks: Block[];
  /** The short question shown on the line (user 2026-10-28: nobody reads
   * the long one); the explainer's own intro moves under More. */
  summary?: string;
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
        {summary ? (
          <b>{summary}</b>
        ) : (
          <>
            <b>{explainer.title}:</b>{" "}
            <Runs runs={explainer.intro} onGlossary={openGlossary} />
          </>
        )}{" "}
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
          {summary && (
            <p>
              <Runs runs={explainer.intro} onGlossary={openGlossary} />
            </p>
          )}
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
