"use client";
// The stats tabs' legend, a compact visual key at the table's top right
// (user 2026-10-28), and the view's intro as a "?" popover at its end.
import { useState } from "react";
import * as Popover from "@radix-ui/react-popover";
import ui from "@/data/shinyUi.json";
import text from "@/data/layoutUi.json";
import { Runs, type Block } from "./Blocks";
import MoreInfo from "./MoreInfo";
import { heatStyle } from "./DataTable";
import { TagWhy } from "./OnOffTables";
import type { TagKind } from "./tagText";
import type { League } from "@/rules/gameLogs";
import { useFrame } from "./TabKit";

const SWATCHES = [0, 5, 10, 15, 19].map((i) => ui.heat.grad[i]);

/** A compact visual key for the table's top right: the shading scale, a
 * miniature shot cell and the on/off markers, each only when the view has
 * them. Captions are one or two words. */
export default function TableLegend({
  shots = false,
  rank = false,
}: {
  shots?: boolean;
  rank?: boolean;
}) {
  const t = text.legend;
  return (
    <div className="table-legend" aria-label="Legend" role="group">
      <span className="tl-item">
        <span className="tl-scale" aria-hidden="true">
          {SWATCHES.map((c) => (
            <i key={c} className="ibpl-heat" style={heatStyle(c)} />
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
              style={{ left: "25%", width: "35%" }}
            />
            <span className="dot-off" style={{ left: "25%" }} />
            <span className="dot-on" style={{ left: "60%" }} />
          </span>
          <span className="tl-ends tl-pct">
            <span>{t.pctLow}</span>
            <span>{t.pctMid}</span>
            <span>{t.pctHigh}</span>
          </span>
        </span>
      )}
    </div>
  );
}

/** On/Off's row tags ("3PT luck", "small sample"), each explained in one
 * sentence on hover or tap. They sit with the "?" and CSV at the start of
 * the row, apart from the colour key at the top right (user 2026-11-16). */
export function TableTags({
  league,
  kinds = ["luck", "sample"],
}: {
  league: League;
  /** Team Ratings shows only "Shooting luck". */
  kinds?: readonly TagKind[];
}) {
  return (
    <span className="tl-tags">
      {kinds.map((k) => (
        <MoreInfo
          key={k}
          label={text.tags[k].label}
          className="onoff-luck-tag tl-tag"
        >
          <TagWhy league={league} kind={k} />
        </MoreInfo>
      ))}
    </span>
  );
}

/** The view's "What this tab answers" explainer as a small "?" button at
 * the end of the legend (user 2026-11-16: the visible question line took
 * a row few people used). Its name and tooltip are the question; a click
 * opens the explainer, the worked example and the glossary link. */
export function ViewIntro({
  blocks,
  summary,
}: {
  blocks: Block[];
  /** The short question (user 2026-10-28: nobody reads the long one). */
  summary?: string;
}) {
  const { openGlossary } = useFrame();
  const [open, setOpen] = useState(false);
  const explainer = blocks.find(
    (b): b is Extract<Block, { kind: "explainer" }> => b.kind === "explainer",
  );
  const example = blocks.find(
    (b): b is Extract<Block, { kind: "example" }> => b.kind === "example",
  );
  if (!explainer) return null;
  const question = summary ?? explainer.title;
  return (
    <div className="view-intro">
      <Popover.Root open={open} onOpenChange={setOpen}>
        <Popover.Trigger
          className="view-intro-btn"
          aria-label={question}
          title={question}
        >
          <i className="bi bi-question-lg" aria-hidden="true"></i>
        </Popover.Trigger>
        <Popover.Portal>
          <Popover.Content
            className="ibpl-more-pop view-intro-pop"
            align="end"
            sideOffset={6}
            collisionPadding={12}
          >
            <p className="view-intro-question">{question}</p>
            <p>
              <Runs runs={explainer.intro} onGlossary={openGlossary} />
            </p>
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
              onClick={() => {
                setOpen(false);
                openGlossary();
              }}
            >
              Open glossary
            </button>
          </Popover.Content>
        </Popover.Portal>
      </Popover.Root>
    </div>
  );
}

/** Which legend parts a view needs, from its exported legend blocks. */
export function keyFromBlocks(blocks: Block[]) {
  const legend = (v: string) =>
    blocks.some((b) => b.kind === "legend" && b.variant === v);
  return { shots: legend("shot_splits"), rank: legend("rank") };
}
