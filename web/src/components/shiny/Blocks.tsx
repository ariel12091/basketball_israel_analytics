"use client";
// The static parts of a Shiny tab, from the structured content that
// scripts/export_web_shiny_ui.R reads out of the R UI: tab_explainer() cards,
// the "Show/Hide Example" boxes, the legends, the Four Factors impact line,
// table_hint(), and rich text (glossary, popovers). React state replaces the
// Bootstrap collapse and app.js toggles.
import { useState, type CSSProperties, type ReactNode } from "react";

export type Run = {
  t: string;
  b?: boolean;
  em?: boolean;
  color?: string;
  glossary?: boolean;
};
export type TextBlock =
  | { p: Run[]; muted?: boolean }
  | { ul: Run[][] }
  | { h: string; color?: string | Record<string, never> }
  | { hr: true };
export type Block =
  | { kind: "explainer"; title: string; intro: Run[]; bullets: Run[][] }
  | {
      kind: "example";
      id: string;
      title: string;
      paragraphs: Run[][];
      image: string;
      alt: string;
      caption: string;
    }
  | {
      kind: "legend";
      variant: "shot_splits" | "rank";
      note?: string | Record<string, never> | null;
    }
  | { kind: "impact"; text: string };

/** Inline runs as elements: bold, emphasis, colour, the glossary link. */
export function Runs({
  runs,
  onGlossary,
}: {
  runs: Run[];
  onGlossary?: () => void;
}) {
  return (
    <>
      {runs.map((r, i) => {
        let node: ReactNode = r.t;
        if (r.em) node = <em>{node}</em>;
        if (r.b) node = <b>{node}</b>;
        if (r.color) node = <span style={{ color: r.color }}>{node}</span>;
        if (r.glossary)
          node = (
            <a
              href="#"
              onClick={(e) => {
                e.preventDefault();
                onGlossary?.();
              }}
            >
              {node}
            </a>
          );
        return <span key={i}>{node}</span>;
      })}
    </>
  );
}

export function TextBlocks({
  blocks,
  onGlossary,
}: {
  blocks: TextBlock[];
  onGlossary?: () => void;
}) {
  return (
    <>
      {blocks.map((b, i) => {
        if ("hr" in b) return <hr key={i} />;
        if ("h" in b)
          return (
            <h5
              key={i}
              style={{
                ...(i === 0 ? { marginTop: 0 } : {}),
                ...(typeof b.color === "string" ? { color: b.color } : {}),
              }}
            >
              {b.h}
            </h5>
          );
        if ("ul" in b)
          return (
            <ul key={i}>
              {b.ul.map((li, j) => (
                <li key={j}>
                  <Runs runs={li} onGlossary={onGlossary} />
                </li>
              ))}
            </ul>
          );
        return (
          <p
            key={i}
            style={
              b.muted
                ? { marginBottom: 0, color: "var(--ibpl-text-muted)" }
                : undefined
            }
          >
            <Runs runs={b.p} onGlossary={onGlossary} />
          </p>
        );
      })}
    </>
  );
}

function Explainer({
  block,
  open,
  onToggle,
  onGlossary,
}: {
  block: Extract<Block, { kind: "explainer" }>;
  open: boolean;
  onToggle: () => void;
  onGlossary: () => void;
}) {
  return (
    <div className="explainer-card">
      <div className="explainer-top">
        <h4 className="explainer-title">{block.title}</h4>
        <a
          href="#"
          className="explainer-toggle js-explainer-toggle"
          aria-expanded={open}
          onClick={(e) => {
            e.preventDefault();
            onToggle();
          }}
        >
          Show/Hide
        </a>
      </div>
      <div className={`explainer-body collapse${open ? " show" : ""}`}>
        <p>
          <Runs runs={block.intro} />
        </p>
        <ul>
          {block.bullets.map((b, i) => (
            <li key={i}>
              <Runs runs={b} />
            </li>
          ))}
        </ul>
        <p style={{ marginTop: 8, marginBottom: 0 }}>
          <a
            href="#"
            onClick={(e) => {
              e.preventDefault();
              onGlossary();
            }}
          >
            Open glossary
          </a>
        </p>
      </div>
    </div>
  );
}

function Example({
  block,
  open,
  onToggle,
}: {
  block: Extract<Block, { kind: "example" }>;
  open: boolean;
  onToggle: () => void;
}) {
  return (
    <>
      <a
        href={`#${block.id}`}
        className="explainer-toggle"
        aria-expanded={open}
        aria-controls={block.id}
        onClick={(e) => {
          e.preventDefault();
          onToggle();
        }}
      >
        Show/Hide Example
      </a>
      <div
        id={block.id}
        className={`collapse example-wrapper${open ? " show" : ""}`}
      >
        <div className="example-grid">
          <div className="example-card">
            <div className="example-card-title">{block.title}</div>
            {block.paragraphs.map((p, i) => (
              <p
                key={i}
                style={{
                  marginBottom: i === block.paragraphs.length - 1 ? 0 : 6,
                }}
              >
                <Runs runs={p} />
              </p>
            ))}
          </div>
          <div className="example-snippet">
            {/* eslint-disable-next-line @next/next/no-img-element -- a static snippet, as in Shiny */}
            <img src={block.image} alt={block.alt} />
            <div className="example-snippet-caption">{block.caption}</div>
          </div>
        </div>
      </div>
    </>
  );
}

const dim: CSSProperties = {
  fontSize: "0.75em",
  color: "var(--ibpl-text-dim)",
  textTransform: "uppercase",
  letterSpacing: "0.5px",
};
const swatch = (bg: string, extra: CSSProperties = {}): CSSProperties => ({
  width: 14,
  height: 14,
  background: bg,
  borderRadius: 3,
  ...extra,
});
/** shot_splits_legend_ui() */
function ShotSplitsLegend() {
  return (
    <div className="legend-box">
      <span style={{ fontWeight: 700, marginRight: 10 }}>Shot Splits:</span>
      <div className="legend-item">
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            gap: 2,
          }}
        >
          <span style={dim}>Frequency</span>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <div style={swatch("var(--ibpl-fg2)")}></div>
            <span>2PT</span>
            <div style={swatch("var(--ibpl-fg3)", { marginLeft: 6 })}></div>
            <span>3PT</span>
          </div>
        </div>
      </div>
      <span style={{ margin: "0 12px", color: "var(--ibpl-border)" }}>|</span>
      <div className="legend-item">
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            gap: 2,
          }}
        >
          <span style={dim}>Accuracy</span>
          <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
            <span style={{ color: "var(--ibpl-neg)", fontWeight: 600 }}>
              FG%
            </span>
            <span style={{ color: "var(--ibpl-text-dim)", margin: "0 2px" }}>
              -&gt;
            </span>
            <span style={{ color: "var(--ibpl-pos)", fontWeight: 600 }}>
              FG%
            </span>
          </div>
        </div>
      </div>
    </div>
  );
}
/** onoff_rank_legend_ui() and Tab 1's Shot Profile legend. */
function RankLegend({ note }: { note: string }) {
  return (
    <div className="legend-box">
      <span style={{ fontWeight: 700, marginRight: 5 }}>Legend:</span>
      <div className="legend-item">
        <div className="legend-icon-on"></div>
        <span>On-Court</span>
      </div>
      <div className="legend-item">
        <div className="legend-icon-off"></div>
        <span>Off-Court</span>
      </div>
      <div className="legend-item">
        <span>0%</span>
        <div className="legend-bar">
          <div className="legend-tick" style={{ left: 0 }}></div>
          <div
            className="legend-tick"
            style={{
              left: "50%",
              height: 12,
              top: -2,
              background: "var(--ibpl-text-dim)",
            }}
          ></div>
          <div className="legend-tick" style={{ right: 0 }}></div>
        </div>
        <span>100% Rank</span>
      </div>
      <span
        style={{
          marginLeft: 15,
          fontSize: "0.8em",
          color: "var(--ibpl-text-dim)",
        }}
      >
        {note}
      </span>
    </div>
  );
}

/** One view's blocks. As in app.js, the explainer's "Show/Hide" also toggles
 * the example box after it; "Show/Hide Example" toggles only the box. */
export function ViewBlocks({
  blocks,
  onGlossary,
}: {
  blocks: Block[];
  onGlossary: () => void;
}) {
  const [explainer, setExplainer] = useState(false);
  const [example, setExample] = useState(false);
  return (
    <>
      {blocks.map((b, i) => {
        switch (b.kind) {
          case "explainer":
            return (
              <Explainer
                key={i}
                block={b}
                open={explainer}
                onGlossary={onGlossary}
                onToggle={() => {
                  setExplainer((o) => !o);
                  setExample((o) => !o);
                }}
              />
            );
          case "example":
            return (
              <Example
                key={i}
                block={b}
                open={example}
                onToggle={() => setExample((o) => !o)}
              />
            );
          case "legend":
            return b.variant === "shot_splits" ? (
              <ShotSplitsLegend key={i} />
            ) : (
              <RankLegend
                key={i}
                note={typeof b.note === "string" ? b.note : ""}
              />
            );
          case "impact":
            return (
              <div key={i} className="ff-impact-legend">
                {b.text}
              </div>
            );
        }
      })}
    </>
  );
}

/** table_hint(): the pointer and touch sentences (CSS shows one). */
export function TableHint({
  hint,
}: {
  hint: { pointer: string; touch: string };
}) {
  return (
    <div className="table-hint">
      <span className="hint-pointer">{hint.pointer}</span>
      <span className="hint-touch">{hint.touch}</span>
    </div>
  );
}
