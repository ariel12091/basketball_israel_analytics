"use client";
import Selectize from "./Selectize";
import Slider from "./Slider";
import { tt } from "./TabKit";
import type { Query, Patch } from "@/lib/tabQuery";
/** The shared Shiny clutch_filter_ui controls, driven by the tab URL state. */
export default function ClutchFilters({
  p,
  query,
  set,
}: {
  p: string;
  query: Query;
  set: (patch: Patch) => void;
}) {
  return (
    <>
      <div className="checkbox">
        <label>
          <input
            type="checkbox"
            aria-label="Clutch"
            checked={query.clutch === "true"}
            onChange={(e) => set({ clutch: String(e.target.checked) })}
          />
          {tt("Clutch", "clutch")}
        </label>
      </div>
      {query.clutch === "true" && (
        <>
          <Slider
            id={`${p}_clutch_margin`}
            label="Max point margin"
            min={0}
            max={10}
            step={1}
            value={Number(query.clutch_margin)}
            onChange={(v) => set({ clutch_margin: String(v) })}
          />
          <Selectize
            id={`${p}_clutch_status`}
            label="Score status"
            choices={["all", "leading", "trailing", "tied"].map((value) => ({
              value,
              label: value[0].toUpperCase() + value.slice(1),
            }))}
            value={[query.clutch_status]}
            onChange={(v) => set({ clutch_status: v[0] ?? "all" })}
          />
          <Slider
            id={`${p}_clutch_minutes`}
            label="Max minutes remaining"
            min={1}
            max={5}
            step={1}
            value={Number(query.clutch_minutes)}
            onChange={(v) => set({ clutch_minutes: String(v) })}
          />
          <div className="checkbox">
            <label>
              <input
                type="checkbox"
                checked={query.ot_margin === "true"}
                onChange={(e) => set({ ot_margin: String(e.target.checked) })}
              />
              Exclude OT if margin exceeded
            </label>
          </div>
          <span className="help-block">
            By default, overtime always qualifies. Check above to apply margin
            filter to OT.
          </span>
        </>
      )}
    </>
  );
}
