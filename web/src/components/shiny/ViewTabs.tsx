"use client";
// The tab's display modes as an always-visible row of tabs above the table
// (design 2026-10-28 § 3). Drives the same state as the navbar menu.
import * as Tabs from "@radix-ui/react-tabs";

export default function ViewTabs({
  views,
  view,
  onView,
  tips,
}: {
  views: string[];
  view: string;
  onView: (v: string) => void;
  tips?: Record<string, string>;
}) {
  return (
    <Tabs.Root value={view} onValueChange={onView} className="view-tabs">
      <Tabs.List aria-label="View" className="view-tabs-list">
        {views.map((v) => (
          <Tabs.Trigger
            key={v}
            value={v}
            className="view-tabs-tab"
            title={tips?.[v]}
          >
            {v}
          </Tabs.Trigger>
        ))}
      </Tabs.List>
    </Tabs.Root>
  );
}
