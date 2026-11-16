"use client";
// The page's views (design 2026-10-28 § 3), the one view control since the
// navbar redesign. A choice that changes the URL is not an ARIA tablist
// (modern-web-guidance "tabs": no tabs pattern for navigation, and a tab
// needs a panel), so this is a labelled group of toggle buttons. The look is
// tab-like (web.css); data-state keeps the styling hook.
import { useLayoutEffect, useRef } from "react";

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
  const list = useRef<HTMLDivElement>(null);
  // Keep the selected view in sight when the row scrolls (Player Stats on a
  // phone, opened on its last view). Scrolls the row only, never the page.
  useLayoutEffect(() => {
    const el = list.current;
    const on = el?.querySelector<HTMLElement>('[aria-pressed="true"]');
    if (!el || !on || el.scrollWidth <= el.clientWidth) return;
    // The row is position: relative (web.css), so offsetLeft is within it.
    const left = on.offsetLeft;
    if (left < el.scrollLeft) el.scrollLeft = left;
    else if (left + on.offsetWidth > el.scrollLeft + el.clientWidth)
      el.scrollLeft = left + on.offsetWidth - el.clientWidth;
  }, [view]);
  return (
    <div className="view-tabs">
      <div ref={list} role="group" aria-label="View" className="view-tabs-list">
        {views.map((v) => (
          <button
            key={v}
            type="button"
            className="view-tabs-tab"
            aria-pressed={v === view}
            data-state={v === view ? "active" : "inactive"}
            title={tips?.[v]}
            onClick={() => v !== view && onView(v)}
          >
            {v}
          </button>
        ))}
      </div>
    </div>
  );
}
