"use client";
// Keeps the layout body classes in step with the viewport (rules and reasons
// in ./layoutClasses), plus the league scope and the remembered collapsed
// filter panel of app.js.
import { useCallback, useEffect, useSyncExternalStore } from "react";
import { NARROW, PHONE, PHONE_FIRST, TOUCH } from "./layoutClasses";

export type LayoutKind = "phone" | "tablet" | "desktop";

const QUERIES = [PHONE, NARROW, TOUCH];
function snapshot() {
  if (typeof window === "undefined") return PHONE_FIRST;
  return QUERIES.map((q) => Number(window.matchMedia(q).matches)).join("|");
}
function subscribe(cb: () => void) {
  const list = QUERIES.map((q) => window.matchMedia(q));
  list.forEach((q) => q.addEventListener("change", cb));
  return () => list.forEach((q) => q.removeEventListener("change", cb));
}

const FILTERS_KEY = "ibpl_filters_collapsed";
const filterListeners = new Set<() => void>();
// Storage can be blocked (private window); the toggle still works for the page.
let filtersMemo: boolean | null = null;
function readFiltersCollapsed() {
  if (filtersMemo === null) {
    try {
      filtersMemo = window.localStorage.getItem(FILTERS_KEY) === "1";
    } catch {
      filtersMemo = false;
    }
  }
  return filtersMemo;
}

export function useShinyBody(euro: boolean) {
  const [phone, narrow, touch] = useSyncExternalStore(
    subscribe,
    snapshot,
    () => PHONE_FIRST,
  )
    .split("|")
    .map((x) => x === "1");
  const kind: LayoutKind = phone ? "phone" : narrow ? "tablet" : "desktop";
  const collapsedNav = narrow;
  const filtersCollapsed = useSyncExternalStore(
    (cb) => {
      filterListeners.add(cb);
      return () => filterListeners.delete(cb);
    },
    readFiltersCollapsed,
    () => false,
  );

  useEffect(() => {
    const b = document.body.classList;
    b.remove("ibpl-device-phone", "ibpl-device-tablet", "ibpl-device-desktop");
    b.add(`ibpl-device-${kind}`);
    b.toggle("ibpl-mobile", phone);
    b.toggle("ibpl-collapsed-nav", collapsedNav);
    b.toggle("ibpl-stacked-filters", narrow);
    b.toggle("ibpl-touch-tips", phone || touch);
    b.toggle("league-il", !euro);
    b.toggle("league-el", euro);
    b.toggle("filters-collapsed", filtersCollapsed);
  }, [kind, phone, narrow, touch, collapsedNav, euro, filtersCollapsed]);

  const toggleFilters = useCallback(() => {
    filtersMemo = !readFiltersCollapsed();
    try {
      window.localStorage.setItem(FILTERS_KEY, filtersMemo ? "1" : "0");
    } catch {}
    filterListeners.forEach((l) => l());
  }, []);

  return {
    kind,
    collapsedNav,
    touchTips: phone || touch,
    filtersCollapsed,
    toggleFilters,
  };
}
