"use client";
// A list of strings kept in this browser's localStorage (column order,
// hidden columns), shared by every component that reads the same key and
// updated in all of them when one saves. Storage may be blocked (private
// windows): then the list is empty and saving does nothing lasting.
import { useCallback, useSyncExternalStore } from "react";

const EMPTY: string[] = [];
const EVENT = "ibpl-stored-list";
const snapshots = new Map<string, { raw: string | null; value: string[] }>();

function read(key: string): string[] {
  try {
    const raw = localStorage.getItem(key),
      old = snapshots.get(key);
    if (old && old.raw === raw) return old.value;
    const parsed = JSON.parse(raw ?? "[]"),
      value = Array.isArray(parsed)
        ? [...new Set(parsed.filter((x): x is string => typeof x === "string"))]
        : EMPTY;
    snapshots.set(key, { raw, value });
    return value;
  } catch {
    return snapshots.get(key)?.value ?? EMPTY;
  }
}
function subscribe(notify: () => void) {
  window.addEventListener("storage", notify);
  window.addEventListener(EVENT, notify);
  return () => {
    window.removeEventListener("storage", notify);
    window.removeEventListener(EVENT, notify);
  };
}

export function useStoredList(key: string) {
  const value = useSyncExternalStore(
    subscribe,
    () => read(key),
    () => EMPTY,
  );
  const save = useCallback(
    (next: string[]) => {
      try {
        localStorage.setItem(key, JSON.stringify(next));
        snapshots.delete(key);
      } catch {
        snapshots.set(key, { raw: null, value: next });
      }
      window.dispatchEvent(new Event(EVENT));
    },
    [key],
  );
  return [value, save] as const;
}
