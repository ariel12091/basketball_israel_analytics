// TanStack handles missing values separately so descending cannot reverse them.
export const orderKey = (v: unknown): number | string | undefined => {
  if (v == null || v === "" || (typeof v === "number" && Number.isNaN(v)))
    return undefined;
  if (typeof v === "number") return v;
  if (typeof v === "boolean") return Number(v);
  return String(v).toLowerCase();
};
