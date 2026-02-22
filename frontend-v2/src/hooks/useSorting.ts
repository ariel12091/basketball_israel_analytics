import { useState, useMemo } from 'react';
import type { SortDir } from '../types';

interface UseSortingResult<T> {
  sorted: T[];
  sortKey: string;
  sortDir: SortDir;
  onSort: (key: string) => void;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function useSorting<T extends Record<string, any>>(
  data: T[],
  defaultKey: string,
  defaultDir: SortDir = 'desc'
): UseSortingResult<T> {
  const [sortKey, setSortKey] = useState(defaultKey);
  const [sortDir, setSortDir] = useState<SortDir>(defaultDir);

  const onSort = (key: string) => {
    if (key === sortKey) {
      setSortDir(d => (d === 'asc' ? 'desc' : 'asc'));
    } else {
      setSortKey(key);
      setSortDir('desc');
    }
  };

  const sorted = useMemo(() => {
    if (!data.length) return data;
    const arr = [...data];
    const first = arr[0][sortKey];
    const isNum = typeof first === 'number';

    arr.sort((a, b) => {
      let aVal = a[sortKey];
      let bVal = b[sortKey];
      if (aVal == null) aVal = isNum ? -Infinity : '';
      if (bVal == null) bVal = isNum ? -Infinity : '';

      if (isNum) {
        return sortDir === 'asc'
          ? (aVal as number) - (bVal as number)
          : (bVal as number) - (aVal as number);
      }
      return sortDir === 'asc'
        ? String(aVal).localeCompare(String(bVal))
        : String(bVal).localeCompare(String(aVal));
    });
    return arr;
  }, [data, sortKey, sortDir]);

  return { sorted, sortKey, sortDir, onSort };
}
