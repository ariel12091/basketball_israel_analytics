import { useEffect, useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';

interface UseApiResult<T> {
  data: T | null;
  loading: boolean;
  error: string | null;
}

type CacheEntry = { ts: number; data: unknown };
const API_CACHE = new Map<string, CacheEntry>();

function cacheTtlMs(endpoint: string): number {
  if (endpoint.startsWith('/api/meta/')) return 10 * 60 * 1000;
  return 60 * 1000;
}

export function useApi<T>(
  endpoint: string,
  params: Record<string, unknown>,
  enabled = true
): UseApiResult<T> {
  // Keep API behavior stable while reducing high-churn requests.
  const [debouncedParams, setDebouncedParams] = useState(params);
  useEffect(() => {
    const id = setTimeout(() => setDebouncedParams(params), 300);
    return () => clearTimeout(id);
  }, [params]);

  const url = useMemo(() => {
    const qs = Object.entries(debouncedParams)
      .filter(([, v]) => v !== null && v !== undefined && v !== '')
      .map(([k, v]) => {
        if (Array.isArray(v)) {
          return v.length > 0 ? `${k}=${v.join(',')}` : null;
        }
        return `${k}=${encodeURIComponent(String(v))}`;
      })
      .filter(Boolean)
      .join('&');
    return qs ? `${endpoint}?${qs}` : endpoint;
  }, [endpoint, debouncedParams]);

  const query = useQuery<T>({
    queryKey: ['api', url],
    queryFn: async ({ signal }) => {
      const ttlMs = cacheTtlMs(endpoint);
      const now = Date.now();
      const cached = API_CACHE.get(url);
      if (cached && now - cached.ts <= ttlMs) {
        return cached.data as T;
      }
      const res = await fetch(url, { signal });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      API_CACHE.set(url, { ts: Date.now(), data: json });
      return json as T;
    },
    enabled,
    staleTime: cacheTtlMs(endpoint),
    gcTime: endpoint.startsWith('/api/meta/') ? 15 * 60 * 1000 : 5 * 60 * 1000,
    placeholderData: prev => prev,
  });

  return {
    data: (query.data ?? null) as T | null,
    loading: query.isLoading || query.isFetching,
    error: query.error ? (query.error as Error).message : null,
  };
}
