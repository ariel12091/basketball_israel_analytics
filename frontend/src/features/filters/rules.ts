import { BaseFilters } from './types';

export function applyGnLastNMutualExclusion(
  prev: BaseFilters,
  patch: Partial<BaseFilters>,
): BaseFilters {
  const next: BaseFilters = { ...prev, ...patch };

  const lastNSet = (patch.lastN ?? '').trim() !== '';
  const gnSet = (patch.gnMin ?? '').trim() !== '' || (patch.gnMax ?? '').trim() !== '';

  if (lastNSet) {
    next.gnMin = '';
    next.gnMax = '';
  } else if (gnSet) {
    next.lastN = '';
  }

  return next;
}
