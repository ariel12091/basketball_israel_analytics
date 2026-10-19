import { createContext } from 'react';
import { buildDefaultTabFilters } from './defaults';
import { TabFilterState } from './types';

interface FilterContextType {
  state: TabFilterState;
  setState: React.Dispatch<React.SetStateAction<TabFilterState>>;
}

export const FilterContext = createContext<FilterContextType>({
  state: buildDefaultTabFilters(),
  setState: () => undefined,
});

export function buildInitialFilterState(): TabFilterState {
  return buildDefaultTabFilters();
}
