import type { SortDir } from '../../types';

export interface ColumnGroup {
  label: string;
  span: number;
  sectionStart?: boolean;
  empty?: boolean;
}

export interface Column<T> {
  key: string;
  header: string;
  tip?: string;
  sectionStart?: boolean;
  sortable?: boolean;
  render: (row: T, index: number) => React.ReactNode;
}

interface DataTableProps<T> {
  groups: ColumnGroup[];
  columns: Column<T>[];
  data: T[];
  sortKey: string;
  sortDir: SortDir;
  onSort: (key: string) => void;
  infoText: string;
  onExport: () => void;
  rowKey?: (row: T, index: number) => string | number;
}

export default function DataTable<T>({
  groups,
  columns,
  data,
  sortKey,
  sortDir,
  onSort,
  infoText,
  onExport,
  rowKey,
}: DataTableProps<T>) {
  return (
    <div className="table-card">
      <div className="table-toolbar">
        <div className="table-info">{infoText}</div>
        <div className="table-actions">
          <button className="table-action-btn" onClick={onExport}>
            CSV
          </button>
        </div>
      </div>
      <div className="data-table-wrap">
        <table className="data-table">
          <thead>
            {/* Group row */}
            <tr>
              {groups.map((g, i) => (
                <th
                  key={i}
                  colSpan={g.span}
                  className={[
                    g.empty ? 'group-empty' : '',
                    g.sectionStart ? 'section-start' : '',
                  ]
                    .filter(Boolean)
                    .join(' ')}
                >
                  {g.label}
                </th>
              ))}
            </tr>
            {/* Column row */}
            <tr>
              {columns.map(col => {
                const sorted = col.key === sortKey;
                const cls = [
                  col.tip ? 'th-tip' : '',
                  col.sectionStart ? 'section-start' : '',
                  sorted ? (sortDir === 'asc' ? 'sorted-asc' : 'sorted-desc') : '',
                ]
                  .filter(Boolean)
                  .join(' ');
                return (
                  <th
                    key={col.key}
                    className={cls}
                    data-tip={col.tip}
                    onClick={col.sortable !== false ? () => onSort(col.key) : undefined}
                    style={col.sortable === false ? { cursor: 'default' } : undefined}
                  >
                    {col.header}
                  </th>
                );
              })}
            </tr>
          </thead>
          <tbody>
            {data.map((row, i) => (
              <tr key={rowKey ? rowKey(row, i) : i}>
                {columns.map(col => col.render(row, i))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

/** Export data as CSV download */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function exportCSV<T extends Record<string, any>>(
  data: T[],
  filename: string,
  columnKeys?: string[],
  columnHeaders?: string[]
) {
  if (!data.length) return;
  const keys = columnKeys || Object.keys(data[0]);
  const headers = columnHeaders || keys;

  const csvRows = [headers.join(',')];
  data.forEach(row => {
    const values = keys.map(k => {
      let val = row[k];
      if (typeof val === 'object' && val !== null) val = JSON.stringify(val);
      return `"${String(val ?? '').replace(/"/g, '""')}"`;
    });
    csvRows.push(values.join(','));
  });

  const blob = new Blob([csvRows.join('\n')], { type: 'text/csv' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}
