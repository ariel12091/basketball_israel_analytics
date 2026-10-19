export function ShotLegend() {
  const frequencyItems = [
    { key: '2pt', label: '2PT', className: 'legend-swatch-2pt' },
    { key: '3pt', label: '3PT', className: 'legend-swatch-3pt' },
  ];

  return (
    <div className="legend-box legend-box-shot">
      <div className="legend-pill legend-pill-title">Shot Splits</div>

      <div className="legend-card">
        <span className="legend-meta">Frequency Mix</span>
        <div className="legend-chip-row">
          {frequencyItems.map((item) => (
            <span key={item.key} className="legend-chip">
              <span className={`legend-swatch ${item.className}`} />
              <span>{item.label}</span>
            </span>
          ))}
        </div>
      </div>

      <div className="legend-card">
        <span className="legend-meta">Accuracy Scale</span>
        <div className="legend-gradient-wrap">
          <span className="legend-acc-low">Low FG%</span>
          <span className="legend-gradient-bar" />
          <span className="legend-acc-high">High FG%</span>
        </div>
      </div>
    </div>
  );
}
