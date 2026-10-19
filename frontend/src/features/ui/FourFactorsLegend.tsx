export function FourFactorsLegend() {
  return (
    <div className="legend-box legend-box-ff">
      <div className="legend-pill legend-pill-title">Four Factors</div>

      <div className="legend-card">
        <span className="legend-meta">Same Scale as Cells</span>
        <div className="legend-ff-scale-wrap">
          <span className="ff-rank-bar-container legend-ff-track">
            <span className="ff-range-connect" style={{ left: '18%', width: '56%' }} />
            <span className="ff-dot-off" style={{ left: '18%' }} title="Off-Court (white dot)" />
            <span className="ff-dot-on" style={{ left: '74%' }} title="On-Court (black dot)" />
          </span>
          <div className="legend-dot-map">
            <span>Off-Court</span>
            <span>On-Court</span>
          </div>
        </div>
      </div>

      <div className="legend-pill legend-pill-note">Ranked players only: above possessions baseline</div>
    </div>
  );
}
