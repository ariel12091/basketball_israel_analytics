import { useEffect } from 'react';

interface GlossaryModalProps {
  onClose: () => void;
}

const TERMS = [
  { term: 'PPP', def: 'Points per possession.' },
  { term: 'Net Rating', def: 'Offensive PPP minus Defensive PPP.' },
  { term: 'TS%', def: 'Shooting efficiency including free throws and threes.' },
  { term: 'OREB%', def: 'Share of available offensive rebounds secured.' },
  { term: 'TOV%', def: 'Turnovers per offensive possession.' },
  { term: 'FTR', def: 'Free throw attempts relative to field goal attempts.' },
  { term: 'Possessions', def: 'Estimated offensive/defensive trips.' },
  { term: 'Game Number (GN)', def: 'League game sequence index.' },
  { term: 'Clutch', def: 'Late-game possessions filtered by margin/time settings.' },
];

export default function GlossaryModal({ onClose }: GlossaryModalProps) {
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    document.addEventListener('keydown', handler);
    return () => document.removeEventListener('keydown', handler);
  }, [onClose]);

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-card" style={{ width: 'min(90vw, 480px)' }} onClick={e => e.stopPropagation()}>
        <div className="modal-header">
          <span className="modal-title">Glossary</span>
          <button className="modal-close-btn" onClick={onClose}>
            &times;
          </button>
        </div>
        <div className="modal-body" style={{ padding: '16px 20px' }}>
          <ul style={{ margin: 0, paddingLeft: 18, lineHeight: 1.8, fontSize: 13, color: 'var(--text-secondary)' }}>
            {TERMS.map(({ term, def }) => (
              <li key={term}>
                <strong style={{ color: 'var(--text-primary)' }}>{term}</strong>: {def}
              </li>
            ))}
          </ul>
        </div>
      </div>
    </div>
  );
}
