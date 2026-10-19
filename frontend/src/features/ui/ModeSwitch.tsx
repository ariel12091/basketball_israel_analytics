interface ModeSwitchProps {
  value: 'Summary' | 'Four Factors';
  onChange: (value: 'Summary' | 'Four Factors') => void;
  idPrefix: string;
}

export function ModeSwitch({ value, onChange, idPrefix }: ModeSwitchProps) {
  return (
    <div className="mode-switch" role="radiogroup" aria-label="View mode">
      <label htmlFor={`${idPrefix}-summary`}>
        <input
          id={`${idPrefix}-summary`}
          type="radio"
          checked={value === 'Summary'}
          onChange={() => onChange('Summary')}
        />
        Summary
      </label>
      <label htmlFor={`${idPrefix}-ff`}>
        <input
          id={`${idPrefix}-ff`}
          type="radio"
          checked={value === 'Four Factors'}
          onChange={() => onChange('Four Factors')}
        />
        Four Factors
      </label>
    </div>
  );
}
