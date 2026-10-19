import { ReactNode } from 'react';

interface ExplainerCardProps {
  title: string;
  intro: string;
  bullets: string[];
  open: boolean;
  onToggle: () => void;
  example: ReactNode;
  exampleImageSrc?: string;
  exampleImageAlt?: string;
}

export function ExplainerCard(props: ExplainerCardProps) {
  const { title, intro, bullets, open, onToggle, example, exampleImageSrc, exampleImageAlt } = props;

  return (
    <section className="explainer">
      <button className="example-toggle" onClick={onToggle}>
        {open ? 'Hide Guide' : 'Show Guide'}
      </button>
      {open ? (
        <div className="example-box">
          <h3>{title}</h3>
          <p>{intro}</p>
          <ul>
            {bullets.map((bullet) => (
              <li key={bullet}>{bullet}</li>
            ))}
          </ul>
          <div className="example-grid">
            <div>{example}</div>
            {exampleImageSrc ? (
              <figure className="example-snippet">
                <img src={exampleImageSrc} alt={exampleImageAlt ?? 'Example table snippet'} loading="lazy" />
              </figure>
            ) : null}
          </div>
        </div>
      ) : null}
    </section>
  );
}
