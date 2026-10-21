"use client";
// mobile.js "touch tooltips" as components. Where there is no hover (phone
// widths, touch pointers -- see ./layoutClasses), a column header with a
// definition carries an "i" button that opens an inline strip above the table,
// and a filter label with a definition opens a strip under itself when tapped.
// mobile.css draws both, scoped to body.ibpl-touch-tips; elsewhere the native
// title and app.css's [data-tooltip] bubble explain on hover.
import { createContext, useContext, useState, type ReactNode } from "react";

export const TouchTipsContext = createContext(false);

export function Strip({
  text,
  open,
  className,
  onClose,
}: {
  text: string;
  open: boolean;
  className: string;
  onClose: () => void;
}) {
  return (
    <div
      className={`ibpl-m-strip ${className}${open ? " ibpl-m-strip-open" : ""}`}
    >
      <span className="ibpl-m-strip-text">{text}</span>
      <button
        type="button"
        className="ibpl-m-strip-close"
        aria-label="Close explanation"
        onClick={onClose}
      >
        ✕
      </button>
    </div>
  );
}

/** tt() in global.R: a label with a FILTER_TOOLTIPS definition. */
export function Tip({ label, tip }: { label: string; tip?: string }) {
  const touch = useContext(TouchTipsContext);
  const [open, setOpen] = useState(false);
  if (!tip) return <>{label}</>;
  return (
    <>
      <span data-tooltip={tip} onClick={() => touch && setOpen((o) => !o)}>
        {label}
      </span>
      {touch && open && (
        <Strip
          text={tip}
          open
          className="ibpl-m-label-strip"
          onClose={() => setOpen(false)}
        />
      )}
    </>
  );
}

/** The header "i" button; its strip is owned by the table (one per table). */
export function InfoButton({ onOpen }: { onOpen: () => void }): ReactNode {
  return (
    <button
      type="button"
      className="ibpl-m-th-info"
      aria-label="What this column means"
      onClick={(e) => {
        // A header tap sorts; the button explains instead.
        e.stopPropagation();
        onOpen();
      }}
    />
  );
}
