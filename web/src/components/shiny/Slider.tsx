"use client";
// sliderInput(ticks = FALSE) as Shiny draws it (ion.rangeSlider, skin
// "shiny"), used by minposs_slider(): the irs markup the theme-compiled
// ion.rangeSlider.css styles, pointer dragging, arrow keys by `step`,
// prettified values ("2,000"). The value is committed when the drag ends, as
// Shiny sends it.
import { useRef, useState, type ReactNode } from "react";

const pretty = (n: number) => n.toLocaleString("en-US");
const HANDLE = 22; // px, irs--shiny .irs-handle width

export default function Slider({
  id,
  label,
  min,
  max,
  step,
  value,
  width = 150,
  onChange,
}: {
  id: string;
  label: ReactNode;
  min: number;
  max: number;
  step: number;
  value: number;
  width?: number;
  onChange: (v: number) => void;
}) {
  const track = useRef<HTMLSpanElement>(null);
  const [drag, setDrag] = useState<number | null>(null);
  const shown = drag ?? value;
  const pct =
    max > min
      ? ((Math.min(Math.max(shown, min), max) - min) / (max - min)) * 100
      : 0;
  const snap = (v: number) =>
    Math.min(max, Math.max(min, Math.round((v - min) / step) * step + min));
  const at = (clientX: number) => {
    const r = track.current!.getBoundingClientRect();
    const usable = r.width - HANDLE;
    const x = Math.min(Math.max(clientX - r.left - HANDLE / 2, 0), usable);
    return snap(min + (x / Math.max(usable, 1)) * (max - min));
  };
  const left = `calc(${pct}% - ${(pct / 100) * HANDLE}px)`;
  return (
    <div className="form-group shiny-input-container" style={{ width }}>
      <label className="control-label" id={`${id}-label`} htmlFor={id}>
        {label}
      </label>
      <span
        ref={track}
        className="irs irs--shiny"
        style={{ touchAction: "none" }}
        onPointerDown={(e) => {
          (e.currentTarget as HTMLElement).setPointerCapture(e.pointerId);
          setDrag(at(e.clientX));
        }}
        onPointerMove={(e) => {
          if (drag != null) setDrag(at(e.clientX));
        }}
        onPointerUp={() => {
          if (drag != null && drag !== value) onChange(drag);
          setDrag(null);
        }}
        onPointerCancel={() => setDrag(null)}
        onLostPointerCapture={() => setDrag(null)}
      >
        <span className="irs">
          <span
            className="irs-line"
            tabIndex={0}
            role="slider"
            id={id}
            aria-labelledby={`${id}-label`}
            aria-valuemin={min}
            aria-valuemax={max}
            aria-valuenow={shown}
            onKeyDown={(e) => {
              const d =
                e.key === "ArrowRight" || e.key === "ArrowUp"
                  ? step
                  : e.key === "ArrowLeft" || e.key === "ArrowDown"
                    ? -step
                    : 0;
              if (!d) return;
              e.preventDefault();
              onChange(snap(value + d));
            }}
          ></span>
          <span
            className="irs-min"
            style={{ visibility: pct < 12 ? "hidden" : "visible" }}
          >
            {pretty(min)}
          </span>
          <span
            className="irs-max"
            style={{ visibility: pct > 88 ? "hidden" : "visible" }}
          >
            {pretty(max)}
          </span>
          <span
            className="irs-single"
            style={{
              left: `calc(${pct}% - ${(pct / 100) * HANDLE}px + ${HANDLE / 2}px)`,
              transform: "translateX(-50%)",
            }}
          >
            {pretty(shown)}
          </span>
        </span>
        <span
          className="irs-bar irs-bar--single"
          style={{
            left: 0,
            width: `calc(${pct}% - ${(pct / 100) * HANDLE}px + ${HANDLE / 2}px)`,
          }}
        ></span>
        <span className="irs-handle single" style={{ left }}>
          <i></i>
          <i></i>
          <i></i>
        </span>
      </span>
    </div>
  );
}
