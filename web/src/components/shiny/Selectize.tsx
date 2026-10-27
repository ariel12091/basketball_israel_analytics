"use client";
// A selectize.js control as Shiny renders it (selectizeInput / selectInput with
// the selectize plugin), so app.css styles it exactly as in the live app.
// Behaviour follows selectize 0.15: type to filter (case-insensitive, matches
// highlighted), arrow keys move the active option, Enter picks, Escape closes;
// a single select closes on pick, a multi select stays open, hides picked
// options and drops the last item on Backspace.
import {
  Fragment,
  useId,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";

export type Choice = { value: string; label: string; group?: string };

type Props = {
  id: string;
  label?: ReactNode;
  choices: Choice[];
  value: string[];
  onChange: (value: string[]) => void;
  multiple?: boolean;
  placeholder?: string;
  /** Label of the "" choice listed first (team_select_choices_with_all). */
  emptyLabel?: string;
  /** selectizeInput adds form-control to the control; selectInput does not. */
  formControl?: boolean;
  style?: React.CSSProperties;
};

function highlight(label: string, query: string): ReactNode {
  if (!query) return label;
  const at = label.toLowerCase().indexOf(query.toLowerCase());
  if (at < 0) return label;
  return (
    <>
      {label.slice(0, at)}
      <span className="highlight">{label.slice(at, at + query.length)}</span>
      {label.slice(at + query.length)}
    </>
  );
}

export default function Selectize({
  id,
  label,
  choices,
  value,
  onChange,
  multiple = false,
  placeholder,
  emptyLabel,
  formControl = true,
  style,
}: Props) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [active, setActive] = useState(0);
  const input = useRef<HTMLInputElement>(null);
  const listId = useId();
  const labels = useMemo(
    () => new Map(choices.map((c) => [c.value, c.label])),
    [choices],
  );
  const picked = value.filter((v) => v !== "");
  const options = useMemo(() => {
    const q = query.trim().toLowerCase();
    const list = choices.filter(
      (c) =>
        c.value !== "" &&
        (!multiple || !picked.includes(c.value)) &&
        (!q || c.label.toLowerCase().includes(q)),
    );
    return emptyLabel != null && !q && !multiple
      ? [{ value: "", label: emptyLabel }, ...list]
      : list;
  }, [choices, query, multiple, picked, emptyLabel]);

  const close = () => {
    setOpen(false);
    setQuery("");
  };
  const pick = (choice: Choice | undefined) => {
    if (!choice) return;
    if (multiple) {
      onChange([...picked, choice.value]);
      setQuery("");
      setActive(0);
    } else {
      onChange(choice.value ? [choice.value] : []);
      close();
      input.current?.blur();
    }
  };
  const showPlaceholder = !picked.length && !query;
  const classes = [
    "selectize-input",
    "items",
    multiple || !picked.length ? "not-full" : "full",
    choices.length ? "has-options" : "",
    picked.length ? "has-items" : "",
    open ? "focus input-active dropdown-active" : "",
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <div className="form-group shiny-input-container" style={style}>
      {label != null && (
        <label
          className="control-label"
          id={`${id}-label`}
          htmlFor={`${id}-selectized`}
        >
          {label}
        </label>
      )}
      <div>
        <div
          className={`shiny-input-select ${formControl ? "form-control " : ""}selectize-control ${multiple ? "multi" : "single"} plugin-selectize-plugin-a11y`}
          data-input-id={id}
        >
          <div
            className={classes}
            onMouseDown={(e) => {
              if (e.target !== input.current) e.preventDefault();
              input.current?.focus();
              setOpen(true);
            }}
          >
            {picked.map((v) => (
              <div key={v} className="item" data-value={v}>
                {labels.get(v) ?? v}
              </div>
            ))}
            <input
              ref={input}
              type="text"
              autoComplete="off"
              id={`${id}-selectized`}
              role="combobox"
              aria-expanded={open}
              aria-haspopup="listbox"
              aria-controls={listId}
              aria-labelledby={label != null ? `${id}-label` : undefined}
              aria-label={label == null ? id : undefined}
              aria-activedescendant={
                open && options[active] ? `${listId}-${active}` : undefined
              }
              placeholder={showPlaceholder ? placeholder : undefined}
              value={query}
              style={{
                width:
                  picked.length && !multiple && !query
                    ? 4
                    : `${Math.max(query.length, showPlaceholder ? (placeholder?.length ?? 0) : 0, 1) + 1}ch`,
                opacity: picked.length && !multiple && !open ? 0 : 1,
                position:
                  picked.length && !multiple && !open ? "absolute" : "relative",
                left: picked.length && !multiple && !open ? -10000 : 0,
              }}
              onFocus={() => setOpen(true)}
              onBlur={close}
              onChange={(e) => {
                setQuery(e.target.value);
                setActive(0);
                setOpen(true);
              }}
              onKeyDown={(e) => {
                if (e.key === "ArrowDown") {
                  e.preventDefault();
                  setOpen(true);
                  setActive((a) => Math.min(a + 1, options.length - 1));
                } else if (e.key === "ArrowUp") {
                  e.preventDefault();
                  setActive((a) => Math.max(a - 1, 0));
                } else if (e.key === "Enter") {
                  e.preventDefault();
                  if (open) pick(options[active]);
                  else setOpen(true);
                } else if (e.key === "Escape") {
                  close();
                  input.current?.blur();
                } else if (e.key === "Backspace" && !query && picked.length) {
                  onChange(picked.slice(0, -1));
                }
              }}
            />
          </div>
          <div
            className={`${multiple ? "multi" : "single"} selectize-dropdown shiny-input-select ${formControl ? "form-control " : ""}plugin-selectize-plugin-a11y`}
            style={{
              display: open && options.length ? "block" : "none",
              width: "100%",
              top: "100%",
              left: 0,
            }}
          >
            <div
              className="selectize-dropdown-content"
              role="listbox"
              id={listId}
              aria-multiselectable={multiple || undefined}
            >
              {options.map((c, i) => (
                <Fragment key={c.value || "__empty"}>
                  {c.group && c.group !== options[i - 1]?.group && (
                    <div className="optgroup-header">{c.group}</div>
                  )}
                  <div
                    id={`${listId}-${i}`}
                    role="option"
                    aria-selected={picked.includes(c.value)}
                    className={`option${c.value === "" ? " selectize-dropdown-emptyoptionlabel" : ""}${i === active ? " active" : ""}${picked.includes(c.value) ? " selected" : ""}`}
                    data-selectable=""
                    data-value={c.value}
                    onMouseEnter={() => setActive(i)}
                    onMouseDown={(e) => {
                      e.preventDefault();
                      pick(c);
                    }}
                  >
                    {highlight(c.label, query)}
                  </div>
                </Fragment>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
