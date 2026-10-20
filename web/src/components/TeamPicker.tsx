"use client";
import { useId, useRef, useState } from "react";

type Team = { id: number; name: string };
export default function TeamPicker({
  label,
  teams,
  values,
  multiple = false,
  onChange,
}: {
  label: string;
  teams: Team[];
  values: string[];
  multiple?: boolean;
  onChange: (values: string[]) => void;
}) {
  const id = useId(),
    input = useRef<HTMLInputElement>(null);
  const [search, setSearch] = useState(""),
    [open, setOpen] = useState(false),
    [active, setActive] = useState(0);
  const options = teams.filter((team) =>
    team.name.toLocaleLowerCase().includes(search.trim().toLocaleLowerCase()),
  );
  const index = Math.min(active, Math.max(0, options.length - 1));
  const choose = (value: string) => {
    onChange(
      multiple
        ? values.includes(value)
          ? values.filter((v) => v !== value)
          : [...values, value]
        : [value],
    );
    setSearch("");
    setActive(0);
    setOpen(multiple);
    input.current?.focus();
  };
  return (
    <div
      className="team-picker"
      onBlur={(e) => {
        if (!e.currentTarget.contains(e.relatedTarget)) {
          setOpen(false);
          setSearch("");
        }
      }}
    >
      <label htmlFor={id}>{label}</label>
      <div className="picker-control">
        {values.map((value) => (
          <span className="picker-token" key={value}>
            {teams.find((team) => String(team.id) === value)?.name ??
              "Selected team"}
            <button
              type="button"
              aria-label={`Remove ${teams.find((team) => String(team.id) === value)?.name ?? "selected team"} from ${label}`}
              onClick={() => onChange(values.filter((v) => v !== value))}
            >
              ×
            </button>
          </span>
        ))}
        <input
          id={id}
          ref={input}
          role="combobox"
          autoComplete="off"
          aria-autocomplete="list"
          aria-expanded={open}
          aria-controls={`${id}-options`}
          aria-activedescendant={
            open && options.length
              ? `${id}-option-${options[index].id}`
              : undefined
          }
          placeholder={
            values.length
              ? "Type to search…"
              : label === "Opponents"
                ? "All opponents — type to search…"
                : "All teams — type to search…"
          }
          value={search}
          onFocus={() => setOpen(true)}
          onClick={() => setOpen(true)}
          onChange={(e) => {
            setSearch(e.target.value);
            setActive(0);
            setOpen(true);
          }}
          onKeyDown={(e) => {
            if (e.key === "ArrowDown" || e.key === "ArrowUp") {
              e.preventDefault();
              setOpen(true);
              setActive(
                open
                  ? Math.max(
                      0,
                      Math.min(
                        options.length - 1,
                        index + (e.key === "ArrowDown" ? 1 : -1),
                      ),
                    )
                  : 0,
              );
            } else if (e.key === "Enter" && open) {
              e.preventDefault();
              if (options[index]) choose(String(options[index].id));
            } else if (e.key === "Escape") {
              e.preventDefault();
              setOpen(false);
              setSearch("");
            } else if (e.key === "Backspace" && !search && values.length)
              onChange(values.slice(0, -1));
          }}
        />
        <button
          type="button"
          className="picker-toggle"
          aria-label={`Show ${label} choices`}
          onClick={() => {
            if (open) setOpen(false);
            else {
              input.current?.focus();
              setOpen(true);
            }
          }}
        >
          ▾
        </button>
      </div>
      {open && (
        <div
          id={`${id}-options`}
          role="listbox"
          aria-label={`${label} choices`}
          aria-multiselectable={multiple || undefined}
          className="picker-options"
        >
          {!options.length && <p>No matching teams</p>}
          {options.map((team, i) => (
            <button
              type="button"
              role="option"
              tabIndex={-1}
              id={`${id}-option-${team.id}`}
              key={team.id}
              aria-selected={values.includes(String(team.id))}
              className={i === index ? "highlighted" : ""}
              onMouseDown={(e) => e.preventDefault()}
              onClick={() => choose(String(team.id))}
            >
              <span>{team.name}</span>
              {values.includes(String(team.id)) && (
                <span aria-hidden="true">✓</span>
              )}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
