import type {Row} from '../rules/gameLogs';
/** docs/shared_definitions.md §3: text only; numeric negatives remain numeric. */
export function defuseCsvCell(s:string){return /^[=+\-@\t\r]/.test(s)?"'"+s:s;}
export function toCsv(rows:Row[],columns:string[]):string{const cell=(v:unknown)=>{const text=v==null?'':typeof v==='string'?defuseCsvCell(v):String(v);return /[",\r\n]/.test(text)?'"'+text.replace(/"/g,'""')+'"':text;};return [columns.map(cell).join(','),...rows.map(r=>columns.map(c=>cell(r[c])).join(','))].join('\r\n')+'\r\n';}
