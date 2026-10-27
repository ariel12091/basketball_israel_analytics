// Copy Shiny's own stylesheets into the web app, byte for byte.
// The migration keeps the look of the Shiny app (spec: "No redesign"), so the
// web pages use app/www/app.css and mobile.css themselves rather than a copy
// that would drift. src/styles/shiny.test.ts fails when these copies differ
// from the originals; run `npm run sync-styles` after editing either file.
import { copyFileSync, mkdirSync } from "node:fs";
import { fileURLToPath } from "node:url";

const from = (name) => fileURLToPath(new URL(`../../app/www/${name}`, import.meta.url));
const to = (name) => fileURLToPath(new URL(`../src/styles/shiny/${name}`, import.meta.url));
mkdirSync(fileURLToPath(new URL("../src/styles/shiny/", import.meta.url)), { recursive: true });
for (const name of ["app.css", "mobile.css"]) copyFileSync(from(name), to(name));
console.log("copied app.css and mobile.css");
