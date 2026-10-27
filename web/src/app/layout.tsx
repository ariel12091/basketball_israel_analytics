import type { Metadata, Viewport } from "next";
import { Analytics } from "@vercel/analytics/next";
import { NuqsAdapter } from "nuqs/adapters/next/app";
// Shiny's stylesheets, in the order the live app loads them (spec: no
// redesign). Vendor files come from scripts/export_web_shiny_ui.R; app.css and
// mobile.css are byte-for-byte copies (npm run sync-styles, pinned by a test).
// DataTables' CSS loads last in Shiny too: the widget injects it on render.
import "@/styles/shiny/vendor/shiny.min.css";
import "@/styles/shiny/vendor/bootstrap.min.css";
import "@/styles/shiny/vendor/selectize.css";
import "@/styles/shiny/vendor/bootstrap-datepicker3.min.css";
import "@/styles/shiny/vendor/bslib-component-css.min.css";
import "@/styles/shiny/vendor/ion.rangeSlider.css";
import "@/styles/shiny/vendor/bootstrap-datepicker.css";
import "@/styles/shiny/app.css";
import "@/styles/shiny/mobile.css";
import "@/styles/shiny/vendor/jquery.dataTables.min.css";
import "@/styles/shiny/vendor/jquery.dataTables.extra.css";
import "@/styles/shiny/vendor/buttons.dataTables.min.css";
import "./web.css";
import AskProvider from "@/components/AskProvider";
import {
  BODY_CLASS_SCRIPT,
  PHONE_BODY_CLASSES,
} from "@/components/shiny/layoutClasses";

export const metadata: Metadata = {
  title: "IBPL Analytics",
  description: "Basketball game logs for Israel, EuroLeague and EuroCup",
};
export const viewport: Viewport = { width: "device-width", initialScale: 1 };

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <head>
        {/* The same font and icon sources as shared_head_tags() in global.R. */}
        <link
          rel="stylesheet"
          href="https://fonts.googleapis.com/css2?family=Archivo:wdth,wght@75..112,500..800&family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&family=Inter:wght@400;500;600;700&family=Heebo:wght@400;500;600;700&display=swap"
        />
        <link
          rel="stylesheet"
          href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css"
        />
      </head>
      {/* Mobile first: the phone layout is the default; the script widens it
          from the viewport before first paint (one design for every device). */}
      <body className={PHONE_BODY_CLASSES} suppressHydrationWarning>
        <script dangerouslySetInnerHTML={{ __html: BODY_CLASS_SCRIPT }} />
        <NuqsAdapter>
          <AskProvider
            enabled={process.env.ASK_ENABLED === "true"}
            followups={process.env.ASK_FOLLOWUPS === "true"}
          >
            {children}
          </AskProvider>
        </NuqsAdapter>
        <Analytics />
      </body>
    </html>
  );
}
