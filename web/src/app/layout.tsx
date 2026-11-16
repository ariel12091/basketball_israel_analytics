import type { Metadata, Viewport } from "next";
import { Archivo, DM_Sans, Heebo, Inter, JetBrains_Mono } from "next/font/google";
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
import "./light.css";
import AskProvider from "@/components/AskProvider";
import BodyClassScript from "@/components/shiny/BodyClassScript";
import ThemeScript from "@/components/shiny/ThemeScript";
import { PHONE_BODY_CLASSES } from "@/components/shiny/layoutClasses";

// Shiny's fonts (shared_head_tags() in global.R), self-hosted by next/font:
// no request to Google from the visitor's browser, and size-matched
// fallbacks so text does not jump when the font arrives. web.css points
// app.css's --ibpl-font-* variables at these.
const archivo = Archivo({
  subsets: ["latin"],
  axes: ["wdth"],
  variable: "--font-archivo",
});
const dmSans = DM_Sans({ subsets: ["latin"], variable: "--font-dm-sans" });
const inter = Inter({
  subsets: ["latin"],
  variable: "--font-inter",
  preload: false,
});
const mono = JetBrains_Mono({
  subsets: ["latin"],
  variable: "--font-mono",
  preload: false,
});
const heebo = Heebo({
  subsets: ["hebrew"],
  variable: "--font-heebo",
  preload: false,
});
const FONT_VARS = [archivo, dmSans, inter, mono, heebo]
  .map((f) => f.variable)
  .join(" ");

export const metadata: Metadata = {
  title: { default: "IBPL Analytics", template: "%s | IBPL Analytics" },
  description: "Basketball game logs for Israel, EuroLeague and EuroCup",
};
// viewport-fit=cover: on iPhones the page reaches the bottom edge, so the
// phone tab bar (web.css, padded by env(safe-area-inset-bottom)) sits flush
// with it instead of floating above an empty band.
export const viewport: Viewport = {
  width: "device-width",
  initialScale: 1,
  viewportFit: "cover",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    // The theme script sets data-theme and colorScheme on <html> before
    // hydration, hence suppressHydrationWarning.
    <html lang="en" className={FONT_VARS} suppressHydrationWarning>
      <head>
        <ThemeScript />
        {/* The same icon source as shared_head_tags() in global.R. */}
        {/* Pinned version, so its hash never changes (checked against
            jsDelivr's published file hash, 2026-11-15). */}
        <link
          rel="stylesheet"
          href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css"
          integrity="sha384-XGjxtQfXaH2tnPFa9x+ruJTuLE3Aa6LhHSWRr1XeTyhezb4abCG4ccI5AkVDxqC+"
          crossOrigin="anonymous"
          referrerPolicy="no-referrer"
        />
      </head>
      {/* Mobile first: the phone layout is the default; the script widens it
          from the viewport before first paint (one design for every device). */}
      <body className={PHONE_BODY_CLASSES} suppressHydrationWarning>
        <BodyClassScript />
        <NuqsAdapter>
          <AskProvider
            enabled={process.env.ASK_ENABLED === "true"}
            followups={process.env.ASK_FOLLOWUPS === "true"}
          >
            {children}
          </AskProvider>
        </NuqsAdapter>
        <footer className="ibpl-site-credits">
          <a
            href="https://www.flaticon.com/free-icons/basketball"
            title="basketball icons"
          >
            Basketball icons created by imaginationlol - Flaticon
          </a>
        </footer>
        {/* Vercel Web Analytics (cookieless page views). Only on Vercel:
            elsewhere its /_vercel/insights script does not exist. */}
        {process.env.VERCEL === "1" && <Analytics />}
      </body>
    </html>
  );
}
