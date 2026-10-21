// The width rules behind the layout body classes that Shiny's app.css and
// mobile.css key on, shared by the client hook (useShinyBody) and the root
// layout. A plain module: a server component reads these as values (exports of
// a "use client" file would arrive as client references).
//
// Spec decision 9: ONE responsive design for every device. Shiny's mobile.js
// picks a layout from the user agent, so a phone and a narrow desktop window
// differ; here the layout follows the viewport width only. Mobile first: the
// phone layout is the default, widened to collapsed-navigation (below 992px)
// or desktop. Touch tooltips follow the pointer, an input capability.
export const PHONE = "(max-width: 767.98px)";
export const NARROW = "(max-width: 991.98px)";
export const TOUCH = "(hover: none) and (pointer: coarse)";

/** Snapshot "phone|narrow|touch" before the browser reports its width. */
export const PHONE_FIRST = "1|1|0";

/** The server-rendered body classes: the phone layout. */
export const PHONE_BODY_CLASSES =
  "bslib-page-navbar ibpl-device-phone ibpl-mobile ibpl-collapsed-nav ibpl-stacked-filters ibpl-touch-tips";

/** Runs first in <body>: the same width rules as the hook, before paint. */
export const BODY_CLASS_SCRIPT = `(function(){var b=document.body.classList,m=function(q){return matchMedia(q).matches};var p=m(${JSON.stringify(PHONE)}),n=m(${JSON.stringify(NARROW)}),t=m(${JSON.stringify(TOUCH)});b.remove("ibpl-device-phone");b.add("ibpl-device-"+(p?"phone":n?"tablet":"desktop"));b.toggle("ibpl-mobile",p);b.toggle("ibpl-collapsed-nav",n);b.toggle("ibpl-stacked-filters",n);b.toggle("ibpl-touch-tips",p||t);try{b.toggle("filters-collapsed",localStorage.getItem("ibpl_filters_collapsed")==="1")}catch(e){}})();`;
