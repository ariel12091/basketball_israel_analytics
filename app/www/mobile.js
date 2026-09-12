/* mobile.js -- viewport-driven mobile presentation layer.

   Loaded after app.js so it can override behaviour without editing it. Every
   rule in mobile.css is scoped to the body class this file sets, so there is
   exactly one answer to "are we in mobile mode" and the CSS and JS cannot
   disagree about it.

   ES5 and one IIFE per concern, matching app.js. */

window.IBPL_MOBILE_MQ = "(max-width: 767.98px)";

(function () {
  var BODY_CLASS = "ibpl-mobile";
  var last = null;

  function isMobile() {
    if (!window.matchMedia) return false;
    return window.matchMedia(window.IBPL_MOBILE_MQ).matches;
  }

  function applyMode() {
    var on = isMobile();
    // Only announce real transitions. resize fires continuously on a phone
    // when the URL bar collapses, and every listener downstream redraws tables.
    if (on === last) return;
    last = on;
    document.body.classList.toggle(BODY_CLASS, on);
    document.dispatchEvent(new CustomEvent("ibpl:mobilechange", {
      detail: { mobile: on }
    }));
  }

  function init() {
    applyMode();
    if (window.matchMedia) {
      var mq = window.matchMedia(window.IBPL_MOBILE_MQ);
      // addEventListener on a MediaQueryList is unsupported in older Safari,
      // where addListener is the only option.
      if (mq.addEventListener) {
        mq.addEventListener("change", applyMode);
      } else if (mq.addListener) {
        mq.addListener(applyMode);
      }
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
