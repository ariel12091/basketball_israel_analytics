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

/* ---- Mobile navigation relocation ---------------------------------------
   The fixed cluster is supplied through navbarPage(header = ...) outside the
   collapsed menu. Positioning it statically does not put it under the burger,
   so move the existing node into the collapse and restore it on desktop.

   View-mode selection itself is NOT handled here any more (see R4 in the
   2026-09-13 rework brief): promoting the radios/select above the table read
   as a stray form control on a phone. The burger menu now carries the same
   view-mode choices through the existing per-tab hover menu (app.js CFG,
   ".tab-hover-menu") -- see the CSS in mobile.css that makes the ACTIVE tab's
   menu render in-flow instead of on hover. Nothing here drives that; it
   already calls Shiny.setInputValue on the real inputs.
   ----------------------------------------------------------------------- */
(function () {
  var clusterHome = null;

  function relocateCluster(on) {
    var cluster = document.getElementById("navbar_right_cluster");
    if (!cluster || !cluster.parentNode) return;
    if (!clusterHome) {
      clusterHome = document.createComment("navbar cluster home");
      cluster.parentNode.insertBefore(clusterHome, cluster);
    }
    if (on) {
      var tabs = document.getElementById("main_tabs");
      var collapse = tabs && tabs.closest(".navbar-collapse");
      if (collapse && cluster.parentNode !== collapse) collapse.appendChild(cluster);
    } else if (clusterHome.parentNode && cluster.parentNode !== clusterHome.parentNode) {
      clusterHome.parentNode.insertBefore(cluster, clusterHome.nextSibling);
    }
  }

  function sync() {
    var on = document.body.classList.contains("ibpl-mobile");
    relocateCluster(on);
  }

  document.addEventListener("ibpl:mobilechange", sync);
  if (window.jQuery) window.jQuery(document).on("shown.bs.tab shiny:value", sync);
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", sync);
  } else {
    sync();
  }
})();

/* ---- Bottom sheet ------------------------------------------------------
   One component, one remaining user: the column header tooltip text (the
   filter panel and the stat-filter popover used to route through this too,
   before the 2026-09-13 rework made both inline instead -- see R2 and R3
   below). Kept as-is: still the only "floating over the page" surface this
   layer uses, for content with nothing sensible to push inline against.

   The sheet MOVES the existing node rather than cloning it, so every Shiny
   input keeps its binding and its id. Cloning would register duplicate ids and
   silently break the filters.
   --------------------------------------------------------------------- */
window.IBPL_MOBILE_SHEET = (function () {
  var sheet = null, backdrop = null, body = null, head = null;
  var origin = null, content = null;

  function build() {
    if (sheet) return;
    backdrop = document.createElement("div");
    backdrop.className = "ibpl-m-sheet-backdrop";
    backdrop.addEventListener("click", close);

    sheet = document.createElement("div");
    sheet.className = "ibpl-m-sheet";
    sheet.setAttribute("role", "dialog");
    sheet.setAttribute("aria-modal", "true");

    head = document.createElement("div");
    head.className = "ibpl-m-sheet-head";

    body = document.createElement("div");
    body.className = "ibpl-m-sheet-body";

    var foot = document.createElement("div");
    foot.className = "ibpl-m-sheet-foot";
    var done = document.createElement("button");
    done.type = "button";
    done.className = "btn btn-warning w-100";
    done.textContent = "Apply";
    done.addEventListener("click", close);
    foot.appendChild(done);

    sheet.appendChild(head);
    sheet.appendChild(body);
    sheet.appendChild(foot);
    document.body.appendChild(backdrop);
    document.body.appendChild(sheet);
  }

  function open(title, node) {
    build();
    close();
    // close() returns a MOVED node to its origin, but it cannot know about
    // content that was appended directly (Task 6 injects tooltip text that
    // way). Without this, that text accumulates across opens and then shows
    // up above the filter panel on the next open.
    body.innerHTML = "";
    head.textContent = title || "";
    if (node) {
      // Remember exactly where it was so close() can put it back.
      origin = { parent: node.parentNode, next: node.nextSibling };
      content = node;
      body.appendChild(node);
    }
    document.body.classList.add("ibpl-m-sheet-open");
  }

  function close() {
    // document.contains guard: same precedent as the stat-filter-add
    // restore() further down this file -- a detached subtree still has a
    // parent, and inserting the live panel into one would remove it from
    // the page entirely.
    if (content && origin && origin.parent && document.contains(origin.parent)) {
      origin.parent.insertBefore(content, origin.next);
    } else if (content && content.parentNode) {
      // The origin is gone -- its container was replaced wholesale, not just
      // moved. This guarded a renderUI-backed consumer (the stat-filter
      // popover, before R3 gave it its own inline component below) whose
      // container regenerated its own trigger+content on every filter
      // add/remove, so a held copy's origin container could be detached out
      // from under it. Leaving such an orphan attached (still Shiny-bound,
      // ids intact) would sit alongside a freshly auto-bound duplicate Shiny
      // just rendered in the container's natural spot -- verified live via
      // Shiny's own "IDs were repeated" console warning at the time.
      // Removing it outright here closes that window immediately instead of
      // deferring the cleanup, and stays correct for any future consumer
      // shaped the same way.
      content.parentNode.removeChild(content);
    }
    content = null;
    origin = null;
    document.body.classList.remove("ibpl-m-sheet-open");
  }

  function isOpen() {
    return document.body.classList.contains("ibpl-m-sheet-open");
  }

  document.addEventListener("keydown", function (e) {
    if (e.key === "Escape" && isOpen()) close();
  });

  // If the viewport crosses the breakpoint while the sheet is open (rotation,
  // a foldable, a devtools resize), body.ibpl-mobile is removed but nothing
  // else here would be -- the sheet's base rule (display: none) would then
  // hide it with the moved filter panel trapped inside, leaving the desktop
  // sidebar empty until Escape is pressed. Route through the normal close()
  // so the panel is restored to its origin like every other path.
  document.addEventListener("ibpl:mobilechange", function (e) {
    if (!e.detail || !e.detail.mobile) close();
  });

  // Kept for any future consumer shaped like the stat-filter popover used to
  // be: a renderUI output that regenerates its own trigger+content on every
  // change (output$*_filter_chips did, for the chip list and the "+ Filter"
  // popover together). Shiny replaces such a container's innerHTML
  // wholesale; a copy moved into the sheet body would no longer be among its
  // children at that point, so it would survive the replace as an orphan
  // while a fresh duplicate-id copy is auto-bound in the container's natural
  // spot -- verified live (two `.on-stat-popover` nodes at once, one
  // "in-sheet" and one "natural", after a single Add, before R3 moved that
  // consumer to its own inline component below). shiny:value fires (bubbled,
  // jQuery-delegated -- see the existing "shown.bs.tab shiny:value"
  // listeners below) on the output whenever a renderUI finishes; if it just
  // detached what the sheet was holding, origin.parent is no longer in the
  // document -- close() already no-ops safely on that (the document.contains
  // guard above), it just needs triggering here instead of waiting for
  // Escape or the next open().
  if (window.jQuery) {
    window.jQuery(document).on("shiny:value", function () {
      if (isOpen() && origin && origin.parent && !document.contains(origin.parent)) {
        close();
      }
    });
  }

  return { open: open, close: close, isOpen: isOpen };
})();

/* ---- Filter panel: inline, never an overlay -----------------------------
   R2 in the 2026-09-13 rework brief reverses the sheet transform this task
   used to apply to every "-filters" panel: "show filters should never be a
   popup". Nothing in this file intercepts the toggle any more --
   Bootstrap's own collapse plugin owns show/hide exactly as it did before
   this whole layer existed, expanding the panel in the page flow and
   pushing the table down. Only the expanded panel's mobile styling
   (full-width controls, tap targets) lives here now, in mobile.css.
   --------------------------------------------------------------------- */

/* ---- Tooltips into the sheet --------------------------------------------
   Two mechanisms, two selectors:
   - th[title]       native title, written by HEADER_TOOLTIP_JS (global.R:212)
   - [data-tooltip]  CSS ::after on :hover, written by tt() (global.R:209)
   The stat-filter popover (helpers.R:546) used to be a third sheet user; R3
   moved it to its own inline component below instead (see "Stat filter
   add: inline, not a sheet").
   --------------------------------------------------------------------- */
(function () {
  function textSheet(title, text) {
    var p = document.createElement("div");
    p.className = "ibpl-m-tip-text";
    p.textContent = text;
    window.IBPL_MOBILE_SHEET.open(title, null);
    document.querySelector(".ibpl-m-sheet-body").appendChild(p);
  }

  // DataTables re-renders the header on every draw, so the affordance has to
  // be re-injected on every draw too. HEADER_TOOLTIP_JS (global.R:212)
  // writes the native title attribute; this only reads it.
  function addInfoMarks() {
    if (!document.body.classList.contains("ibpl-mobile")) return;
    var ths = document.querySelectorAll("table.dataTable thead th[title]");
    for (var i = 0; i < ths.length; i++) {
      if (ths[i].querySelector(".ibpl-m-th-info")) continue;
      var b = document.createElement("button");
      b.type = "button";
      b.className = "ibpl-m-th-info";
      b.setAttribute("aria-label", "What this column means");
      b.textContent = "i";
      ths[i].appendChild(b);
    }
  }

  function removeInfoMarks() {
    var marks = document.querySelectorAll(".ibpl-m-th-info");
    for (var i = 0; i < marks.length; i++) {
      if (marks[i].parentNode) marks[i].parentNode.removeChild(marks[i]);
    }
  }

  function bind() {
    if (!window.jQuery) return;
    var $ = window.jQuery;

    $(document).on("draw.dt", addInfoMarks);
    document.addEventListener("ibpl:mobilechange", function (e) {
      if (e.detail && e.detail.mobile) addInfoMarks();
      else removeInfoMarks();
    });

    // [data-tooltip] stays bubble phase and skips preventDefault(): several
    // tt() labels wrap a real checkbox/radio <input> (e.g. ts_clutch_enabled),
    // and clicking anywhere in that <label> is what toggles it -- a browser
    // default action, decided only by whether ANY listener called
    // preventDefault() during the whole dispatch, independent of phase order.
    // Calling it here suppressed the toggle entirely (verified live: tapping
    // "Enable clutch filter" left the checkbox unchecked and, worse, closed
    // the already-open Filters sheet to show a tooltip instead). Omitting it
    // lets the native toggle proceed exactly as before; the sheet opens too,
    // which is a bonus, not a conflict, since nothing here owns "toggle vs.
    // explain" -- both can happen from one tap.
    //
    // isTrusted guard added for R4 (2026-09-13 rework): a view-mode choice's
    // <label> (e.g. onoff_view_mode's "Four Factors") is itself a
    // data-tooltip target ("eFG%, OREB%, TOV%, FTR breakdown"), and the
    // burger-menu row for that choice drives the real radio with a
    // synthetic radio.click() (app.js updateInput()) -- a click on the
    // <input> bubbles through its ancestor <label>, straight into this
    // delegated handler, popping the tooltip sheet open on every mode
    // switch made from the menu. Verified live (reproduced with a capture
    // logger: the four expected synthetic clicks were burger-toggle,
    // thm-item, nav-link and the radio input -- none of them a real tap on
    // the tooltip label -- yet the sheet opened anyway). A real finger tap
    // on a tooltip-bearing label is always isTrusted; app.js's own
    // programmatic .click()/.setValue() calls never are, so this excludes
    // exactly the synthetic case without touching the real one.
    $(document).on("click", "[data-tooltip]", function (e) {
      if (!document.body.classList.contains("ibpl-mobile")) return;
      if (!e.isTrusted) return;
      var tip = this.getAttribute("data-tooltip");
      if (!tip) return;
      textSheet((this.textContent || "").trim(), tip);
    });

    // CAPTURE phase, not $(document).on() (bubble phase): a column header's
    // tap already means SORT, in DataTables' own click.DT listener bound
    // DIRECTLY on the th (verified: $._data(th, 'events') lists 'click').
    // That listener fires at the target phase, before a bubble-phase
    // document handler ever runs, so e.stopPropagation() there is too late
    // -- it only stops the event reaching document, after DataTables already
    // reacted. Verified live: an info-dot tap toggled the column's sort
    // order every time, in addition to opening the tooltip sheet. Capturing
    // on document intercepts before the event ever reaches the th, so
    // DataTables' listener never runs. (The stat-filter popover trigger used
    // to share this exact hazard with Bootstrap's own popover toggle; its
    // capture-phase handler now lives with the rest of that component below.)
    document.addEventListener("click", function (e) {
      if (!document.body.classList.contains("ibpl-mobile")) return;
      var $info = $(e.target).closest(".ibpl-m-th-info");
      if (!$info.length) return;
      var th = $info.get(0).parentNode;
      var tip = th ? th.getAttribute("title") : "";
      // Guard BEFORE intercepting: addInfoMarks() only ever creates this
      // button inside a th[title], so tip is empty only if the title was
      // removed after the fact. When there's nothing to show, don't eat
      // the click either -- let it fall through to the normal sort tap
      // instead of silently swallowing it for no reason.
      if (!tip) return;
      e.preventDefault();
      e.stopPropagation();
      var label = (th.textContent || "").replace(/\s*i\s*$/, "").trim();
      textSheet(label, tip);
    }, true);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bind);
  } else {
    bind();
  }
})();

/* ---- Stat filter add: inline, not a sheet -------------------------------
   R3 in the 2026-09-13 rework brief: render the "+ Filter" controls inline
   with the other filter chips instead of relocating them into the bottom
   sheet.

   bslib's popover() does NOT leave its content inert inside the <template>
   the server-rendered HTML shows -- its custom element (<bslib-popover>)
   moves the content out into a real, but display:none, <div> sitting next
   to the trigger the moment the element upgrades (verified live: the panel
   is reachable by plain document.querySelector before any interaction).
   Bootstrap only clones THAT div's content into a floating .popover box
   when its own show() runs. So "suppress the popover at the source" is
   exactly the capture-phase interception this file already uses for
   .ibpl-m-th-info: stopping the click before it ever reaches the trigger
   means Bootstrap's show() is never called and nothing is ever built to
   leak, and the div itself can just be moved (not cloned) inline -- the
   same "move, don't clone" rule as the sheet, for the same reason: the
   selectInput / radioButtons / numericInput inside it keep their ids, so
   apply_stat_filters() and every server observer keep working untouched.

   Desktop must keep working exactly as before, so a moved panel is always
   restored to its original spot in <bslib-popover> before mobile mode is
   left -- same origin-tracking shape as IBPL_MOBILE_SHEET.close().

   The chips row that hosts the trigger re-renders wholesale on every
   Add/Remove (output$*_filter_chips is one renderUI covering the whole
   chip list, helpers.R:520 stat_filter_chips_ui()), which rebuilds a fresh
   <bslib-popover> carrying the SAME ids as whatever this component is
   currently holding inline -- the same duplicate-id hazard Task 6 hit with
   the sheet. Detect it the same way: when shiny:value fires and the held
   panel's origin is no longer in the document, the container was replaced
   out from under it, so discard the now-orphaned copy instead of leaving a
   stale duplicate-id form on screen.
   --------------------------------------------------------------------- */
(function () {
  var origins = {}; // prefix -> { parent, next }

  function wrapFor(prefix) {
    return document.querySelector('[data-ibpl-m-filteradd="' + prefix + '"]');
  }

  function restore(prefix) {
    var wrap = wrapFor(prefix);
    if (!wrap) return;
    var panel = wrap.querySelector("." + prefix + "-stat-popover");
    var origin = origins[prefix];
    // document.contains guard: same precedent as the sheet's close() -- a
    // detached subtree still has a parent, and inserting the live panel
    // into one would remove it from the page entirely.
    if (panel && origin && origin.parent && document.contains(origin.parent)) {
      origin.parent.insertBefore(panel, origin.next);
    }
    delete origins[prefix];
    if (wrap.parentNode) wrap.parentNode.removeChild(wrap);
  }

  function open(trigger) {
    var id = trigger.id || "";
    var prefix = id.replace(/_stat_filter_add_btn$/, "");
    var panel = document.querySelector("." + prefix + "-stat-popover");
    if (!panel) return;

    var wrap = document.createElement("div");
    wrap.className = "ibpl-m-filteradd";
    wrap.setAttribute("data-ibpl-m-filteradd", prefix);
    origins[prefix] = { parent: panel.parentNode, next: panel.nextSibling };
    wrap.appendChild(panel);
    trigger.parentNode.insertBefore(wrap, trigger.nextSibling);
  }

  function bind() {
    if (!window.jQuery) return;
    var $ = window.jQuery;

    // CAPTURE phase, same shape and same reason as .ibpl-m-th-info above:
    // Bootstrap binds its popover show/hide directly on the trigger
    // (data-bs-toggle="popover"), closer to the target than a bubble-phase
    // document handler, so stopPropagation() there always runs too late.
    document.addEventListener("click", function (e) {
      if (!document.body.classList.contains("ibpl-mobile")) return;
      var $trigger = $(e.target).closest(".filter-chip-add");
      if (!$trigger.length) return;
      e.preventDefault();
      e.stopPropagation();
      var trigger = $trigger.get(0);
      var id = trigger.id || "";
      var prefix = id.replace(/_stat_filter_add_btn$/, "");
      if (wrapFor(prefix)) {
        restore(prefix); // Tap again to collapse it back.
      } else {
        open(trigger);
      }
    }, true);

    $(document).on("shiny:value", function () {
      Object.keys(origins).forEach(function (prefix) {
        var origin = origins[prefix];
        if (origin && origin.parent && !document.contains(origin.parent)) {
          var wrap = wrapFor(prefix);
          if (wrap && wrap.parentNode) wrap.parentNode.removeChild(wrap);
          delete origins[prefix];
        }
      });
    });

    // Crossing the breakpoint mid-open must restore every held panel to its
    // <bslib-popover>, or the desktop popover would show empty content.
    document.addEventListener("ibpl:mobilechange", function (e) {
      if (e.detail && e.detail.mobile) return;
      Object.keys(origins).forEach(restore);
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bind);
  } else {
    bind();
  }
})();
