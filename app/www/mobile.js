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
   The view-mode inputs (radios, or Player Stats' select) are also moved above
   each table on mobile. Preserve their sidebar positions with placeholders
   for the desktop transition.
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

  function promote(on) {
    var panes = document.querySelectorAll(".tab-pane");
    for (var i = 0; i < panes.length; i++) {
      var group = panes[i].querySelector(".view-mode-container");
      if (!group) {
        // Player Stats uses a hidden selectInput instead of view-mode radios.
        // Move the input container, not its display:none wrapper, so it can be
        // used on mobile while keeping the existing Shiny binding and value.
        var select = panes[i].querySelector("#ts_display_mode");
        group = select && select.closest(".shiny-input-container");
      }
      // Compare's mode radios (#cmp_mode) are hidden by app.css:969
      // (#cmp_mode.shiny-input-radiogroup { display: none !important; }) and
      // are not wrapped in .view-mode-container, so neither case above finds
      // them. Move the radio group itself, mirroring the Player Stats select
      // case above -- the existing placeholder/restore logic below handles it
      // without cloning the live Shiny input.
      if (!group) group = panes[i].querySelector("#cmp_mode");
      if (!group) continue;
      var main = panes[i].querySelector(".col-sm-9, .col-md-9, [role='main']");
      if (!main) continue;

      if (on) {
        if (group.getAttribute("data-ibpl-m-home")) continue;
        var holder = document.createElement("div");
        holder.className = "ibpl-m-viewmode";
        holder.setAttribute("data-ibpl-m-holder", "1");
        // Leave a placeholder in the sidebar. Moving the holder itself would
        // lose the original parent and restore the radios into the main panel.
        var home = document.createElement("span");
        home.style.display = "none";
        group.parentNode.insertBefore(home, group);
        holder.ibplHome = home;
        group.setAttribute("data-ibpl-m-home", "1");
        holder.appendChild(group);
        main.insertBefore(holder, main.firstChild);
      } else if (group.getAttribute("data-ibpl-m-home")) {
        var oldHolder = group.parentNode;
        group.removeAttribute("data-ibpl-m-home");
        if (oldHolder && oldHolder.getAttribute("data-ibpl-m-holder")) {
          var original = oldHolder.ibplHome;
          // document.contains, not just parentNode: a detached subtree still
          // has a parent, and inserting the live radios into one would remove
          // them from the page entirely.
          if (original && original.parentNode && document.contains(original)) {
            original.parentNode.insertBefore(group, original);
            original.parentNode.removeChild(original);
          } else {
            // A sidebar may have been re-rendered while the group was away.
            // Keep the live input in the page even if its marker disappeared.
            oldHolder.parentNode.insertBefore(group, oldHolder);
          }
          oldHolder.parentNode.removeChild(oldHolder);
        }
      }
    }
  }

  function sync() {
    var on = document.body.classList.contains("ibpl-mobile");
    relocateCluster(on);
    promote(on);
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
   One component, three users: the filter panel (this task), the tooltip text
   and the stat-filter popover (Task 6).

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
    // document.contains guard: same precedent as the view-mode restore at
    // mobile.js:392 -- a detached subtree still has a parent, and inserting
    // the live panel into one would remove it from the page entirely.
    if (content && origin && origin.parent && document.contains(origin.parent)) {
      origin.parent.insertBefore(content, origin.next);
    } else if (content && content.parentNode) {
      // The origin is gone -- its container was replaced wholesale, not just
      // moved. Task 6's stat-filter popover lives inside a renderUI that
      // regenerates its own trigger+content on every filter add/remove, so a
      // held copy's origin container is routinely detached out from under it.
      // Leaving this orphan attached (still Shiny-bound, ids intact) sits
      // alongside the freshly auto-bound duplicate Shiny just rendered in the
      // container's natural spot -- verified live via Shiny's own "IDs were
      // repeated" console warning, incrementing with every add until the next
      // open() wiped it via body.innerHTML="". Removing it outright here
      // closes that window immediately instead of deferring the cleanup.
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

  // Task 6's stat-filter popover lives inside a renderUI output that
  // regenerates its own trigger+content on every filter add/remove
  // (output$*_filter_chips, since the chip list and the "+ Filter" popover
  // share one output). Shiny replaces that container's innerHTML wholesale;
  // the copy WE moved into the sheet body is no longer among its children at
  // that point, so it survives the replace as an orphan while a fresh
  // duplicate-id copy is auto-bound in the container's natural spot --
  // verified live (two `.on-stat-popover` nodes at once, one "in-sheet" and
  // one "natural", after a single Add). Left open, the sheet keeps showing
  // the stale orphan. shiny:value fires (bubbled, jQuery-delegated -- see the
  // existing "shown.bs.tab shiny:value" listeners below) on the output
  // whenever a renderUI finishes; if it just detached what we were holding,
  // origin.parent is no longer in the document -- close() already no-ops
  // safely on that (the document.contains guard above), it just needs
  // triggering here instead of waiting for Escape or the next open().
  if (window.jQuery) {
    window.jQuery(document).on("shiny:value", function () {
      if (isOpen() && origin && origin.parent && !document.contains(origin.parent)) {
        close();
      }
    });
  }

  return { open: open, close: close, isOpen: isOpen };
})();

/* ---- Filter panel into the sheet --------------------------------------
   All 11 tabs use the same toggle shape, so this is one selector rather than
   11 R edits. Bootstrap's collapse still owns show/hide, so the button, its
   aria-expanded state and the chips-bar wiring are untouched.
   --------------------------------------------------------------------- */
(function () {
  var SEL = '[data-bs-toggle="collapse"][data-bs-target$="-filters"]';

  function bind() {
    if (!window.jQuery) return;
    window.jQuery(document).on("click", SEL, function (e) {
      if (!document.body.classList.contains("ibpl-mobile")) return;
      e.preventDefault();
      e.stopPropagation();   // Do not let Bootstrap also toggle the collapse.
      var target = document.querySelector(this.getAttribute("data-bs-target"));
      if (!target) return;
      if (window.IBPL_MOBILE_SHEET.isOpen()) {
        window.IBPL_MOBILE_SHEET.close();
        return;
      }
      window.IBPL_MOBILE_SHEET.open("Filters", target);
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bind);
  } else {
    bind();
  }
})();

/* ---- Tooltips and the stat-filter popover into the sheet ---------------
   Three mechanisms, three selectors:
   - th[title]       native title, written by HEADER_TOOLTIP_JS (global.R:212)
   - [data-tooltip]  CSS ::after on :hover, written by tt() (global.R:209)
   - the bslib popover at helpers.R:546, whose Shiny input ids must survive
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
  // be re-injected the same way the caret is. HEADER_TOOLTIP_JS (global.R:212)
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
    $(document).on("click", "[data-tooltip]", function (e) {
      if (!document.body.classList.contains("ibpl-mobile")) return;
      var tip = this.getAttribute("data-tooltip");
      if (!tip) return;
      textSheet((this.textContent || "").trim(), tip);
    });

    // CAPTURE phase, not $(document).on() (bubble phase), for both of the
    // handlers below -- same hazard the caret already solved (mobile.js
    // applyAll's click handler, "Capture phase, not $(document).on()").
    //
    // .ibpl-m-th-info: a column header's tap already means SORT, in
    // DataTables' own click.DT listener bound DIRECTLY on the th (verified:
    // $._data(th, 'events') lists 'click'). That listener fires at the target
    // phase, before a bubble-phase document handler ever runs, so
    // e.stopPropagation() there is too late -- it only stops the event
    // reaching document, after DataTables already reacted. Verified live: an
    // info-dot tap toggled the column's sort order every time, in addition to
    // opening the tooltip sheet. Capturing on document intercepts before the
    // event ever reaches the th, so DataTables' listener never runs.
    //
    // .filter-chip-add: the same shape, with Bootstrap's popover in place of
    // DataTables. bslib's trigger carries data-bs-toggle="popover", and
    // Bootstrap binds its show/hide toggle directly on the trigger element.
    // At bubble phase that listener already ran by the time our handler got a
    // chance to stopPropagation() -- verified live: the panel (normally
    // sitting inert in a display:none template inside <bslib-popover>) had
    // already been moved into a freshly built, positioned `.popover` box
    // appended to body. Moving it again from there into the sheet still
    // "worked" while the sheet was open (the sheet's ibpl-m-sheet-open CSS
    // hides any `.popover`), but close() returns the panel to wherever it
    // last was -- Bootstrap's now-empty `.popover` box, not the template --
    // and that box is still marked shown (class "popover ... show",
    // display:block) with no CSS masking it once ibpl-m-sheet-open is gone.
    // Verified live: after closing the sheet, a fully visible floating
    // popover was left sitting on screen. Capturing ahead of Bootstrap's
    // listener means show() is never called, so nothing is ever built to
    // leak -- the panel only ever moves via this component's own open/close.
    document.addEventListener("click", function (e) {
      if (!document.body.classList.contains("ibpl-mobile")) return;
      var $info = $(e.target).closest(".ibpl-m-th-info");
      if ($info.length) {
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
        return;
      }

      var $trigger = $(e.target).closest(".filter-chip-add");
      if ($trigger.length) {
        e.preventDefault();
        e.stopPropagation();
        var id = $trigger.attr("id") || "";
        var prefix = id.replace(/_stat_filter_add_btn$/, "");
        // The popover BODY is moved, not cloned, so the selectInput /
        // radioButtons / numericInput keep their ids and every server
        // observer and apply_stat_filters() call keeps working.
        var panel = document.querySelector("." + prefix + "-stat-popover");
        if (!panel) return;
        window.IBPL_MOBILE_SHEET.open("Add stat filter", panel);
      }
    }, true);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bind);
  } else {
    bind();
  }
})();
