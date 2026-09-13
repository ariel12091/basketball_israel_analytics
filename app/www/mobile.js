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

/* ---- Mobile table layer -------------------------------------------------
   Wide tables (up to 18 visible columns) are unusable on a phone. Keep a few
   priority columns and move the rest into a DataTables child row behind a
   caret.

   A caret rather than a row tap because Compare binds
   table.on('click', 'tbody tr', ...) directly on its tables
   (server_tab7_compare.R:3324, :3980) for its own team/lineup detail views.
   (Tab 2's lineup modal is not a row tap -- input$ld_lineup_click is set by
   an onclick on an <a class="ld-lineup-link"> inside the Players column,
   one of the 3 columns this layer keeps, so it was never at risk from
   column hiding either way. Compare alone is reason enough for a caret.)

   Re-applied on every draw: DataTables re-renders every cell on sort, page and
   filter, so nothing can be stored on a cell.
   ----------------------------------------------------------------------- */
window.IBPL_MOBILE_TABLE = {
  // Ordered preference lists keyed by DT output id. Matching is by column
  // HEADER NAME: one output id serves several view modes with different column
  // sets, so a name list simply fails to match in the wrong mode and falls
  // back to the default.
  priority: {
    // Compare's columns are #, Team|Player, GP A, A, Total Poss A, GP B, B,
    // Total Poss B, Gap. The default "first 3 visible" would yield
    // #, Player, GP A -- deleting both compared values and the gap, which is
    // the entire point of the tab. Team and Player are alternatives: Teams
    // mode has one, Players mode the other, and only the present one matches.
    cmp_table: ["Team", "Player", "A", "B", "Gap"]
  }
};

(function () {
  var DEFAULT_KEEP = 3;
  var MAX_KEEP = 4;
  var applying = false;

  function $() { return window.jQuery; }

  function escapeHtml(s) {
    return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;")
      .replace(/>/g, "&gt;").replace(/"/g, "&quot;");
  }

  function isMobile() {
    return document.body.classList.contains("ibpl-mobile");
  }

  function headerNames(api) {
    var out = [];
    api.columns().every(function () {
      var th = this.header();
      out[this.index()] = th ? (th.textContent || "").trim() : "";
    });
    return out;
  }

  function outputIdOf(node) {
    var wrap = $()(node).closest(".datatables");
    return wrap.length ? (wrap.attr("id") || "") : "";
  }

  // The set of columns visible the first time we see this table. Columns the R
  // side hid on purpose (the 16 raw shooting columns, the PR fields) must stay
  // hidden, so "restore" means restore to THIS set, never visible(true) on all.
  function origVisible(node, api) {
    var $node = $()(node);
    var rec = $node.data("ibplOrigVisible");
    if (rec) return rec;
    rec = [];
    api.columns().every(function () { if (this.visible()) rec.push(this.index()); });
    $node.data("ibplOrigVisible", rec);
    return rec;
  }

  function keepSet(api, outputId, orig) {
    var names = headerNames(api);
    var pref = window.IBPL_MOBILE_TABLE.priority[outputId];
    var keep = [];
    var i;

    if (pref) {
      for (i = 0; i < pref.length && keep.length < MAX_KEEP; i++) {
        var idx = names.indexOf(pref[i]);
        if (idx >= 0 && orig.indexOf(idx) >= 0) keep.push(idx);
      }
      // Fewer than two matches means this override belongs to a different view
      // mode of the same output. Fall through to the default rather than
      // rendering a one-column table.
      if (keep.length >= 2) return keep;
      keep = [];
    }

    for (i = 0; i < orig.length && keep.length < DEFAULT_KEEP; i++) keep.push(orig[i]);
    return keep;
  }

  function detailHtml(api, rowIdx, keep, orig) {
    var names = headerNames(api);
    var parts = [];
    for (var i = 0; i < orig.length; i++) {
      var c = orig[i];
      if (keep.indexOf(c) >= 0) continue;
      // render("display") returns the cell's rendered HTML, so the HeatCell /
      // ShotCell / FFCell gradient markup survives. Reading it as text would
      // lose the colour that carries the meaning. Escaping is whatever DT
      // already applied for that column -- identical to the main grid.
      parts.push(
        '<div class="ibpl-m-detail-row"><span class="ibpl-m-detail-label">' +
        escapeHtml(names[c]) +
        '</span><span class="ibpl-m-detail-value">' +
        api.cell(rowIdx, c).render("display") +
        "</span></div>"
      );
    }
    return '<div class="ibpl-m-detail">' + parts.join("") + "</div>";
  }

  function addCarets(api, keep, orig) {
    if (keep.length >= orig.length) return;
    var first = keep[0];
    api.rows({ page: "current" }).every(function () {
      // api.cell, not this.cell: inside rows().every() `this` is a row-scoped
      // API and addressing a cell through it is not a documented form.
      var cell = api.cell(this.index(), first);
      if (!cell) return;
      var node = cell.node();
      if (!node || node.querySelector(".ibpl-m-caret")) return;
      var btn = document.createElement("button");
      btn.type = "button";
      btn.className = "ibpl-m-caret";
      btn.setAttribute("aria-expanded", this.child.isShown() ? "true" : "false");
      btn.setAttribute("aria-label", "Show remaining columns");
      btn.textContent = this.child.isShown() ? "−" : "+";
      node.insertBefore(btn, node.firstChild);
    });
  }

  function removeCarets(node) {
    // Guard only -- the path that reaches here with an undefined node is
    // unexplained (three independent static readings failed to find it from
    // the :199 applyTable guard to this call site), but a crash here aborts
    // the sweep for every OTHER table on the page, so the guard stays even
    // without a causal story. Do not delete this as dead code.
    if (!node) return;
    var carets = node.querySelectorAll(".ibpl-m-caret");
    for (var i = 0; i < carets.length; i++) {
      carets[i].parentNode.removeChild(carets[i]);
    }
  }

  function applyTable(api) {
    // applyAll() sweeps every table.dataTable node on the page, including
    // ones from tabs that were re-rendered (Shiny replaces the DT instance
    // on a filter change) and left a stale node without a live settings
    // object behind. Api() on such a node returns an object whose .table()
    // has no node -- guard before touching it, or removeCarets(undefined)
    // throws and aborts the whole sweep for every other table.
    var node = api.table().node();
    if (!node) return;
    var orig = origVisible(node, api);
    var keep = isMobile() ? keepSet(api, outputIdOf(node), orig) : orig;
    var changed = false;

    api.columns().every(function () {
      var want = keep.indexOf(this.index()) >= 0;
      if (this.visible() !== want) {
        this.visible(want, false);
        changed = true;
      }
    });

    if (!isMobile()) {
      api.rows().every(function () { if (this.child.isShown()) this.child.hide(); });
    }

    if (changed) {
      // columns.adjust() alone leaves the header measured against the old
      // layout; a redraw re-measures it and keeps the current page. The guarded
      // draw.dt handler skips this draw, so finish the caret pass below here.
      api.columns.adjust();
      api.draw(false);
    }
    if (isMobile()) {
      addCarets(api, keep, orig);
    } else {
      removeCarets(node);
    }
  }

  function applyAll() {
    if (!$() || !$().fn.dataTable) return;
    if (applying) return;
    applying = true;
    try {
      // Iterate DOM nodes and build one Api per table, the same construction
      // the draw.dt handler below uses. tables().every() is not a documented
      // idiom and would fail silently, leaving every table untouched.
      var nodes = document.querySelectorAll("table.dataTable");
      for (var i = 0; i < nodes.length; i++) {
        applyTable($().fn.dataTable.Api(nodes[i]));
      }
    } finally {
      applying = false;
    }
  }

  function bind() {
    if (!$()) return;

    $()(document).on("draw.dt", function (e) {
      if (applying) return;
      applying = true;
      try {
        applyTable($().fn.dataTable.Api(e.target));
      } finally {
        applying = false;
      }
    });

    // Capture phase, not $(document).on() (bubble phase): Compare binds
    // table.on('click', 'tbody tr', ...) directly on the <table> node, which
    // is a closer ancestor of the caret than document. In the native bubble
    // order the table's listener fires BEFORE a document-bound bubble
    // listener ever gets a chance to run, so calling stopPropagation() there
    // is too late -- Compare's row-click handler already fired (verified
    // live: a bare caret click alone set cmp_table_row_click). Listening on
    // document during the CAPTURE phase runs before the event ever reaches
    // the table, so stopPropagation() here removes it from the rest of the
    // dispatch, bubble phase included.
    document.addEventListener("click", function (e) {
      var $caret = $()(e.target).closest(".ibpl-m-caret");
      if (!$caret.length) return;
      // The pivot menu's only outside-click dismissal (app.js) is bubble
      // phase, so our capture-phase stopPropagation() below removes the
      // click before that listener ever runs -- a caret tap elsewhere would
      // otherwise leave an open menu stuck on screen. Escape is the pivot
      // feature's own close path (app.js keydown handler); dispatching it
      // routes through that closure so its internal state is reset properly
      // instead of leaving it pointing at a node we removed ourselves.
      // Guarded on a menu actually being open, so this is a no-op otherwise.
      var btn = $caret.get(0);
      if (document.querySelector(".ibpl-pivot-menu")) {
        document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
        // close(true) in app.js restores focus to the row that opened the
        // menu, not to the caret the user just tapped (verified live: focus
        // landed on the original opener's <td>, not this button). That
        // opener is necessarily still on screen -- app.js closes the menu on
        // any scroll, so it can never be left open with its opener off
        // screen -- so this never causes a scroll jump, only a focus target
        // that doesn't match what the user just interacted with. Put focus
        // back on the caret, whose aria-expanded state is what actually
        // changed.
        btn.focus();
      }
      e.preventDefault();
      e.stopPropagation();
      var tableNode = $caret.closest("table.dataTable").get(0);
      if (!tableNode) return;
      var api = $().fn.dataTable.Api(tableNode);
      var row = api.row($caret.closest("tr").get(0));
      var orig = origVisible(tableNode, api);
      var keep = keepSet(api, outputIdOf(tableNode), orig);

      if (row.child.isShown()) {
        row.child.hide();
        btn.textContent = "+";
        btn.setAttribute("aria-expanded", "false");
      } else {
        row.child(detailHtml(api, row.index(), keep, orig)).show();
        btn.textContent = "−";
        btn.setAttribute("aria-expanded", "true");
      }
    }, true);

    document.addEventListener("ibpl:mobilechange", applyAll);
    // Tables render lazily inside conditionalPanels, so a freshly shown table
    // has to be brought in line with the current mode.
    $()(document).on("shown.bs.tab shiny:value", applyAll);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bind);
  } else {
    bind();
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
