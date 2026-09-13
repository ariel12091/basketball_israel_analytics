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
  // back to the default. Task 7 adds cmp_table.
  priority: {}
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
