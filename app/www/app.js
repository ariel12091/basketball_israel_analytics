// ---- Startup readiness timing --------------------------------------------
// The nav cards are bound and look clickable at DOMContentLoaded, but
// sendShinyEvent() below silently drops every click until the websocket is up.
// app.R's log_startup() cannot see that window: its clock starts at
// startup_t0, inside the server function, which only runs once the connection
// already exists. So measure it here, where it actually happens, and report it
// once per session.
(function() {
  if (!window.performance || typeof window.performance.now !== "function") return;

  var domReadyAt = null;

  function markDomReady() {
    if (domReadyAt === null) domReadyAt = window.performance.now();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", markDomReady);
  } else {
    markDomReady();
  }

  function onConnected() {
    var connectedAt = window.performance.now();
    var timing = {
      dom_ready_ms: domReadyAt === null ? null : Math.round(domReadyAt),
      connected_ms: Math.round(connectedAt),
      dead_window_ms: domReadyAt === null ? null : Math.round(connectedAt - domReadyAt)
    };

    if (window.console && typeof window.console.info === "function") {
      window.console.info(
        "[startup] nav->DOMContentLoaded " + timing.dom_ready_ms + "ms | " +
        "DOMContentLoaded->shiny:connected " + timing.dead_window_ms + "ms (clicks dead) | " +
        "nav->connected " + timing.connected_ms + "ms"
      );
    }

    if (window.Shiny && typeof window.Shiny.setInputValue === "function") {
      window.Shiny.setInputValue("client_startup_timing", timing, { priority: "event" });
    }
  }

  // jQuery is the proven path for shiny:connected in this file (see the
  // view-mode tooltip binding below); the DOM listener is a fallback only.
  if (window.jQuery) {
    window.jQuery(document).one("shiny:connected", onConnected);
  } else {
    document.addEventListener("shiny:connected", onConnected, { once: true });
  }
})();

(function() {
  if (!window.console || typeof window.console.warn !== "function") return;

  var origWarn = window.console.warn.bind(window.console);
  var blocked = [
    "DEPRECATED: This filename",
    "The language code \"kh\" is deprecated",
    "The language code \"kr\" is deprecated",
    "This language code \"rs-latin\" is deprecated",
    "This language code \"rs\" is deprecated"
  ];

  window.console.warn = function() {
    var msg = arguments.length ? String(arguments[0]) : "";
    for (var i = 0; i < blocked.length; i++) {
      if (msg.indexOf(blocked[i]) !== -1) return;
    }
    return origWarn.apply(window.console, arguments);
  };
})();

(function() {
  var viewTips = {
    Summary: "PPP ratings and shooting splits",
    "Four Factors": "eFG%, OREB%, TOV%, FTR breakdown",
    Traditional: "Box-score counting stats"
  };

  window.applyViewModeTooltips = function() {
    if (!window.jQuery) return;
    window.jQuery(".view-mode-container .radio label, .view-mode-container .shiny-options-group label").each(function() {
      var txt = window.jQuery(this).text().trim();
      if (viewTips[txt]) window.jQuery(this).attr("data-tooltip", viewTips[txt]);
    });
  };

  function bindViewModeTooltips() {
    window.applyViewModeTooltips();
    if (window.jQuery) {
      window.jQuery(document).on("shiny:connected shiny:value", window.applyViewModeTooltips);
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bindViewModeTooltips);
  } else {
    bindViewModeTooltips();
  }
})();

(function() {
  window.handleLineupLinkClick = function(linkEl) {
    if (!linkEl || !window.Shiny || typeof window.Shiny.setInputValue !== "function") return;
    var teamId = parseInt(linkEl.dataset.teamId, 10);
    window.Shiny.setInputValue("ld_lineup_click", {
      hash: linkEl.dataset.hash,
      team_id: Number.isNaN(teamId) ? null : teamId,
      ts: Date.now()
    }, { priority: "event" });
  };

  window.handleRibbonLinkClick = function(linkEl) {
    if (!linkEl) return;
    var gameId = parseInt(linkEl.dataset.gameId, 10);
    var teamId = parseInt(linkEl.dataset.teamId, 10);
    if (Number.isNaN(gameId) || Number.isNaN(teamId)) return;
    window.ibplSendShinyEvent(linkEl.dataset.inputId || "gl_ribbon_click", {
      game_id: gameId,
      team_id: teamId,
      own_team: linkEl.dataset.ownTeam || "",
      opp_team: linkEl.dataset.oppTeam || "",
      mobile: document.body.classList.contains("ibpl-mobile"),
      ts: Date.now()
    });
  };

  window.handleCompareTableRowClick = function(table, rowEl, entityColIdx) {
    if (!table || !rowEl || !window.Shiny || typeof window.Shiny.setInputValue !== "function") return;
    var data = table.row(rowEl).data();
    if (!data) return;
    window.Shiny.setInputValue("cmp_table_row_click", {
      entity_name: data[entityColIdx],
      rand: Math.random()
    }, { priority: "event" });
  };

  function registerCompareViewHandler() {
    if (!window.Shiny || typeof window.Shiny.addCustomMessageHandler !== "function") return false;
    if (window.__cmpViewHandlerRegistered) return true;
    window.__cmpViewHandlerRegistered = true;

    window.Shiny.addCustomMessageHandler("toggle_cmp_view", function(msg) {
      var view = msg && msg.view ? msg.view : ((msg && msg.detail) ? "detail" : "league");
      var showDetail = view === "detail";
      var showPlayers = view === "players";
      var league = document.getElementById("cmp_view_league_btn");
      var detail = document.getElementById("cmp_view_detail_btn");
      var players = document.getElementById("cmp_view_players_btn");
      var leagueC = document.getElementById("cmp_league_container");
      var detailC = document.getElementById("cmp_detail_container");
      var playersC = document.getElementById("cmp_team_players_container");

      if (league) {
        league.classList.toggle("btn-warning", view === "league");
        league.classList.toggle("btn-outline-secondary", view !== "league");
      }
      if (detail) {
        detail.classList.toggle("btn-warning", showDetail);
        detail.classList.toggle("btn-outline-secondary", !showDetail);
      }
      if (players) {
        players.classList.toggle("btn-warning", showPlayers);
        players.classList.toggle("btn-outline-secondary", !showPlayers);
      }
      if (leagueC) leagueC.classList.toggle("cmp-view-hidden", view !== "league");
      if (detailC) detailC.classList.toggle("cmp-view-hidden", !showDetail);
      if (playersC) playersC.classList.toggle("cmp-view-hidden", !showPlayers);
    });

    return true;
  }

  function initCompareViewHandler() {
    if (registerCompareViewHandler()) return;
    var attempts = 0;
    var timer = window.setInterval(function() {
      attempts += 1;
      if (registerCompareViewHandler() || attempts >= 40) window.clearInterval(timer);
    }, 250);
  }

  function detailSectionTitleFor(cell) {
    var node = cell ? cell.previousElementSibling : null;
    while (node) {
      if (node.classList && node.classList.contains("cmp-section-title")) return node;
      node = node.previousElementSibling;
    }
    return null;
  }

  function sortCompareDetailGrid(trigger) {
    var grid = trigger.closest(".detail-container");
    grid = grid ? grid.querySelector(".cmp-compare-grid") : document.querySelector(".cmp-compare-grid");
    if (!grid) return;

    var sortState = (parseInt(grid.dataset.sortState || "0", 10) + 1) % 3;
    grid.dataset.sortState = String(sortState);

    var icon = trigger.querySelector("#cmp-sort-icon") || document.getElementById("cmp-sort-icon");
    var icons = ["\u2195", "\u2193", "\u2191"];
    if (icon) icon.textContent = icons[sortState];

    ["ratings", "off_ff", "def_ff"].forEach(function(group) {
      var gapCells = Array.from(grid.querySelectorAll(".cmp-gap-row[data-group=\"" + group + "\"]"));
      if (!gapCells.length) return;

      var triplets = gapCells.map(function(gapCell) {
        var idx = gapCell.dataset.idx;
        return {
          a: grid.querySelector(".cmp-stat-row.cmp-col-a[data-group=\"" + group + "\"][data-idx=\"" + idx + "\"]"),
          gap: gapCell,
          b: grid.querySelector(".cmp-stat-row.cmp-col-b[data-group=\"" + group + "\"][data-idx=\"" + idx + "\"]"),
          gapVal: parseFloat(gapCell.dataset.gap || "0"),
          original: parseInt(gapCell.dataset.defaultIdx || "0", 10)
        };
      }).filter(function(row) {
        return row.a && row.gap && row.b;
      });
      if (!triplets.length) return;

      if (sortState === 1) {
        triplets.sort(function(a, b) { return Math.abs(b.gapVal) - Math.abs(a.gapVal); });
      } else if (sortState === 2) {
        triplets.sort(function(a, b) { return Math.abs(a.gapVal) - Math.abs(b.gapVal); });
      } else {
        triplets.sort(function(a, b) { return a.original - b.original; });
      }

      var anchor = detailSectionTitleFor(triplets[0].a);
      if (!anchor) return;
      triplets.forEach(function(row) {
        grid.insertBefore(row.a, anchor.nextSibling);
        grid.insertBefore(row.gap, row.a.nextSibling);
        grid.insertBefore(row.b, row.gap.nextSibling);
        anchor = row.b;
      });
    });
  }

  initCompareViewHandler();

  // Clicks landing before the websocket is up used to be swallowed: the
  // listener below calls preventDefault(), then sendShinyEvent() returned
  // early because there was no session to carry the value. That window is
  // 0.6-1.3s after DOMContentLoaded (see the startup timing at the top of this
  // file), which is exactly when an impatient user hits a Home nav card and
  // nothing happens. Hold the intent instead, and replay it on connect.
  //
  // Ordering assumption: this file is loaded via includeScript() in the
  // navbarPage header, so the listener below is always bound before Shiny
  // finishes connecting.
  var shinyReady = false;
  var pendingEvents = [];
  var PENDING_MAX_AGE_MS = 30000;

  function flushPendingEvents() {
    shinyReady = true;
    var queued = pendingEvents;
    pendingEvents = [];
    if (!queued.length) return;

    var now = Date.now();
    for (var i = 0; i < queued.length; i++) {
      // Don't navigate on an intent the user has long since abandoned.
      if (now - queued[i].ts > PENDING_MAX_AGE_MS) continue;
      window.Shiny.setInputValue(queued[i].inputId, queued[i].value, { priority: "event" });
      if (window.console && typeof window.console.info === "function") {
        window.console.info("[startup] replayed queued click: " + queued[i].inputId +
                            " (held " + Math.round(now - queued[i].ts) + "ms)");
      }
    }
  }

  // Deliberately .on(), not .one(): a one-shot handler is consumed by the
  // first connect, so anything queued after that could never drain. The queue
  // is empty on a normal connect, so re-running this is free.
  if (window.jQuery) {
    window.jQuery(document).on("shiny:connected", flushPendingEvents);
  } else {
    document.addEventListener("shiny:connected", flushPendingEvents);
  }

  function sendShinyEvent(inputId, value) {
    if (!inputId) return;
    var payload = value === undefined ? Math.random() : value;

    if (shinyReady && window.Shiny && typeof window.Shiny.setInputValue === "function") {
      window.Shiny.setInputValue(inputId, payload, { priority: "event" });
      return;
    }

    // Last intent per control wins: three impatient clicks on one card should
    // resolve to one navigation, not three.
    pendingEvents = pendingEvents.filter(function(p) { return p.inputId !== inputId; });
    pendingEvents.push({ inputId: inputId, value: payload, ts: Date.now() });
    if (window.console && typeof window.console.info === "function") {
      // Depth is logged so repeated clicks on one card read as deduped rather
      // than dropped: three clicks log three times but leave depth at 1.
      window.console.info("[startup] queued click before connect: " + inputId +
                          " (queue depth " + pendingEvents.length + ")");
    }
  }

  // Reuse the existing queue-and-replay path from delegated handlers defined
  // in later IIFEs. Direct Shiny calls can drop clicks before connection.
  window.ibplSendShinyEvent = sendShinyEvent;

  document.addEventListener("click", function(e) {
    // A chip click reveals the control that owns the value; the x still falls
    // through to the clear event below.
    var chipFocus = e.target.closest("[data-chip-focus]");
    if (chipFocus && !e.target.closest(".chip-x")) {
      e.preventDefault();
      var targetId = chipFocus.dataset.chipFocus;

      // A control inside a collapsed panel cannot take focus, so open it
      // first and let the layout settle before reaching for the input.
      if (document.body.classList.contains("filters-collapsed")) {
        var toggle = document.querySelector(".js-filters-toggle");
        if (toggle) toggle.click();
      }

      window.setTimeout(function() {
        var el = document.getElementById(targetId);
        if (!el) return;

        // Bootstrap accordions hold most of these controls closed.
        var panel = el.closest(".accordion-collapse");
        if (panel && !panel.classList.contains("show") &&
            window.bootstrap && window.bootstrap.Collapse) {
          window.bootstrap.Collapse.getOrCreateInstance(panel).show();
        }

        var group = el.closest(".form-group, .shiny-input-container") || el;
        group.scrollIntoView({ block: "center", behavior: "smooth" });
        group.classList.add("ibpl-chip-revealed");
        window.setTimeout(function() {
          group.classList.remove("ibpl-chip-revealed");
        }, 2400);

        // Selectize replaces the original input with its own focusable node.
        var selectize = group.querySelector(".selectize-input");
        if (selectize) { selectize.click(); return; }
        if (typeof el.focus === "function") el.focus({ preventScroll: true });
      }, 80);
      return;
    }

    var eventEl = e.target.closest("[data-shiny-event], .js-shiny-event");
    if (eventEl) {
      e.preventDefault();
      var value = Object.prototype.hasOwnProperty.call(eventEl.dataset, "shinyValue")
        ? eventEl.dataset.shinyValue
        : undefined;
      sendShinyEvent(eventEl.dataset.shinyEvent || eventEl.dataset.inputId, value);
      return;
    }

    var clickTargetEl = e.target.closest("[data-click-target]");
    if (clickTargetEl) {
      e.preventDefault();
      var target = document.getElementById(clickTargetEl.dataset.clickTarget);
      if (target) target.click();
      return;
    }

    var toggleAllEl = e.target.closest(".js-accordion-toggle-all");
    if (toggleAllEl) {
      e.preventDefault();
      var acc = toggleAllEl.parentElement ? toggleAllEl.parentElement.nextElementSibling : null;
      if (!acc) return;
      var items = acc.querySelectorAll(".accordion-collapse");
      var anyOpen = Array.prototype.some.call(items, function(el) {
        return el.classList.contains("show");
      });
      items.forEach(function(el) {
        if (anyOpen) {
          el.classList.remove("show");
        } else {
          el.classList.add("show");
        }
      });
      return;
    }

    var explainerEl = e.target.closest(".js-explainer-toggle");
    if (explainerEl) {
      e.preventDefault();
      if (!window.bootstrap || !window.bootstrap.Collapse) return;
      var body = document.getElementById(explainerEl.dataset.targetId);
      if (body) window.bootstrap.Collapse.getOrCreateInstance(body).toggle();

      var card = explainerEl.closest(".explainer-card");
      var sib = card ? card.nextElementSibling : null;
      while (sib) {
        if (sib.classList && sib.classList.contains("collapse")) {
          window.bootstrap.Collapse.getOrCreateInstance(sib).toggle();
          break;
        }
        if (sib.querySelector && sib.querySelector(".collapse")) {
          window.bootstrap.Collapse.getOrCreateInstance(sib.querySelector(".collapse")).toggle();
          break;
        }
        sib = sib.nextElementSibling;
      }
      return;
    }

    var detailSortEl = e.target.closest(".js-cmp-detail-sort");
    if (detailSortEl) {
      e.preventDefault();
      sortCompareDetailGrid(detailSortEl);
    }
  });
})();

// ---------------- Stint ribbon hover ----------------
(function() {
  // laneFrom() deliberately also matches the gutter <text> labels, which
  // carry data-clip so a name works as an index into the lanes. Those
  // elements have NO data-start/data-end/data-player, so every function
  // below that reads a stint's attributes must reject them -- otherwise
  // hovering a name renders "undefined · undefined · undefined" in the
  // strip and bands a NaN-wide window.
  function isStint(lane) {
    return !!(lane && lane.dataset && lane.dataset.start !== undefined);
  }

  function bandFor(svg) {
    var band = svg.querySelector(".ibpl-ribbon-hover-band");
    if (!band) {
      band = document.createElementNS("http://www.w3.org/2000/svg", "rect");
      band.setAttribute("class", "ibpl-ribbon-hover-band");
      // First child so it paints behind every lane, curve and gridline.
      svg.insertBefore(band, svg.firstChild);
    }
    return band;
  }

  // The lane <g> elements carry their own x/width, so the band's geometry
  // comes from the hovered lane's rect rather than from a seconds->px
  // conversion the script would have to keep in step with the R constants.
  function setBand(svg, lane) {
    var band = bandFor(svg);
    if (!isStint(lane)) { band.setAttribute("width", "0"); return; }
    var r = lane.querySelector("rect");
    if (!r) { band.setAttribute("width", "0"); return; }
    var vb = svg.viewBox.baseVal;
    band.setAttribute("x", r.getAttribute("x"));
    band.setAttribute("width", r.getAttribute("width"));
    band.setAttribute("y", vb.y);
    band.setAttribute("height", vb.height);
  }

  function clearSelection(svg) {
    if (!svg) return;
    clearLineupFocus(svg);
    var selected = svg.querySelector(".ibpl-ribbon-lane.is-selected");
    if (selected) selected.classList.remove("is-selected");
    var overlays = svg.querySelectorAll(".ibpl-ribbon-selection-overlay");
    for (var i = 0; i < overlays.length; i++) overlays[i].remove();
    resetDetailLayout(svg);
  }

  function clockLabel(seconds) {
    var total = Math.max(0, Math.floor(Number(seconds) || 0));
    return Math.floor(total / 60) + ":" + String(total % 60).padStart(2, "0");
  }


  function pmLabel(value, available) {
    if (!available || !isFinite(value)) return "";
    var rounded = Math.round(value);
    return rounded > 0 ? "+" + rounded : String(rounded);
  }

  function aggregateSegments(segments) {
    var groups = Object.create(null);
    segments.forEach(function(seg) {
      var key = String(seg.dictIndex);
      if (!groups[key]) {
        groups[key] = {
          dictIndex: seg.dictIndex,
          duration: 0,
          pm: 0,
          hasPm: false,
          firstStart: seg.start,
          windows: []
        };
      }
      var group = groups[key];
      group.duration += seg.end - seg.start;
      group.firstStart = Math.min(group.firstStart, seg.start);
      group.windows.push({ start: seg.start, end: seg.end });
      if (seg.pm !== "" && isFinite(Number(seg.pm))) {
        group.pm += Number(seg.pm);
        group.hasPm = true;
      }
    });
    return Object.keys(groups).map(function(key) {
      groups[key].windows.sort(function(a, b) { return a.start - b.start; });
      return groups[key];
    }).sort(function(a, b) {
      return b.duration - a.duration || a.firstStart - b.firstStart;
    });
  }

  // Period markers span the whole chart and so must grow with it. A <line>
  // carries its extent in y2, an alternating band rect in height; both are
  // keyed off the data-base-y2 the server wrote.
  function setPeriodExtent(svg, extraHeight) {
    var marks = svg.querySelectorAll("[data-base-y2]");
    for (var i = 0; i < marks.length; i++) {
      var mark = marks[i];
      var y2 = Number(mark.dataset.baseY2) + extraHeight;
      if (mark.tagName === "rect") {
        mark.setAttribute("height",
          Math.max(y2 - Number(mark.getAttribute("y")), 0));
      } else {
        mark.setAttribute("y2", y2);
      }
    }
  }

  function resetDetailLayout(svg) {
    if (!svg) return;
    var slots = svg.querySelectorAll(".ibpl-ribbon-detail-slot");
    for (var i = 0; i < slots.length; i++) slots[i].remove();
    var host = detailHost(svg);
    var cards = host ? host.querySelectorAll(".ibpl-ribbon-detail.is-card") : [];
    for (var c = 0; c < cards.length; c++) cards[c].remove();

    var shifted = svg.querySelectorAll(
      ".ibpl-ribbon-shift-after-own, .ibpl-ribbon-shift-after-opp"
    );
    for (var j = 0; j < shifted.length; j++) shifted[j].removeAttribute("transform");

    setPeriodExtent(svg, 0);

    var vb = svg.viewBox.baseVal;
    var baseHeight = Number(svg.dataset.baseHeight);
    if (isFinite(baseHeight) && baseHeight > 0) {
      svg.setAttribute("viewBox", [vb.x, vb.y, vb.width, baseHeight].join(" "));
    }
  }

  function applyDetailLayout(svg, side, extraHeight) {
    var selector = side === "own"
      ? ".ibpl-ribbon-shift-after-own"
      : ".ibpl-ribbon-shift-after-opp";
    var shifted = svg.querySelectorAll(selector);
    for (var i = 0; i < shifted.length; i++) {
      shifted[i].setAttribute("transform", "translate(0 " + extraHeight + ")");
    }

    setPeriodExtent(svg, extraHeight);

    var vb = svg.viewBox.baseVal;
    var baseHeight = Number(svg.dataset.baseHeight);
    svg.setAttribute("viewBox", [vb.x, vb.y, vb.width, baseHeight + extraHeight].join(" "));
  }

  // Where a compact chart's detail card and lineup rows live: the inline
  // result wrapper on a phone, else the SVG's parent.
  function detailHost(svg) {
    return (svg.closest && svg.closest(".ibpl-ribbon-inline-result")) || svg.parentNode;
  }

  // The compact (phone) chart gets its detail as an HTML card in the page
  // flow after the chart's horizontal scroller, never a <foreignObject>
  // inside the SVG: the chart is wider than the screen, so an in-chart panel
  // would be cut off at the scroller's edge and slide away when swiped.
  function appendDetailCard(svg, lane, lineups, groups) {
    var card = document.createElement("div");
    card.className = "ibpl-ribbon-detail is-card";
    card.setAttribute("role", "group");
    card.setAttribute("aria-label", "Lineups in " + lane.dataset.player + "'s selected stint");
    card.ibplSvg = svg;

    var head = document.createElement("div");
    head.className = "ibpl-ribbon-detail-head";
    head.textContent = lane.dataset.player;
    card.appendChild(head);

    var facts = [
      lane.dataset.window,
      clockLabel(Number(lane.dataset.end) - Number(lane.dataset.start)) + " on floor"
    ];
    if (lane.dataset.pm) facts.push("+/- " + lane.dataset.pm);
    if (lane.dataset.pf !== "" && lane.dataset.pa !== "") {
      facts.push(lane.dataset.pf + " for, " + lane.dataset.pa + " against");
    }
    var summary = document.createElement("div");
    summary.className = "ibpl-ribbon-detail-summary";
    summary.textContent = facts.join(" · ");
    card.appendChild(summary);

    var caption = document.createElement("div");
    caption.className = "ibpl-ribbon-detail-caption";
    caption.textContent = groups.length === 1
      ? "One lineup played this stint"
      : groups.length + " lineups played this stint. Tap one to mark its minutes.";
    card.appendChild(caption);

    var list = document.createElement("div");
    list.className = "ibpl-ribbon-detail-list";
    groups.forEach(function(group) {
      var row = document.createElement("div");
      row.className = "ibpl-ribbon-detail-row";
      row.setAttribute("tabindex", "0");
      row.setAttribute("role", "button");
      row.dataset.lineupIndex = group.dictIndex;
      row.dataset.windows = group.windows.map(function(window) {
        return window.start + "," + window.end;
      }).join(";");

      var members = document.createElement("div");
      members.className = "ibpl-ribbon-detail-members";
      members.textContent = (lineups[group.dictIndex] || "Lineup unavailable")
        .split(" | ").join(" · ");
      row.appendChild(members);

      var meta = document.createElement("div");
      meta.className = "ibpl-ribbon-detail-meta";
      var pieces = [clockLabel(group.duration)];
      var totalPm = pmLabel(group.pm, group.hasPm);
      if (totalPm) pieces.push("+/- " + totalPm);
      pieces.push(group.windows.map(function(window) {
        return clockLabel(window.start) + "–" + clockLabel(window.end);
      }).join(", "));
      meta.textContent = pieces.join(" · ");
      row.appendChild(meta);
      list.appendChild(row);
    });
    card.appendChild(list);
    var anchor = (svg.closest && svg.closest(".ibpl-ribbon-inline-scroll")) || svg;
    anchor.parentNode.insertBefore(card, anchor.nextSibling);

    // The compact chart is taller than a phone screen (it scrolls vertically
    // rather than squeezing its rows), so the card usually opens below the
    // fold. Scroll toward its head, but never so far that the tapped stint
    // leaves the top of the screen -- the user can scroll the rest.
    var top = card.getBoundingClientRect().top;
    var viewport = window.innerHeight || document.documentElement.clientHeight;
    if (top > viewport - 96) {
      var laneRect = lane.getBoundingClientRect();
      var delta = Math.min(top - (viewport - 180), laneRect.top - 80);
      if (delta > 0) window.scrollBy({ top: delta, behavior: "smooth" });
    }
  }

  function appendInlineDetail(svg, lane, lineups, groups) {
    if (svg.classList.contains("is-compact") && svg.parentNode) {
      appendDetailCard(svg, lane, lineups, groups);
      return;
    }
    var side = lane.classList.contains("is-own") ? "own" : "opp";
    var y = Number(side === "own" ? svg.dataset.ownDetailY : svg.dataset.oppDetailY);
    var x = Number(svg.dataset.detailX) || 0;
    var vb = svg.viewBox.baseVal;
    if (!isFinite(y)) return;

    var slot = document.createElementNS("http://www.w3.org/2000/svg", "foreignObject");
    slot.setAttribute("class", "ibpl-ribbon-detail-slot");
    slot.setAttribute("x", x);
    slot.setAttribute("y", y + 4);
    slot.setAttribute("width", Math.max(1, vb.width - x - 8));
    slot.setAttribute("height", 1000);

    var panel = document.createElement("div");
    panel.className = "ibpl-ribbon-detail";
    panel.setAttribute("role", "group");
    panel.setAttribute("aria-label", "Lineups in " + lane.dataset.player + "'s selected stint");

    var head = document.createElement("div");
    head.className = "ibpl-ribbon-detail-head";
    head.textContent = lane.dataset.player + " | " + lane.dataset.window;
    panel.appendChild(head);

    var list = document.createElement("div");
    list.className = "ibpl-ribbon-detail-list";
    groups.forEach(function(group) {
      var row = document.createElement("div");
      row.className = "ibpl-ribbon-detail-row";
      row.setAttribute("tabindex", "0");
      row.setAttribute("role", "button");
      row.dataset.lineupIndex = group.dictIndex;
      row.dataset.windows = group.windows.map(function(window) {
        return window.start + "," + window.end;
      }).join(";");

      var members = document.createElement("div");
      members.className = "ibpl-ribbon-detail-members";
      members.textContent = lineups[group.dictIndex] || "Lineup unavailable";
      row.appendChild(members);

      var windows = group.windows.map(function(window) {
        return clockLabel(window.start) + "-" + clockLabel(window.end);
      });
      var meta = document.createElement("div");
      meta.className = "ibpl-ribbon-detail-meta";
      var pieces = [clockLabel(group.duration) + " total"];
      var totalPm = pmLabel(group.pm, group.hasPm);
      if (totalPm) pieces.push("+/- " + totalPm);
      pieces.push((windows.length === 1 ? "window: " : "windows: ") + windows.join(", "));
      meta.textContent = pieces.join(" | ");
      row.appendChild(meta);
      list.appendChild(row);
    });
    panel.appendChild(list);
    slot.appendChild(panel);
    svg.appendChild(slot);

    var panelHeight = Math.max(44, Math.ceil(panel.scrollHeight) + 8);
    slot.setAttribute("height", panelHeight);
    applyDetailLayout(svg, side, panelHeight + 8);
  }

  function segmentNumberFits(text, width) {
    return text && width >= text.length * 0.6 * 9 + 6;
  }

  function segmentsForLane(lane) {
    return (lane.dataset.segments || "").split(";").filter(Boolean).map(function(value) {
      var parts = value.split(",");
      return {
        start: Number(parts[0]),
        end: Number(parts[1]),
        pm: parts[2] || "",
        dictIndex: Number(parts[3])
      };
    }).filter(function(seg) {
      return isFinite(seg.start) && isFinite(seg.end) && seg.end > seg.start &&
        isFinite(seg.dictIndex);
    });
  }

  function segmentGeometry(lane, start, end) {
    var rect = lane && lane.querySelector("rect");
    if (!rect) return null;
    var laneStart = Number(lane.dataset.start);
    var laneEnd = Number(lane.dataset.end);
    if (!isFinite(laneStart) || !isFinite(laneEnd) || laneEnd <= laneStart) return null;
    var x0 = Number(rect.getAttribute("x"));
    var width = Number(rect.getAttribute("width"));
    return {
      x: x0 + ((start - laneStart) / (laneEnd - laneStart)) * width,
      width: ((end - start) / (laneEnd - laneStart)) * width,
      y: Number(rect.getAttribute("y")),
      height: Number(rect.getAttribute("height"))
    };
  }

  // L5 is PROVISIONAL: the user chose "all windows that five played" while
  // unsure, and will decide once they have seen it live. Narrowing to the
  // clicked stint alone must stay a one-line change -- return false here.
  // Do not inline this test anywhere else.
  function marksOtherWindows() { return true; }

  function clearLineupFocus(svg) {
    if (!svg) return;
    var generated = svg.querySelectorAll(
      ".ibpl-ribbon-lineup-mark-overlay, .ibpl-ribbon-lineup-clip, .ibpl-ribbon-margin-lineup-echo"
    );
    for (var i = 0; i < generated.length; i++) generated[i].remove();
    var rowHost = detailHost(svg);
    var activeRows = rowHost &&
      rowHost.querySelectorAll(".ibpl-ribbon-detail-row.is-active");
    for (var j = 0; activeRows && j < activeRows.length; j++) {
      activeRows[j].classList.remove("is-active");
    }
    var focus = svg.querySelector(".ibpl-ribbon-margin-focus");
    if (focus) focus.removeAttribute("clip-path");
    svg.classList.remove("is-lineup-focused");
  }

  function appendClipRect(clip, geometry, y, height) {
    var rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
    rect.setAttribute("x", geometry.x);
    rect.setAttribute("y", y);
    rect.setAttribute("width", geometry.width);
    rect.setAttribute("height", height);
    clip.appendChild(rect);
  }

  function setLineupFocus(svg, row) {
    clearLineupFocus(svg);
    if (!svg || !row) return;
    var selected = svg.querySelector(".ibpl-ribbon-lane.is-selected");
    var focus = svg.querySelector(".ibpl-ribbon-margin-focus");
    var defs = svg.querySelector("defs");
    if (!selected || !focus || !defs) return;

    var wantedIndex = Number(row.dataset.lineupIndex);
    var wantedWindows = (row.dataset.windows || "").split(";").filter(Boolean).map(function(value) {
      var parts = value.split(",");
      return { start: Number(parts[0]), end: Number(parts[1]) };
    });
    if (!isFinite(wantedIndex) || !wantedWindows.length) return;

    var sourceClip = svg.querySelector("#" + selected.dataset.clip);
    var sourceClipRect = sourceClip && sourceClip.querySelector("rect");
    if (!sourceClipRect) return;
    var clipY = Number(sourceClipRect.getAttribute("y"));
    var clipHeight = Number(sourceClipRect.getAttribute("height"));
    var fullClip = document.createElementNS("http://www.w3.org/2000/svg", "clipPath");
    var echoClip = document.createElementNS("http://www.w3.org/2000/svg", "clipPath");
    fullClip.setAttribute("class", "ibpl-ribbon-lineup-clip");
    echoClip.setAttribute("class", "ibpl-ribbon-lineup-clip");
    fullClip.setAttribute("id", selected.dataset.clip + "-lineup-full");
    echoClip.setAttribute("id", selected.dataset.clip + "-lineup-echo");

    var lanes = svg.querySelectorAll(".ibpl-ribbon-lane");
    var hasFull = false;
    var hasEcho = false;
    for (var i = 0; i < lanes.length; i++) {
      var lane = lanes[i];
      if (lane.dataset.clip !== selected.dataset.clip) continue;
      var segments = segmentsForLane(lane);
      var overlay = null;
      for (var j = 0; j < segments.length; j++) {
        var seg = segments[j];
        if (seg.dictIndex !== wantedIndex) continue;
        var primary = lane === selected && wantedWindows.some(function(window) {
          return seg.start === window.start && seg.end === window.end;
        });
        if (!primary && !marksOtherWindows()) continue;
        var geometry = segmentGeometry(lane, seg.start, seg.end);
        if (!geometry) continue;
        if (!overlay) {
          overlay = document.createElementNS("http://www.w3.org/2000/svg", "g");
          overlay.setAttribute("class", "ibpl-ribbon-lineup-mark-overlay");
        }
        var mark = document.createElementNS("http://www.w3.org/2000/svg", "rect");
        mark.setAttribute("class", "ibpl-ribbon-lineup-mark " +
          (primary ? "is-primary" : "is-echo"));
        mark.setAttribute("x", geometry.x);
        mark.setAttribute("y", geometry.y);
        mark.setAttribute("width", geometry.width);
        mark.setAttribute("height", geometry.height);
        mark.setAttribute("rx", "2");
        overlay.appendChild(mark);
        appendClipRect(primary ? fullClip : echoClip, geometry, clipY, clipHeight);
        if (primary) hasFull = true;
        else hasEcho = true;
      }
      if (overlay) {
        var selectionOverlay = lane.querySelector(".ibpl-ribbon-selection-overlay");
        lane.insertBefore(overlay, selectionOverlay || null);
      }
    }

    if (!hasFull) {
      clearLineupFocus(svg);
      return;
    }
    defs.appendChild(fullClip);
    focus.setAttribute("clip-path", "url(#" + fullClip.id + ")");
    if (hasEcho) {
      defs.appendChild(echoClip);
      var echoPath = focus.cloneNode(false);
      echoPath.setAttribute("class", "ibpl-ribbon-margin-lineup-echo");
      echoPath.setAttribute("clip-path", "url(#" + echoClip.id + ")");
      focus.parentNode.insertBefore(echoPath, focus.nextSibling);
    }
    row.classList.add("is-active");
    svg.classList.add("is-lineup-focused");
  }

  function setSelection(svg, lane) {
    var otherSelected = document.querySelectorAll(".ibpl-ribbon-lane.is-selected");
    for (var i = 0; i < otherSelected.length; i++) {
      var otherSvg = otherSelected[i].closest(".ibpl-ribbon");
      if (otherSvg && otherSvg !== svg) clearSelection(otherSvg);
    }
    clearSelection(svg);
    if (!isStint(lane)) return;

    var segments = segmentsForLane(lane);
    if (!segments.length) return;

    var lineups = [];
    try { lineups = JSON.parse(svg.dataset.lineups || "[]"); } catch (e) {}
    lane.classList.add("is-selected");
    var rect = lane.querySelector("rect");
    if (!rect) return;
    var overlay = document.createElementNS("http://www.w3.org/2000/svg", "g");
    overlay.setAttribute("class", "ibpl-ribbon-selection-overlay");
    var laneStart = Number(lane.dataset.start);
    var laneEnd = Number(lane.dataset.end);
    var x0 = Number(rect.getAttribute("x"));
    var width = Number(rect.getAttribute("width"));
    var y = Number(rect.getAttribute("y"));
    var h = Number(rect.getAttribute("height"));

    segments.forEach(function(seg, index) {
      var x = x0 + ((seg.start - laneStart) / (laneEnd - laneStart)) * width;
      var w = ((seg.end - seg.start) / (laneEnd - laneStart)) * width;
      if (index > 0) {
        var divider = document.createElementNS("http://www.w3.org/2000/svg", "line");
        divider.setAttribute("class", "ibpl-ribbon-segment-divider");
        divider.setAttribute("x1", x);
        divider.setAttribute("x2", x);
        divider.setAttribute("y1", y);
        divider.setAttribute("y2", y + h);
        overlay.appendChild(divider);
      }
      if (segmentNumberFits(seg.pm, w)) {
        var number = document.createElementNS("http://www.w3.org/2000/svg", "text");
        number.setAttribute("class", "ibpl-ribbon-segment-num");
        number.setAttribute("x", x + w / 2);
        // From the lane centre, matching the server-drawn bar numbers; at the
        // desktop 14-unit lane this is the old y + h - 4.
        number.setAttribute("y", y + h / 2 + 3);
        number.setAttribute("text-anchor", "middle");
        number.textContent = seg.pm;
        overlay.appendChild(number);
      }
    });
    lane.appendChild(overlay);

    appendInlineDetail(svg, lane, lineups, aggregateSegments(segments));
  }

  // mobile.js pins a copy of the compact gutter (svg.ibplPin); its names
  // carry data-pin-clip and must light up with the real ones.
  function syncPinFocus(svg, clip) {
    var pin = svg.ibplPin;
    if (!pin) return;
    var names = pin.querySelectorAll("[data-pin-clip]");
    for (var i = 0; i < names.length; i++) {
      names[i].classList.toggle("is-active", !!clip && names[i].getAttribute("data-pin-clip") === clip);
    }
  }

  function setFocus(svg, lane) {
    var focus = svg.querySelector(".ibpl-ribbon-margin-focus");
    if (!focus) return;
    syncPinFocus(svg, lane && lane.dataset.clip);

    var active = svg.querySelectorAll("[data-clip].is-active");
    for (var i = 0; i < active.length; i++) active[i].classList.remove("is-active");

    if (lane && lane.dataset.clip) {
      focus.setAttribute("clip-path", "url(#" + lane.dataset.clip + ")");
      // [data-clip] rather than .ibpl-ribbon-lane[data-clip]: the gutter
      // label for this lane carries the same data-clip value (outside the
      // lane <g>) and must light up too, so it works as an index.
      var mates = svg.querySelectorAll('[data-clip="' + lane.dataset.clip + '"]');
      for (var m = 0; m < mates.length; m++) mates[m].classList.add("is-active");
      svg.classList.add("is-focused");
    } else {
      focus.removeAttribute("clip-path");
      svg.classList.remove("is-focused");
    }

    setBand(svg, lane);
  }

  function laneFrom(target) {
    // A gutter label (<text data-clip="...">) is a hover target in its own
    // right, not just the lane <g> it labels -- it has no .ibpl-ribbon-lane
    // class of its own, so match on data-clip too.
    return target && target.closest
      ? target.closest(".ibpl-ribbon-lane, [data-clip]")
      : null;
  }

  document.addEventListener("mouseover", function(e) {
    var lane = laneFrom(e.target);
    if (!lane) return;
    var svg = lane.closest(".ibpl-ribbon");
    if (svg) setFocus(svg, lane);
  });

  document.addEventListener("mouseout", function(e) {
    var lane = laneFrom(e.target);
    if (!lane) return;
    var svg = lane.closest(".ibpl-ribbon");
    if (svg && !laneFrom(e.relatedTarget)) setFocus(svg, null);
  });

  document.addEventListener("focusin", function(e) {
    var lane = laneFrom(e.target);
    if (!lane) return;
    var svg = lane.closest(".ibpl-ribbon");
    if (svg) setFocus(svg, lane);
  });

  // A compact row is 18 units tall and a short stint a few units wide --
  // well below a fingertip. So a tap anywhere in the
  // chart resolves to the nearest row (within half a row pitch), then to
  // the stint in that row nearest the tap horizontally. A tap on a gutter
  // name therefore picks that player's first stint.
  function laneAtPoint(svg, clientX, clientY) {
    var ctm = svg.getScreenCTM && svg.getScreenCTM();
    if (!ctm) return null;
    var pt = svg.createSVGPoint();
    pt.x = clientX;
    pt.y = clientY;
    var p = pt.matrixTransform(ctm.inverse());
    var lanes = svg.querySelectorAll(".ibpl-ribbon-lane");
    var best = null, bestDy = Infinity, bestDx = Infinity;
    for (var i = 0; i < lanes.length; i++) {
      var r = lanes[i].querySelector("rect");
      if (!r) continue;
      var x = Number(r.getAttribute("x")), w = Number(r.getAttribute("width"));
      var y = Number(r.getAttribute("y")), h = Number(r.getAttribute("height"));
      var dy = Math.max(0, y - p.y, p.y - (y + h));
      var dx = Math.max(0, x - p.x, p.x - (x + w));
      if (dy < bestDy || (dy === bestDy && dx < bestDx)) {
        best = lanes[i]; bestDy = dy; bestDx = dx;
      }
    }
    return best && bestDy <= 8 ? best : null;
  }

  function noHover() {
    return !!(window.matchMedia && window.matchMedia("(hover: none)").matches);
  }

  document.addEventListener("click", function(e) {
    // Selection is click-driven on every device.
    var lane = laneFrom(e.target);
    if (!isStint(lane)) {
      // A tap on the pinned gutter (mobile.js) picks that row's stint
      // nearest the pin's right edge -- the earliest time currently in view.
      var pinEl = e.target.closest && e.target.closest(".ibpl-ribbon-pin");
      if (pinEl && pinEl.ibplSvg) {
        lane = laneAtPoint(pinEl.ibplSvg, pinEl.getBoundingClientRect().right + 1, e.clientY) || lane;
      } else {
        var compact = e.target.closest && e.target.closest(".ibpl-ribbon.is-compact");
        if (compact) lane = laneAtPoint(compact, e.clientX, e.clientY) || lane;
      }
    }
    if (lane && isStint(lane)) {
      e.preventDefault();
      var svg = lane.closest(".ibpl-ribbon");
      if (!svg) return;
      var wasSelected = lane.classList.contains("is-selected");
      if (wasSelected) clearSelection(svg);
      else setSelection(svg, lane);
      // Focus AFTER selection, and decided by the selection state rather
      // than is-active. A touch tap is preceded by an emulated mouseover
      // that already marks the lane is-active, so toggling on is-active
      // turned the focus straight back off -- a tap never highlighted
      // anything. And clearSelection() strips the curve's clip-path, so
      // focus set before it left the WHOLE curve highlighted instead of
      // this player's minutes. A hover pointer is still over the lane, so
      // it keeps focus on deselect; a touch has nothing left to point at.
      setFocus(svg, wasSelected && (noHover() || svg.classList.contains("is-compact")) ? null : lane);
      if (wasSelected && document.activeElement === lane && lane.blur) lane.blur();
      return;
    }
    if (e.target.closest && e.target.closest(".ibpl-ribbon-detail")) return;
    var selected = document.querySelectorAll(".ibpl-ribbon-lane.is-selected");
    for (var i = 0; i < selected.length; i++) {
      var selectedSvg = selected[i].closest(".ibpl-ribbon");
      if (selectedSvg) clearSelection(selectedSvg);
    }
  });

  document.addEventListener("keydown", function(e) {
    var lane = laneFrom(e.target);
    if (lane && isStint(lane) && (e.key === "Enter" || e.key === " ")) {
      e.preventDefault();
      var svg = lane.closest(".ibpl-ribbon");
      if (svg) {
        if (lane.classList.contains("is-selected")) clearSelection(svg);
        else setSelection(svg, lane);
      }
      return;
    }
    if (e.key === "Escape") {
      var selected = document.querySelectorAll(".ibpl-ribbon-lane.is-selected");
      for (var i = 0; i < selected.length; i++) {
        var selectedSvg = selected[i].closest(".ibpl-ribbon");
        if (selectedSvg) clearSelection(selectedSvg);
      }
    }
  });

  // A desktop row lives inside the SVG's foreignObject; a compact card is
  // the SVG's next sibling and carries a reference back to it.
  function svgForRow(row) {
    var inSvg = row.closest(".ibpl-ribbon");
    if (inSvg) return inSvg;
    var card = row.closest(".ibpl-ribbon-detail.is-card");
    return card && card.ibplSvg && document.contains(card.ibplSvg) ? card.ibplSvg : null;
  }

  document.addEventListener("mouseover", function(e) {
    var row = e.target.closest && e.target.closest(".ibpl-ribbon-detail-row");
    if (!row) return;
    var svg = svgForRow(row);
    if (svg) setLineupFocus(svg, row);
  });

  // Touch browsers do not all emulate mouseover or focus a tapped div, so
  // a tap on a lineup row marks it explicitly. Idempotent with the above.
  document.addEventListener("click", function(e) {
    var row = e.target.closest && e.target.closest(".ibpl-ribbon-detail-row");
    if (!row || row.classList.contains("is-active")) return;
    var svg = svgForRow(row);
    if (svg) setLineupFocus(svg, row);
  });

  document.addEventListener("mouseout", function(e) {
    var row = e.target.closest && e.target.closest(".ibpl-ribbon-detail-row");
    if (!row || (e.relatedTarget && row.contains(e.relatedTarget))) return;
    var svg = svgForRow(row);
    if (svg) clearLineupFocus(svg);
  });

  document.addEventListener("focusin", function(e) {
    var row = e.target.closest && e.target.closest(".ibpl-ribbon-detail-row");
    if (!row) return;
    var svg = svgForRow(row);
    if (svg) setLineupFocus(svg, row);
  });

  document.addEventListener("focusout", function(e) {
    var row = e.target.closest && e.target.closest(".ibpl-ribbon-detail-row");
    if (!row || (e.relatedTarget && row.contains(e.relatedTarget))) return;
    var svg = svgForRow(row);
    if (svg) clearLineupFocus(svg);
  });
})();

(function() {
  var cfg = window.IBPL_IDLE_CONFIG || {};
  var timeoutMs = Math.max(1, Number(cfg.timeoutSec || 360)) * 1000;
  var warningMs = Math.max(1, Number(cfg.warningSec || 60)) * 1000;
  var ttlMs = Math.max(1, Number(cfg.stateTtlHours || 24)) * 60 * 60 * 1000;
  var stateVersion = Number(cfg.stateVersion || 1);
  // The server tells us whether it will actually close an idle session. When
  // it will not (the default since the move to Posit Connect Cloud), the
  // countdown and the paused pill would be lying, so the timer-driven half of
  // this is switched off. The disconnect-driven half stays: Connect Cloud
  // stops the container on its own and the pill is still the right response.
  var serverClosesSession = cfg.closeSession !== false;
  warningMs = Math.min(warningMs, Math.max(1000, timeoutMs - 1000));
  var loadedFromBookmark = location.search.indexOf("_inputs_") !== -1;
  var bookmarkCaptureArmed = !loadedFromBookmark;

  var keyBase = "ibpl_idle_resume:" + location.pathname.replace(/\/+$/, "");
  var tabIdKey = keyBase + ":tab_id";
  var tabId = getOrCreateTabId();
  var urlKey = keyBase + ":tab:" + tabId + ":bookmark:v" + stateVersion;
  var skipRestoreKey = keyBase + ":tab:" + tabId + ":skip_restore";
  var restoredFlagKey = keyBase + ":tab:" + tabId + ":restored";
  var hubTeamKey = "ibplHubTeam";
  var hubTeamDefaultKey = "ibplHubTeamDefaultEnabled";

  var idleExpired = false;
  var sessionReady = false;
  var navigating = false;
  var lastActivity = Date.now();
  var lastSent = 0;
  var minIntervalMs = 15000;
  var timerId = null;
  var handlersRegistered = false;

  function safeSessionGet(key) {
    try { return window.sessionStorage.getItem(key); } catch (e) { return null; }
  }

  function safeSessionSet(key, value) {
    try { window.sessionStorage.setItem(key, value); } catch (e) {}
  }

  function safeSessionRemove(key) {
    try { window.sessionStorage.removeItem(key); } catch (e) {}
  }

  function safeLocalGet(key) {
    try { return window.localStorage.getItem(key); } catch (e) { return null; }
  }

  function safeLocalSet(key, value) {
    try { window.localStorage.setItem(key, value); } catch (e) {}
  }

  function safeLocalRemove(key) {
    try { window.localStorage.removeItem(key); } catch (e) {}
  }

  // The Home controls are followed by an inline call to this helper. That call
  // runs while the initial HTML is being parsed, before Shiny binds its inputs,
  // so a saved default never flashes as the random fallback first.
  window.ibplApplyInitialHubTeamDefault = function() {
    if (safeLocalGet(hubTeamDefaultKey) !== "1") return;
    var teamId = safeLocalGet(hubTeamKey);
    var teamSelect = document.getElementById("home_team");
    var hasTeam = teamSelect && Array.prototype.some.call(
      teamSelect.options,
      function(option) { return option.value === teamId; }
    );
    if (!teamId || !hasTeam) {
      return;
    }
    teamSelect.value = teamId;
  };

  function getOrCreateTabId() {
    var existing = safeSessionGet(tabIdKey);
    if (existing) return existing;
    var id = String(Date.now()) + "-" + Math.random().toString(36).slice(2, 10);
    safeSessionSet(tabIdKey, id);
    return id;
  }

  function storeBookmarkUrl(url) {
    var payload = JSON.stringify({ url: url, savedAt: Date.now(), v: stateVersion });
    safeSessionSet(urlKey, payload);
    safeLocalSet(urlKey, payload);
  }

  function loadBookmarkUrl() {
    var raw = safeSessionGet(urlKey) || safeLocalGet(urlKey);
    if (!raw) return null;
    try {
      var parsed = JSON.parse(raw);
      if (!parsed || parsed.v !== stateVersion || !parsed.url) return null;
      if ((Date.now() - Number(parsed.savedAt)) > ttlMs) return null;
      return parsed.url;
    } catch (e) {
      return null;
    }
  }

  function cleanLocation() {
    return location.pathname + location.hash;
  }

  // A restored Shiny session emits transient bookmarks while server-populated
  // choices are rebuilding. Keep the saved pre-idle URL until the user
  // deliberately interacts with the restored page.
  function armBookmarkCaptureFromUserEvent(event) {
    if (bookmarkCaptureArmed || !loadedFromBookmark || !event) return;
    if (event.type === "mousemove") return;
    if (event.isTrusted === false) return;
    bookmarkCaptureArmed = true;
  }

  // One-shot restore navigation. The bookmark params are stripped from the
  // address bar as soon as the new session has been created.
  function restoreOnReturn() {
    if (navigating || !idleExpired) return;
    if (safeSessionGet(skipRestoreKey)) return;
    var url = loadBookmarkUrl();
    navigating = true;
    safeSessionSet(restoredFlagKey, String(Date.now()));
    if (!url) {
      window.location.reload();
      return;
    }
    window.location.replace(url);
  }

  function clearBookmarkParams() {
    if (!window.history || typeof window.history.replaceState !== "function") return;
    if (location.search.indexOf("_inputs_") === -1) return;
    window.history.replaceState(window.history.state, "", cleanLocation());
  }

  // Shiny builds the server-side restore context from `.clientdata_url_search`,
  // which its client reads out of location.search when it sends the init
  // message. Stripping the bookmark parameters before that leaves the new
  // session with an inactive restore context, so every server-populated choice
  // (teams, opponents, lineup players, compare players) loses its value while
  // UI-time restoreInput() still works. Defer the cleanup until the session
  // exists.
  function scheduleBookmarkParamCleanup() {
    if (location.search.indexOf("_inputs_") === -1) return;
    var done = false;
    var run = function() {
      if (done) return;
      done = true;
      clearBookmarkParams();
    };
    if (window.jQuery) {
      window.jQuery(document).one("shiny:sessioninitialized", run);
    } else {
      document.addEventListener("shiny:sessioninitialized", run, { once: true });
    }
  }

  function shinyReadyForRestore() {
    return !!(window.Shiny && typeof window.Shiny.setInputValue === "function");
  }

  function hideNativeDisconnectNodes() {
    var selectors = [
      "#shiny-disconnected-overlay",
      ".shiny-disconnected-overlay",
      "#shiny-disconnected-dialog",
      ".shiny-disconnected-dialog",
      "#shiny-notification-reconnect",
      "#shiny-reconnect-dialog",
      ".shiny-reconnect-dialog",
      ".reconnect-dialog",
      // shiny-server-client's nodes, not Shiny's own. Served by Posit Connect
      // Cloud as well as the old shinyapps.io; verified present in the
      // deployed page 2026-09-07. These are what actually appear in production.
      "#ss-overlay",
      ".ss-gray-out",
      "#ss-connect-dialog"
    ];
    for (var i = 0; i < selectors.length; i++) {
      var nodes = document.querySelectorAll(selectors[i]);
      for (var j = 0; j < nodes.length; j++) {
        nodes[j].style.setProperty("display", "none", "important");
        nodes[j].style.setProperty("pointer-events", "none", "important");
      }
    }
  }

  // Hiding the hosting layer's dialog removes the only thing that told the user
  // the app had stopped, so treat its reveal as a disconnect and show the pill.
  // shiny-server-client sets an inline display on a node that already exists at
  // page load; on a local run the node is absent and this is a no-op.
  function watchHostingDisconnectDialog() {
    if (typeof window.MutationObserver !== "function") return;
    var attached = false;
    var attach = function() {
      var dialog = document.getElementById("ss-connect-dialog");
      if (attached || !dialog) return false;
      attached = true;
      new window.MutationObserver(function() {
        if (dialog.style.display && dialog.style.display !== "none") handleDisconnected();
      }).observe(dialog, { attributes: true, attributeFilter: ["style"] });
      return true;
    };
    // Present at page load on Connect Cloud; watch for it otherwise rather than
    // assume the ordering, since getting this wrong means hiding the dialog and
    // showing nothing in its place.
    if (attach() || !document.body) return;
    var bodyObserver = new window.MutationObserver(function() {
      if (attach()) bodyObserver.disconnect();
    });
    bodyObserver.observe(document.body, { childList: true });
  }

  function toggleNativeDisconnectUi(hidden) {
    if (document.body && document.body.classList) {
      document.body.classList.toggle("ibpl-idle-expired", !!hidden);
    }
    if (!hidden) return;
    hideNativeDisconnectNodes();
    window.setTimeout(hideNativeDisconnectNodes, 50);
    window.setTimeout(hideNativeDisconnectNodes, 500);
  }

  function ensureIdleOverlay() {
    var existing = document.getElementById("ibpl-idle-overlay");
    if (existing) return existing;

    var overlay = document.createElement("div");
    overlay.id = "ibpl-idle-overlay";
    overlay.className = "idle-overlay";
    overlay.setAttribute("role", "dialog");
    overlay.setAttribute("aria-modal", "true");
    overlay.setAttribute("aria-labelledby", "ibpl-idle-title");
    overlay.innerHTML =
      '<div class="idle-panel">' +
        '<div class="idle-kicker">Session status</div>' +
        '<h2 id="ibpl-idle-title">Still working?</h2>' +
        '<p class="idle-copy">This session will pause soon to keep the app responsive.</p>' +
        '<div class="idle-countdown" id="ibpl-idle-countdown"></div>' +
        '<div class="idle-actions">' +
          '<button type="button" class="btn btn-primary idle-keep-btn" id="ibpl-idle-keep">Keep working</button>' +
        '</div>' +
      '</div>';
    document.body.appendChild(overlay);

    var keepBtn = document.getElementById("ibpl-idle-keep");
    if (keepBtn) {
      keepBtn.addEventListener("click", function() {
        markActivity(true);
        keepBtn.blur();
      });
    }
    return overlay;
  }

  function setOverlayState(secondsLeft) {
    var overlay = ensureIdleOverlay();
    var countdown = document.getElementById("ibpl-idle-countdown");
    if (countdown) countdown.textContent = "Pausing in " + secondsLeft + " seconds";
    overlay.classList.add("visible");
  }

  function hideIdleWarning() {
    var overlay = document.getElementById("ibpl-idle-overlay");
    if (!overlay) return;
    overlay.classList.remove("visible");
  }

  function showPausedPill() {
    var pill = document.getElementById("ibpl-idle-pill");
    if (!pill) {
      pill = document.createElement("div");
      pill.id = "ibpl-idle-pill";
      pill.className = "restore-notice";
      pill.innerHTML =
        '<span>Session paused.</span>' +
        '<button type="button" id="ibpl-idle-resume">Resume</button>' +
        '<button type="button" id="ibpl-idle-fresh">Start fresh</button>';
      document.body.appendChild(pill);
      var resumeBtn = document.getElementById("ibpl-idle-resume");
      if (resumeBtn) {
        resumeBtn.addEventListener("click", function(e) {
          e.stopPropagation();
          restoreOnReturn();
        });
      }
      var freshBtn = document.getElementById("ibpl-idle-fresh");
      if (freshBtn) {
        freshBtn.addEventListener("click", function(e) {
          e.stopPropagation();
          window.ibplClearSavedSession();
          navigating = true;
          window.location.reload();
        });
      }
    }
    hideIdleWarning();
    toggleNativeDisconnectUi(true);
    pill.classList.add("visible");
  }

  function showRestoredNotice() {
    var notice = document.getElementById("ibpl-restore-notice");
    if (!notice) {
      notice = document.createElement("div");
      notice.id = "ibpl-restore-notice";
      notice.className = "restore-notice";
      notice.innerHTML =
        '<span>Restored your last tab and filters.</span>' +
        '<button type="button" id="ibpl-restore-clear">Start fresh</button>';
      document.body.appendChild(notice);
      var clearBtn = document.getElementById("ibpl-restore-clear");
      if (clearBtn) {
        clearBtn.addEventListener("click", function() {
          window.ibplClearSavedSession();
          window.location.reload();
        });
      }
    }
    notice.classList.add("visible");
    window.setTimeout(function() { notice.classList.remove("visible"); }, 6000);
  }

  function sendActivity(force) {
    // Nothing reads idle_activity_ts when the server is not closing idle
    // sessions, and a 15s heartbeat would also hold Connect Cloud's container
    // open for as long as a tab is left on screen.
    if (!serverClosesSession) return;
    var now = Date.now();
    // Hard guard: never emit an input before shiny's init message has been
    // answered, or we steal the restore context. See handleConnected().
    if (!sessionReady) return;
    if (!shinyReadyForRestore()) return;
    if (!force && (now - lastSent) < minIntervalMs) return;
    lastSent = now;
    try {
      window.Shiny.setInputValue("idle_activity_ts", now, { priority: "event" });
    } catch (e) {}
  }

  function markActivity(force) {
    if (idleExpired) {
      restoreOnReturn();
      return;
    }
    lastActivity = Date.now();
    hideIdleWarning();
    sendActivity(force);
  }

  function checkIdleState() {
    if (!serverClosesSession) return;
    if (idleExpired || document.visibilityState === "hidden") return;
    var remainingMs = timeoutMs - (Date.now() - lastActivity);
    if (remainingMs <= 0) {
      idleExpired = true;
      showPausedPill();
      return;
    }
    if (remainingMs <= warningMs) {
      setOverlayState(Math.max(0, Math.ceil(remainingMs / 1000)));
    } else {
      hideIdleWarning();
    }
  }

  function handleDisconnected() {
    if (document.visibilityState === "hidden") {
      idleExpired = true;
      toggleNativeDisconnectUi(true);
      return;
    }
    idleExpired = true;
    toggleNativeDisconnectUi(true);
    showPausedPill();
  }

  function registerMessageHandlers() {
    if (handlersRegistered ||
        !window.Shiny ||
        typeof window.Shiny.addCustomMessageHandler !== "function") return false;
    handlersRegistered = true;
    window.Shiny.addCustomMessageHandler("ibpl_bookmark_url", function(msg) {
      if (msg && msg.url && bookmarkCaptureArmed) {
        storeBookmarkUrl(msg.url + "&ibpl_v=" + stateVersion);
      }
    });
    window.Shiny.addCustomMessageHandler("ibpl-store-hub-team", function(msg) {
      if (msg && msg.enabled && msg.teamId) {
        safeLocalSet(hubTeamKey, String(msg.teamId));
        safeLocalSet(hubTeamDefaultKey, "1");
      } else {
        safeLocalRemove(hubTeamKey);
        safeLocalRemove(hubTeamDefaultKey);
      }
    });
    return true;
  }

  // Nothing here may send an input. shiny:connected fires inside the socket's
  // onopen handler, *before* shiny sends its own init message, and an
  // event-priority setInputValue() is flushed synchronously. That input would
  // then be the first message the server sees, and Shiny builds the session's
  // restore context from the first message's `.clientdata_url_search` — absent
  // on an update — leaving every bookmark restore dead. Input sends belong in
  // handleSessionInitialized().
  function handleConnected() {
    registerMessageHandlers();
    toggleNativeDisconnectUi(false);
  }

  function handleSessionInitialized() {
    sessionReady = true;
    sendActivity(true);
    if (window.Shiny && typeof window.Shiny.setInputValue === "function") {
      var rememberedTeam = safeLocalGet(hubTeamDefaultKey) === "1"
        ? (safeLocalGet(hubTeamKey) || "")
        : "";
      window.Shiny.setInputValue(
        "hub_remembered_team",
        rememberedTeam
      );
    }
  }

  function shouldRestoreFromPausedEvent(event) {
    if (!event) return true;
    if (event.type === "mousemove") return false;
    if (event.type === "keydown" && event.key === "Tab") return false;
    var target = event.target;
    if (target && typeof target.closest === "function" &&
        target.closest("#ibpl-idle-pill")) return false;
    return true;
  }

  function handleVisibilityChange() {
    if (document.visibilityState !== "visible") return;
    // Background tabs throttle timers and can deliver shiny:disconnected after
    // visibilitychange. Compare wall-clock time before activity can reset it.
    if (serverClosesSession && (Date.now() - lastActivity) >= timeoutMs) idleExpired = true;
    if (!shinyReadyForRestore()) idleExpired = true;
    if (idleExpired) {
      restoreOnReturn();
      return;
    }
    markActivity(true);
  }

  function bindActivity() {
    var events = ["mousemove", "mousedown", "keydown", "scroll", "touchstart", "click"];
    for (var i = 0; i < events.length; i++) {
      document.addEventListener(events[i], function(event) {
        if (idleExpired) {
          if (shouldRestoreFromPausedEvent(event)) restoreOnReturn();
          return;
        }
        armBookmarkCaptureFromUserEvent(event);
        markActivity(false);
      }, { passive: true });
    }
    document.addEventListener("visibilitychange", handleVisibilityChange);
    if (window.jQuery) {
      window.jQuery(document).on("shiny:connected", handleConnected);
      window.jQuery(document).on("shiny:disconnected", handleDisconnected);
    } else {
      document.addEventListener("shiny:connected", handleConnected);
      document.addEventListener("shiny:disconnected", handleDisconnected);
    }
    registerMessageHandlers();
    watchHostingDisconnectDialog();
    sendActivity(true);
    if (timerId) window.clearInterval(timerId);
    if (serverClosesSession) timerId = window.setInterval(checkIdleState, 1000);
  }

  // Registered at parse time, not in bindActivity(), so the listener is in
  // place before shiny can answer its own init message.
  if (window.jQuery) {
    window.jQuery(document).one("shiny:sessioninitialized", handleSessionInitialized);
  } else {
    document.addEventListener("shiny:sessioninitialized", handleSessionInitialized, { once: true });
  }
  // Safety net: if that event is ever missed the heartbeat would never start
  // and R would close a session the user is actively using. By this point init
  // is long past, so releasing the guard cannot steal the restore context.
  window.setTimeout(function() {
    if (!sessionReady && shinyReadyForRestore()) handleSessionInitialized();
  }, 10000);

  scheduleBookmarkParamCleanup();
  if (safeSessionGet(skipRestoreKey)) safeSessionRemove(skipRestoreKey);
  if (safeSessionGet(restoredFlagKey)) {
    safeSessionRemove(restoredFlagKey);
    window.setTimeout(showRestoredNotice, 400);
  }

  window.ibplDebugSavedSession = function() {
    return {
      url: loadBookmarkUrl(),
      idleExpired: idleExpired,
      tabId: tabId,
      bookmarkCaptureArmed: bookmarkCaptureArmed
    };
  };
  window.ibplClearSavedSession = function() {
    safeSessionRemove(urlKey);
    safeLocalRemove(urlKey);
    safeSessionSet(skipRestoreKey, String(Date.now()));
  };
  window.ibplRestoreSavedSession = function() {
    idleExpired = true;
    restoreOnReturn();
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bindActivity);
  } else {
    bindActivity();
  }
})();

(function() {
  var CFG = [
    { tab: "onoff", inputId: "onoff_view_mode", items: ["Summary", "Four Factors", "Shot Profile"], def: "Summary" },
    { tab: "lineup_data", inputId: "ld_view_mode", items: ["Summary", "Four Factors"], def: "Summary" },
    { tab: "team_ratings", inputId: "tr_view_mode", items: ["Summary", "Four Factors", "Shot Profile", "Traditional"], def: "Summary" },
    { tab: "game_logs", inputId: "gl_view_mode", items: ["Summary", "Four Factors"], def: "Summary" },
    { tab: "traditional_stats", inputId: "ts_display_mode", items: ["Totals", "Per Game", "Per 60 Possessions", "Per 30 Minutes"], def: "Per Game", type: "select" },
    { tab: "compare", inputId: "cmp_mode", items: ["Teams", "Lineups", "Players"], def: "Teams" },
    { tab: "euro", inputId: "euro_view_mode", items: ["Summary", "Four Factors"], def: "Summary" },
    { tab: "euro_team", inputId: "euroteam_view_mode", items: ["Summary", "Four Factors"], def: "Summary" },
    { tab: "euro_lineups", inputId: "euro_ld_view_mode", items: ["Summary", "Four Factors"], def: "Summary" },
    { tab: "euro_game_logs", inputId: "eurogl_view_mode", items: ["Summary", "Four Factors"], def: "Summary" }
  ];

  function setRowActive(row, active) {
    row.className = "thm-item" + (active ? " active" : "");
    var check = row.querySelector(".thm-check");
    if (check) check.textContent = active ? "\u2713" : "";
  }

  function updateInput(inputId, value, type) {
    if (type === "select") {
      var sel = document.getElementById(inputId);
      if (sel) {
        sel.value = value;
        sel.dispatchEvent(new Event("change", { bubbles: true }));
      }
    } else {
      var radio = document.querySelector("input[name=\"" + inputId + "\"][value=\"" + value + "\"]");
      if (radio) radio.click();
    }

    if (window.Shiny && typeof window.Shiny.setInputValue === "function") {
      window.Shiny.setInputValue(inputId, value, { priority: "event" });
    }
  }

  function currentInputValue(config) {
    if (config.type === "select") {
      var sel = document.getElementById(config.inputId);
      return sel ? sel.value : config.def;
    }

    var checked = document.querySelector("input[name=\"" + config.inputId + "\"]:checked");
    return checked ? checked.value : config.def;
  }

  function initOne(config) {
    var link = document.querySelector(".nav-link[data-value=\"" + config.tab + "\"]");
    if (!link) return;

    var li = link.closest(".nav-item");
    if (!li || li.querySelector(".tab-hover-menu")) return;

    li.classList.add("tab-has-dropdown");

    var menu = document.createElement("div");
    menu.className = "tab-hover-menu";

    config.items.forEach(function(item) {
      var row = document.createElement("div");
      var check = document.createElement("span");
      var label = document.createTextNode(item);

      row.dataset.value = item;
      row.dataset.inputId = config.inputId;
      check.className = "thm-check";
      row.appendChild(check);
      row.appendChild(label);
      setRowActive(row, item === config.def);

      row.addEventListener("click", function(e) {
        e.stopPropagation();
        e.preventDefault();

        var val = this.dataset.value;
        link.click();

        setTimeout(function() {
          updateInput(config.inputId, val, config.type);
        }, 0);

        menu.querySelectorAll(".thm-item").forEach(function(r) {
          setRowActive(r, r.dataset.value === val);
        });

        menu.style.display = "none";
        setTimeout(function() {
          menu.style.display = "";
        }, 50);
      });

      menu.appendChild(row);
    });

    li.appendChild(menu);

    function syncMenu() {
      var current = currentInputValue(config);
      menu.querySelectorAll(".thm-item").forEach(function(row) {
        setRowActive(row, row.dataset.value === current);
      });
    }

    link.addEventListener("shown.bs.tab", syncMenu);
    link.addEventListener("click", function() {
      setTimeout(syncMenu, 100);
    });
  }

  function init() {
    CFG.forEach(initOne);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();

/* --------------------------------------------------------------------------
   League scoping.

   The navbar holds every league's tabs, but only one league's are visible at
   a time -- otherwise a EuroLeague section at parity with the Israeli one
   would make a 14-item navbar. Nav items are filtered by a tab -> league map,
   and Home swaps its content through a body class.

   ONE control chooses the league: the #league_select dropdown, whose value is
   a competition code ("E", "U") or "il". Applying it is done here rather than
   on the server so switching is instant, but the value itself is a Shiny
   input -- so it bookmarks and restores like any other filter.

   A tab NOT listed here is league-neutral and always visible (e.g. "home").
   -------------------------------------------------------------------------- */
(function() {
  var TAB_LEAGUE = {
    onoff: "il", lineup_data: "il", team_ratings: "il",
    game_logs: "il", compare: "il",
    euro: "el", euro_team: "el", euro_lineups: "el", euro_game_logs: "el"
  };
  var STORE_KEY = "ibpl_league_select";
  var DEFAULT_VALUE = "il";
  // Shiny restores main_tabs and league_select independently. During that
  // startup window the select can briefly report its UI default (Israeli)
  // after main_tabs has already restored a EuroLeague page. Treating that
  // transient mismatch as a real league switch clicks Home and destroys the
  // restored tab. Keep both leagues on the same native bookmark path by
  // disabling only the mismatch redirect until Shiny finishes initialization.
  var bookmarkRestorePending = location.search.indexOf("_inputs_") !== -1;

  // Which league owns each league_select value. Deliberately explicit rather
  // than "anything that is not Israeli must be EuroLeague": that assumption
  // would silently route a new ISRAELI-side competition to the EuroLeague
  // tabs. Keep in step with LEAGUE_SELECT_CHOICES in global.R -- an unmapped
  // value warns rather than guessing, because neither guess is safe.
  var VALUE_LEAGUE = { "il": "il", "E": "el", "U": "el" };

  function leagueOf(value) {
    if (Object.prototype.hasOwnProperty.call(VALUE_LEAGUE, value)) return VALUE_LEAGUE[value];
    if (window.console && window.console.warn) {
      window.console.warn(
        "[ibpl] league_select value '" + value + "' is not in VALUE_LEAGUE; " +
        "falling back to '" + DEFAULT_VALUE + "'. Update app.js to match " +
        "LEAGUE_SELECT_CHOICES in global.R."
      );
    }
    return VALUE_LEAGUE[DEFAULT_VALUE];
  }

  // First value belonging to a league -- where a tab restored into the other
  // league lands. Derived, so adding a competition needs no second edit here.
  function firstValueForLeague(league) {
    for (var k in VALUE_LEAGUE) {
      if (Object.prototype.hasOwnProperty.call(VALUE_LEAGUE, k) && VALUE_LEAGUE[k] === league) return k;
    }
    return DEFAULT_VALUE;
  }

  function read() {
    try { return window.localStorage.getItem(STORE_KEY); } catch (e) { return null; }
  }
  function write(v) {
    try { window.localStorage.setItem(STORE_KEY, v); } catch (e) {}
  }

  function selectEl() { return document.getElementById("league_select"); }

  // Guards against a stale localStorage value naming an option that no longer
  // exists. Selectize owns the option list once it initialises, so ask it
  // first; the raw <select> is only authoritative before that.
  function isValidValue(v) {
    if (!v) return false;
    var el = selectEl();
    if (el && el.selectize) return Object.prototype.hasOwnProperty.call(el.selectize.options, v);
    if (el && el.options && el.options.length) {
      return Array.prototype.some.call(el.options, function(o) { return o.value === v; });
    }
    return Object.prototype.hasOwnProperty.call(VALUE_LEAGUE, v);
  }

  function navLinks() {
    return Array.prototype.slice.call(
      document.querySelectorAll('.navbar a[data-value]')
    );
  }

  function activeTabValue() {
    var el = document.querySelector('.navbar a[data-value].active');
    return el ? el.getAttribute("data-value") : null;
  }

  // Reflects the current value into the page. Does NOT write the select --
  // callers do that first, so the select stays the single source of truth.
  function applyValue(value, opts) {
    opts = opts || {};
    var league = leagueOf(value);
    document.body.classList.toggle("league-il", league === "il");
    document.body.classList.toggle("league-el", league === "el");

    navLinks().forEach(function(a) {
      var owner = TAB_LEAGUE[a.getAttribute("data-value")];
      var li = a.parentNode;
      if (!li || li.tagName !== "LI") return;
      li.style.display = (!owner || owner === league) ? "" : "none";
    });

    Array.prototype.forEach.call(
      document.querySelectorAll("[data-league-btn]"),
      function(b) {
        b.classList.toggle("active", b.getAttribute("data-league-btn") === value);
      }
    );

    // If the tab we are on belongs to the other league it just became
    // invisible -- go Home rather than stranding the user on a hidden tab.
    var current = activeTabValue();
    var owner = current ? TAB_LEAGUE[current] : null;
    if (owner && owner !== league && !opts.noRedirect && !bookmarkRestorePending) {
      var home = document.querySelector('.navbar a[data-value="home"]');
      if (home) home.click();
    }
  }

  // The select is a selectize widget, so the original <select> is hidden and
  // its change event is fired through jQuery. Write via the selectize API when
  // it exists, and fall back to the raw element before it initialises.
  function writeSelect(value) {
    var el = selectEl();
    if (!el) return;
    if (el.selectize) {
      if (el.selectize.getValue() !== value) el.selectize.setValue(value);
      return;
    }
    if (el.value !== value) {
      el.value = value;
      if (window.jQuery) window.jQuery(el).trigger("change");
    }
  }

  function currentValue() {
    var el = selectEl();
    if (!el) return read() || DEFAULT_VALUE;
    return (el.selectize ? el.selectize.getValue() : el.value) || DEFAULT_VALUE;
  }

  // The one way to change league from anywhere: write the select, let its
  // change event tell Shiny, then reflect it. Home's cards go through here.
  function setValue(value) {
    if (!isValidValue(value)) return;
    write(value);
    writeSelect(value);
    applyValue(value);
  }

  // What the league SHOULD be on load: a restored bookmark points at a
  // specific tab, and that tab's league wins over the stored preference or the
  // restore lands on a hidden tab. Only the league is implied by a tab, so a
  // stored competition survives when it agrees with that league.
  function desiredValue() {
    var stored = isValidValue(read()) ? read() : null;
    var value = stored || DEFAULT_VALUE;
    var current = activeTabValue();
    var fromTab = current ? TAB_LEAGUE[current] : null;
    if (fromTab && leagueOf(value) !== fromTab) {
      value = firstValueForLeague(fromTab);
    }
    return value;
  }

  function init() {
    if (!selectEl()) return;

    document.addEventListener("click", function(e) {
      var btn = e.target.closest ? e.target.closest("[data-league-btn]") : null;
      if (!btn) return;
      e.preventDefault();
      setValue(btn.getAttribute("data-league-btn"));
    });

    // Delegated and via jQuery: selectize fires change with jQuery.trigger(),
    // which addEventListener would never see.
    if (window.jQuery) {
      window.jQuery(document).on("change", "#league_select", function() {
        var v = currentValue();
        write(v);
        applyValue(v);
      });
    }

    // Apply the visual state immediately so the navbar never shows the wrong
    // league's tabs, then re-assert once Shiny has bound the input -- the two
    // orderings (selectize initialised before or after this) are both live.
    var value = desiredValue();
    write(value);
    writeSelect(value);
    applyValue(value, { noRedirect: true });

    if (window.jQuery) {
      window.jQuery(document).one("shiny:sessioninitialized", function() {
        var restoredValue = desiredValue();
        writeSelect(restoredValue);
        applyValue(restoredValue, { noRedirect: true });
        bookmarkRestorePending = false;
      });
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();

/* --------------------------------------------------------------------------
   Navbar overlap.

   #navbar_right_cluster is position:fixed, so it is outside normal flow and
   the tab list lays out as if it were not there. Below ~1440px the rightmost
   tabs rendered underneath it, and because the cluster also takes the pointer
   their hover menus could never open (li:hover never matched). Reserve the
   cluster's width on the tab list so the tabs stop before it.

   Measured rather than hard-coded: the width changes with the "last updated"
   text and with which league's season selector is showing.
   -------------------------------------------------------------------------- */
(function() {
  var pending = null;

  function sync() {
    pending = null;
    var cluster = document.getElementById("navbar_right_cluster");
    var tabs = document.getElementById("main_tabs");
    if (!cluster || !tabs) return;
    var c = cluster.getBoundingClientRect();
    var u = tabs.getBoundingClientRect();
    if (!c.width || !u.width) return;
    // Reserve only the part of the cluster that actually overhangs the tab
    // list, not its full width: the ul stops short of the viewport edge, so
    // reserving the whole cluster over-reserved by that margin and wrapped
    // the navbar onto a second row at 1440px, where it used to fit.
    var overlap = Math.ceil(u.right - c.left);
    document.documentElement.style.setProperty(
      "--navbar-cluster-w", Math.max(0, overlap + 8) + "px");
  }

  function schedule() {
    if (pending !== null) return;
    pending = window.requestAnimationFrame
      ? window.requestAnimationFrame(sync)
      : window.setTimeout(sync, 16);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", schedule);
  } else {
    schedule();
  }
  window.addEventListener("load", schedule);
  window.addEventListener("resize", schedule);

  // The cluster only reaches full width once the season selectors and the
  // last-updated text arrive, and the league switch swaps which selector shows.
  if (window.jQuery) {
    window.jQuery(document).on("shiny:connected shiny:value", schedule);
    window.jQuery(document).on("change", "#league_select", schedule);
  }
})();

/* ---- FLIP row transitions on table redraw ---------------------------------
   A ranking table that repaints in place discards the one thing a re-sort
   actually tells you: who moved, and how far. Measure each row's position
   before the redraw, compare after, and play the difference back as a
   transform so the movement is visible.

   Deliberately narrow: opt-in per table via the ibpl-flip class, capped at
   MAX_ROWS because past that the effect reads as noise rather than as
   information, and skipped entirely under prefers-reduced-motion. Rows
   present on only one side of the redraw are left alone -- animating arrival
   and departure would be decoration, not information.
   -------------------------------------------------------------------------- */
(function() {
  var MAX_ROWS = 60;
  var DURATION_MS = 300;
  var SEP = String.fromCharCode(31);
  var pending = null;

  function reducedMotion() {
    return window.matchMedia &&
           window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  }

  // Identity has to survive a re-sort, and the first cell alone does not carry
  // it: on both tables that opt in, column 1 is Team and column 2 is Player, so
  // a team's several players would share a key and animate from each other's
  // positions. Measured on a 30-row table: 14 unique first cells, 30 unique
  // first-and-second. SEP is a unit separator so a name cannot forge a key.
  function rowKey(tr) {
    var cells = tr.querySelectorAll("td");
    if (!cells.length) return null;
    var k = cells[0].textContent.trim();
    if (cells.length > 1) k += SEP + cells[1].textContent.trim();
    return k;
  }

  function measure(table) {
    var rows = table.querySelectorAll("tbody tr");
    if (!rows.length || rows.length > MAX_ROWS) return null;
    var boxes = {};
    for (var i = 0; i < rows.length; i++) {
      var k = rowKey(rows[i]);
      if (k) boxes[k] = rows[i].getBoundingClientRect().top;
    }
    return boxes;
  }

  function play(table, before) {
    var rows = table.querySelectorAll("tbody tr");
    var moved = [];
    for (var i = 0; i < rows.length; i++) {
      var k = rowKey(rows[i]);
      if (!k || !Object.prototype.hasOwnProperty.call(before, k)) continue;
      var delta = before[k] - rows[i].getBoundingClientRect().top;
      if (!delta) continue;
      rows[i].style.transition = "none";
      rows[i].style.transform = "translateY(" + delta + "px)";
      moved.push(rows[i]);
    }
    if (!moved.length) return;

    // Force the start frame to commit before the transition is attached.
    void table.offsetHeight;

    for (var j = 0; j < moved.length; j++) {
      moved[j].style.transition = "transform " + DURATION_MS + "ms cubic-bezier(.2,.7,.3,1)";
      moved[j].style.transform = "";
    }
    window.setTimeout(function() {
      for (var m = 0; m < moved.length; m++) {
        moved[m].style.transition = "";
        moved[m].style.transform = "";
      }
    }, DURATION_MS + 50);
  }

  function bind() {
    if (!window.jQuery) return;
    var $ = window.jQuery;

    $(document).on("preDraw.dt", function(e) {
      if (reducedMotion()) { pending = null; return; }
      var table = e.target;
      if (!table || !table.classList || !table.classList.contains("ibpl-flip")) return;
      // DataTables fires preDraw more than once per redraw, and only the first
      // lands before the rows are reordered. Letting a later one overwrite the
      // measurement compares the new layout against itself, so every delta is
      // zero and nothing animates -- measured: 30 rows move, 1 animates.
      if (pending && pending.table === table) return;
      pending = { table: table, boxes: measure(table) };
    });

    $(document).on("draw.dt", function(e) {
      if (!pending || pending.table !== e.target || !pending.boxes) {
        pending = null;
        return;
      }
      var table = pending.table;
      var boxes = pending.boxes;
      pending = null;
      play(table, boxes);
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bind);
  } else {
    bind();
  }
})();

/* ---- Collapsible filter panel ---------------------------------------------
   Tags the two columns of each tab's sidebarLayout so CSS can collapse them,
   then toggles a body class. The sidebar column is identified by the .well it
   contains rather than by a class added in R, so the ten tab files stay
   untouched and no :has() support is assumed.

   State is client-only and persists per browser. Storage can throw outright
   in a private window, so every access is guarded and a failure just means
   the panel opens expanded.
   -------------------------------------------------------------------------- */
(function() {
  var STORE_KEY = "ibpl_filters_collapsed";

  function readStored() {
    try {
      return window.localStorage.getItem(STORE_KEY) === "1";
    } catch (e) {
      return false;
    }
  }

  function writeStored(collapsed) {
    try {
      window.localStorage.setItem(STORE_KEY, collapsed ? "1" : "0");
    } catch (e) {
      /* private window or blocked site data: the toggle still works, it just
         does not survive a reload. */
    }
  }

  function tagColumns() {
    var wells = document.querySelectorAll(".tab-pane .well");
    for (var i = 0; i < wells.length; i++) {
      // Only tabs that render the shared toggle participate. Compare has a
      // sidebar too, but no toggle; tagging it would let another tab's saved
      // collapse state hide Compare's filters with no way to reopen them.
      var pane = wells[i].closest(".tab-pane");
      if (!pane || !pane.querySelector(".js-filters-toggle")) continue;
      var col = wells[i].closest("div[class*='col-sm-']");
      if (!col || col.classList.contains("ibpl-filter-col")) continue;
      col.classList.add("ibpl-filter-col");
      var main = col.nextElementSibling;
      if (main && main.className.indexOf("col-sm-") !== -1) {
        main.classList.add("ibpl-main-col");
      }
    }
  }

  function syncToggles(collapsed) {
    var buttons = document.querySelectorAll(".js-filters-toggle");
    for (var i = 0; i < buttons.length; i++) {
      buttons[i].setAttribute("aria-expanded", collapsed ? "false" : "true");
      buttons[i].setAttribute(
        "aria-label",
        collapsed ? "Show the filter panel" : "Hide the filter panel"
      );
    }
  }

  function apply(collapsed) {
    document.body.classList.toggle("filters-collapsed", collapsed);
    syncToggles(collapsed);
  }

  function init() {
    tagColumns();
    apply(readStored());

    document.addEventListener("click", function(e) {
      var btn = e.target.closest(".js-filters-toggle");
      if (!btn) return;
      e.preventDefault();
      var collapsed = !document.body.classList.contains("filters-collapsed");
      apply(collapsed);
      writeStored(collapsed);
      // DataTables sizes its header to the container width, so a column that
      // just changed width has to be told to remeasure.
      if (window.jQuery && window.jQuery.fn.dataTable) {
        window.jQuery.fn.dataTable.tables({ visible: true, api: true }).columns.adjust();
      }
    });

    // Tabs render lazily, so a tab shown for the first time brings untagged
    // columns with it.
    if (window.jQuery) {
      window.jQuery(document).on("shown.bs.tab shiny:value", function() {
        tagColumns();
        apply(document.body.classList.contains("filters-collapsed"));
      });
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();

/* A chip reached by Tab responds to Enter and Space, like the button its role
   claims it is. */
document.addEventListener("keydown", function(e) {
  if (e.key !== "Enter" && e.key !== " ") return;
  var chip = e.target.closest("[data-chip-focus]");
  if (!chip) return;
  e.preventDefault();
  chip.click();
});

/* ---- Pivot menu -----------------------------------------------------------
   A finding in one table is usually a question for another: a player with a
   large on/off gap raises "which lineups", a team raises "which games". The
   app already carries filter state between tabs for three Home cards; this
   opens the same road from any row.

   Identity is read from the row's data attributes, which DT sets from hidden
   id columns, and the label from textContent -- so nothing here depends on
   unescaped HTML reaching a cell.
   -------------------------------------------------------------------------- */
(function() {
  var menu = null;
  var opener = null;

  // The destination is read off the row, not hardcoded: the same two tables
  // serve both leagues, and each league's rows carry its own tab ids.
  var ACTIONS = [
    { attr: "data-pivot-lineups", label: "Lineups with this player", needs: "player" },
    { attr: "data-pivot-lineups", label: "Lineups for this team", needs: "team" },
    { attr: "data-pivot-gamelogs", label: "Game log for this team", needs: "team" }
  ];

  function close(restoreFocus) {
    if (!menu) return;
    menu.remove();
    menu = null;
    if (restoreFocus && opener && typeof opener.focus === "function") opener.focus();
    opener = null;
  }

  function send(action, row, label) {
    if (!window.Shiny || typeof window.Shiny.setInputValue !== "function") return;
    window.Shiny.setInputValue("pivot_action", {
      target: row.getAttribute(action.attr) || "",
      team_id: row.getAttribute("data-pivot-team") || "",
      // Only the entity this action is about. A row carries both ids, so
      // sending both would leave a player selected on a team-level pivot.
      player_id: action.needs === "player" ? (row.getAttribute("data-pivot-player") || "") : "",
      entity_name: label,
      rand: Math.random()
    }, { priority: "event" });
  }

  function open(row, x, y, trigger) {
    close();
    opener = trigger || null;
    var hasTeam = !!row.getAttribute("data-pivot-team");
    var hasPlayer = !!row.getAttribute("data-pivot-player");
    var firstCell = row.querySelector("td");
    var label = firstCell ? firstCell.textContent.trim() : "";

    var items = ACTIONS.filter(function(a) {
      // Both the entity and a destination for it have to be present.
      if (!row.getAttribute(a.attr)) return false;
      return a.needs === "team" ? hasTeam : hasPlayer;
    });
    if (!items.length) return;

    menu = document.createElement("div");
    menu.className = "ibpl-pivot-menu";
    menu.setAttribute("role", "menu");

    items.forEach(function(a) {
      var btn = document.createElement("button");
      btn.type = "button";
      btn.className = "ibpl-pivot-item";
      btn.setAttribute("role", "menuitem");
      btn.textContent = a.label;
      btn.addEventListener("click", function() {
        send(a, row, label);
        close();
      });
      menu.appendChild(btn);
    });

    document.body.appendChild(menu);
    var box = menu.getBoundingClientRect();
    menu.style.left = Math.min(x, window.innerWidth - box.width - 8) + "px";
    menu.style.top = Math.min(y, window.innerHeight - box.height - 8) + "px";
    var first = menu.querySelector(".ibpl-pivot-item");
    if (first) first.focus();
  }

  document.addEventListener("click", function(e) {
    if (menu && !e.target.closest(".ibpl-pivot-menu")) { close(); return; }

    var cell = e.target.closest("td");
    if (!cell || cell.cellIndex > 1) return;
    var row = cell.closest("tr[data-pivot-team], tr[data-pivot-player]");
    if (!row) return;

    e.preventDefault();
    e.stopPropagation();
    open(row, e.clientX, e.clientY, cell);
  });

  document.addEventListener("keydown", function(e) {
    if (e.key === "Escape") { close(true); return; }
    if (e.key !== "Enter" && e.key !== " ") return;

    var cell = e.target.closest("td[data-pivot-trigger]");
    if (!cell || cell.cellIndex > 1) return;
    var row = cell.closest("tr[data-pivot-team], tr[data-pivot-player]");
    if (!row) return;

    e.preventDefault();
    e.stopPropagation();
    var box = cell.getBoundingClientRect();
    open(row, box.left, box.bottom, cell);
  });

  window.addEventListener("resize", close);
  window.addEventListener("scroll", close, true);
})();

/* ---- Four Factors range toggle -------------------------------------------
   The Four Factors cell can show a range track, the on-court and off-court
   values and a points estimate under its headline. That is four lines per cell
   across ten cells, which is too much to scan, so the extra lines are hidden
   until asked for.

   A body class rather than per-cell state, for the same reason the filter
   collapse uses one: DataTables re-renders every cell on sort, page and filter,
   so anything stored on a cell is gone by the next draw. A class on <body>
   survives all of it and costs nothing to re-apply.

   Deliberately NOT persisted. The detail IS the view -- every load shows it,
   and the button is there for anyone who wants a bare grid of diffs for a
   moment. Remembering the hidden state would silently withhold the range
   tracks from someone who collapsed them once, weeks ago.
   -------------------------------------------------------------------------- */
(function() {
  function syncToggles(on) {
    var buttons = document.querySelectorAll(".js-ranges-toggle");
    for (var i = 0; i < buttons.length; i++) {
      buttons[i].setAttribute("aria-pressed", on ? "true" : "false");
      var label = buttons[i].querySelector(".js-ranges-toggle-label");
      if (label) label.textContent = on ? "Hide on/off detail" : "Show on/off detail";
    }
  }

  function apply(on) {
    document.body.classList.toggle("ff-ranges-off", !on);
    syncToggles(on);
  }

  function init() {
    apply(true);

    document.addEventListener("click", function(e) {
      var btn = e.target.closest(".js-ranges-toggle");
      if (!btn) return;
      e.preventDefault();
      var on = document.body.classList.contains("ff-ranges-off");
      apply(on);
      // Cell content changes width, which changes how the Team and Player
      // columns wrap, which changes row height. columns.adjust() alone does
      // not re-measure that -- collapsing left every row at its expanded
      // height until the next sort. A redraw does, and keeps the current page.
      if (window.jQuery && window.jQuery.fn.dataTable) {
        var api = window.jQuery.fn.dataTable.tables({ visible: true, api: true });
        api.columns.adjust();
        api.draw(false);
      }
    });

    // The button is inside a conditionalPanel and the tables render lazily, so
    // a freshly shown toggle has to be brought in line with the stored state.
    if (window.jQuery) {
      window.jQuery(document).on("shown.bs.tab shiny:value", function() {
        apply(!document.body.classList.contains("ff-ranges-off"));
      });
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();

/* ---- Lineup player chips (Tabs 2 and 10) ----------------------------------
   The client half of lineup_player_filter_ui(layout = "chips"). One roster of
   player chips replaces the three player boxes, and a mode switch says what a
   tap does: On (must be on), Any of (at least k of these), Off (must be off).
   A tap on a chip already in the current mode clears it.

   The three selectizes stay in the DOM, hidden, and remain the source of
   truth: a tap writes them, and any change to them -- restore, a row pivot, a
   chip-bar clear -- re-renders the chips. The roster (ordered by season
   minutes) arrives as the "lineup-chips-roster" message. The group count is
   the one input only this widget sets: <ns>players_on_any_min.
   -------------------------------------------------------------------------- */
(function() {
  var FOLD_AT = 12;
  var BOX = { on: "players_on", any: "players_on_any", off: "players_off" };
  var STATES = ["on", "any", "off"];
  var STATE_TEXT = { none: "not set", on: "must be on", any: "any of", off: "must be off" };
  var widgets = {};

  function widget(el) {
    if (!widgets[el.id]) widgets[el.id] = { roster: [], expanded: false, anyMin: 1, sentMin: 1 };
    return widgets[el.id];
  }

  function selectizeFor(el, state) {
    var sel = document.getElementById(el.getAttribute("data-ns") + BOX[state]);
    return sel && sel.selectize ? sel.selectize : null;
  }

  function valuesOf(s) {
    var v = s ? s.getValue() : [];
    if (!Array.isArray(v)) v = v ? [v] : [];
    return v.map(String);
  }

  // player id -> "on" | "any" | "off". The server keeps the boxes disjoint; if
  // they ever were not, the earlier box wins, as it does server-side.
  function readState(el) {
    var out = {};
    STATES.forEach(function(state) {
      valuesOf(selectizeFor(el, state)).forEach(function(id) {
        if (!out[id]) out[id] = state;
      });
    });
    return out;
  }

  function setBox(el, state, id, add, label) {
    var s = selectizeFor(el, state);
    if (!s) return;
    var vals = valuesOf(s).filter(function(v) { return v !== id; });
    if (add) {
      if (!s.options[id]) s.addOption({ value: id, label: label });
      vals.push(id);
    }
    s.setValue(vals, false);
  }

  // w.anyMin is the count asked for; what is shown and sent is that count
  // clamped to the current group. Clamping the stored value instead would lose
  // a restored count: the roster message (which carries it) lands before the
  // boxes' restored selections do, so the first render sees an empty group.
  function effectiveMin(w, groupSize) {
    return Math.max(1, Math.min(w.anyMin, groupSize - 1));
  }

  function sendAnyMin(el, w, value) {
    if (w.sentMin === value) return;
    if (!window.Shiny || typeof window.Shiny.setInputValue !== "function") return;
    w.sentMin = value;
    window.Shiny.setInputValue(el.getAttribute("data-ns") + "players_on_any_min", value);
  }

  // Provider names are often all caps; anything already mixed-case is kept.
  function displayName(name) {
    name = String(name || "").trim();
    if (name !== name.toUpperCase()) return name;
    return name.toLowerCase()
      .replace(/(^|[\s\-'.])([a-z])/g, function(m, p, c) { return p + c.toUpperCase(); })
      .replace(/\b(Ii|Iii|Iv)\b/g, function(m) { return m.toUpperCase(); });
  }

  // "Last, First" (EuroLeague) or "First Last" (Israeli).
  function nameParts(name) {
    name = displayName(name);
    var comma = name.indexOf(",");
    if (comma > 0) return { last: name.slice(0, comma).trim(), first: name.slice(comma + 1).trim() };
    var sp = name.indexOf(" ");
    return sp > 0 ? { last: name.slice(sp + 1), first: name.slice(0, sp) } : { last: name, first: "" };
  }

  // Surname only, with a first initial where two teammates share one.
  function chipLabels(roster) {
    var parts = roster.map(function(p) { return nameParts(p.name); });
    var seen = {};
    parts.forEach(function(p) { seen[p.last] = (seen[p.last] || 0) + 1; });
    return parts.map(function(p) {
      return seen[p.last] > 1 && p.first ? p.first.charAt(0) + ". " + p.last : p.last;
    });
  }

  function node(tag, cls, text) {
    var n = document.createElement(tag);
    if (cls) n.className = cls;
    if (text != null) n.textContent = text;
    return n;
  }

  function render(el) {
    var w = widget(el);
    var list = el.querySelector(".lineup-chips-list");
    var summary = el.querySelector(".lineup-chips-summary");
    if (!list || !summary) return;
    list.textContent = "";
    summary.textContent = "";
    el.classList.toggle("is-empty", !w.roster.length);
    if (!w.roster.length) {
      summary.appendChild(node("span", "lineup-chips-hint", "Pick a team to filter by player."));
      return;
    }

    var byId = readState(el);
    var labels = chipLabels(w.roster);
    var hidden = 0;
    w.roster.forEach(function(p, i) {
      var state = byId[String(p.id)] || "none";
      // A folded player who is set stays visible, so what is set is always shown.
      if (!w.expanded && i >= FOLD_AT && state === "none") { hidden += 1; return; }
      var full = displayName(p.name);
      var b = node("button", "lineup-chip", labels[i]);
      b.type = "button";
      b.setAttribute("data-id", String(p.id));
      b.setAttribute("data-state", state);
      b.setAttribute("aria-label", full + ", " + STATE_TEXT[state]);
      b.title = typeof p.min === "number" ? full + " · " + p.min + " min this season" : full;
      list.appendChild(b);
    });
    if (hidden > 0 || (w.expanded && w.roster.length > FOLD_AT)) {
      var more = node("button", "lineup-chip lineup-chip-more",
                      hidden > 0 ? "+" + hidden + " more" : "Show fewer");
      more.type = "button";
      more.setAttribute("data-fold", hidden > 0 ? "open" : "close");
      list.appendChild(more);
    }

    renderSummary(el, w, byId, labels);
  }

  function renderSummary(el, w, byId, labels) {
    var summary = el.querySelector(".lineup-chips-summary");
    var groups = { on: [], any: [], off: [] };
    w.roster.forEach(function(p, i) {
      var state = byId[String(p.id)];
      if (state) groups[state].push(labels[i]);
    });

    // All n of the group is just "On", so the count tops out at n - 1.
    var anyMin = effectiveMin(w, groups.any.length);
    sendAnyMin(el, w, anyMin);

    if (!groups.on.length && !groups.any.length && !groups.off.length) {
      summary.appendChild(node("span", "lineup-chips-hint", "All lineups. Pick a mode, then tap players."));
      return;
    }

    var text = node("span", "lineup-chips-sentence");
    var first = true;
    function sep() {
      if (!first) text.appendChild(node("span", "lineup-chips-conn", " · "));
      first = false;
    }
    if (groups.on.length) {
      sep();
      text.appendChild(node("b", "lineup-chips-on", groups.on.join(" + ")));
    }
    if (groups.any.length) {
      sep();
      // The count is only a choice from three players up: of two, "both" is
      // just On, so "one of" is the only meaningful count.
      if (groups.any.length < 3) {
        var lead = groups.any.length === 1 ? (groups.on.length ? "with " : "including ")
                                           : (groups.on.length ? "with one of " : "one of ");
        text.appendChild(node("span", "lineup-chips-conn", lead));
      } else {
        text.appendChild(node("span", "lineup-chips-conn", groups.on.length ? "with at least " : "at least "));
        var count = node("button", "lineup-chips-count", String(anyMin));
        count.type = "button";
        count.title = "How many of the group must be on together. Click to change.";
        count.setAttribute("aria-label", "At least " + anyMin + " of " + groups.any.length + ". Click to change.");
        text.appendChild(count);
        text.appendChild(node("span", "lineup-chips-conn", " of "));
      }
      text.appendChild(node("span", "lineup-chips-any", groups.any.join(", ")));
    }
    if (groups.off.length) {
      sep();
      text.appendChild(node("span", "lineup-chips-conn", "without "));
      text.appendChild(node("span", "lineup-chips-off", groups.off.join(", ")));
    }
    summary.appendChild(text);
    var clear = node("button", "lineup-chips-clear", "Clear");
    clear.type = "button";
    summary.appendChild(clear);
  }

  function focusAfterRender(el, selector) {
    var target = el.querySelector(selector);
    if (target) target.focus();
  }

  document.addEventListener("click", function(e) {
    var el = e.target.closest(".lineup-chips");
    if (!el) return;
    var w = widget(el);

    var mode = e.target.closest(".lineup-chips-mode");
    if (mode) {
      el.setAttribute("data-mode", mode.getAttribute("data-mode"));
      el.querySelectorAll(".lineup-chips-mode").forEach(function(b) {
        b.setAttribute("aria-checked", b === mode ? "true" : "false");
      });
      return;
    }

    var fold = e.target.closest("[data-fold]");
    if (fold) {
      w.expanded = fold.getAttribute("data-fold") === "open";
      render(el);
      focusAfterRender(el, "[data-fold]");
      return;
    }

    if (e.target.closest(".lineup-chips-count")) {
      var current = readState(el);
      var n = Object.keys(current).filter(function(id) { return current[id] === "any"; }).length;
      var shown = effectiveMin(w, n);
      w.anyMin = shown >= n - 1 ? 1 : shown + 1;
      render(el);
      focusAfterRender(el, ".lineup-chips-count");
      return;
    }

    if (e.target.closest(".lineup-chips-clear")) {
      STATES.forEach(function(state) {
        var s = selectizeFor(el, state);
        if (s && valuesOf(s).length) s.setValue([], false);
      });
      w.anyMin = 1;
      render(el);
      return;
    }

    var chip = e.target.closest(".lineup-chip[data-id]");
    if (chip) {
      var id = chip.getAttribute("data-id");
      var want = el.getAttribute("data-mode") || "on";
      var from = readState(el)[id] || "none";
      var to = from === want ? "none" : want;
      var player = w.roster.filter(function(p) { return String(p.id) === id; })[0];
      // Leave the old box before joining the new one: the reverse order would
      // briefly put the player in both, and the server would filter on that.
      if (from !== "none") setBox(el, from, id, false);
      if (to !== "none") setBox(el, to, id, true, player ? player.name : id);
      render(el);
      focusAfterRender(el, '.lineup-chip[data-id="' + id + '"]');
    }
  });

  // Selectize fires jQuery "change" on the original select, which a native
  // listener never sees; any box change (ours, restore, pivot, chip-bar clear)
  // re-renders its widget.
  function bindBoxChanges() {
    if (!window.jQuery) return false;
    window.jQuery(document).on("change", ".lineup-chips-model select", function() {
      var el = this.closest(".lineup-chips");
      if (el) render(el);
    });
    return true;
  }

  function registerRosterHandler() {
    if (!window.Shiny || typeof window.Shiny.addCustomMessageHandler !== "function") return false;
    window.Shiny.addCustomMessageHandler("lineup-chips-roster", function(msg) {
      var el = msg && msg.id ? document.getElementById(msg.id) : null;
      if (!el) return;
      var w = widget(el);
      var players = Array.isArray(msg.players) ? msg.players : [];
      var key = players.map(function(p) { return p.id; }).join(",");
      // A different team starts a fresh count. The same roster re-sent (the
      // team input echoing a restore, say) must not reset a restored one.
      if (key !== w.rosterKey) {
        w.anyMin = 1;
        w.expanded = false;
      }
      w.rosterKey = key;
      w.roster = players;
      if (typeof msg.any_min === "number" && msg.any_min >= 1) w.anyMin = Math.floor(msg.any_min);
      render(el);
    });
    return true;
  }

  function init() {
    var boundChanges = bindBoxChanges();
    var registered = registerRosterHandler();
    if (boundChanges && registered) return;
    var attempts = 0;
    var timer = window.setInterval(function() {
      attempts += 1;
      if (!boundChanges) boundChanges = bindBoxChanges();
      if (!registered) registered = registerRosterHandler();
      if ((boundChanges && registered) || attempts >= 40) window.clearInterval(timer);
    }, 250);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
