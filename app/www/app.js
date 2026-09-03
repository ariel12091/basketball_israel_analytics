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

(function() {
  var cfg = window.IBPL_IDLE_CONFIG || {};
  var timeoutMs = Math.max(1, Number(cfg.timeoutSec || 360)) * 1000;
  var warningMs = Math.max(1, Number(cfg.warningSec || 60)) * 1000;
  var ttlMs = Math.max(1, Number(cfg.stateTtlHours || 24)) * 60 * 60 * 1000;
  var stateVersion = Number(cfg.stateVersion || 1);
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
      // shinyapps.io's hosting layer, not Shiny itself. These are the nodes
      // that actually appear in production.
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
    // Present at page load on shinyapps.io; watch for it otherwise rather than
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
    if ((Date.now() - lastActivity) >= timeoutMs) idleExpired = true;
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
    timerId = window.setInterval(checkIdleState, 1000);
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
