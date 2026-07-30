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

  function sendShinyEvent(inputId, value) {
    if (!inputId || !window.Shiny || typeof window.Shiny.setInputValue !== "function") return;
    window.Shiny.setInputValue(inputId, value === undefined ? Math.random() : value, { priority: "event" });
  }

  document.addEventListener("click", function(e) {
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

  var keyBase = "ibpl_idle_resume:" + location.pathname.replace(/\/+$/, "");
  var tabIdKey = keyBase + ":tab_id";
  var tabId = getOrCreateTabId();
  var urlKey = keyBase + ":tab:" + tabId + ":bookmark:v" + stateVersion;
  var skipRestoreKey = keyBase + ":tab:" + tabId + ":skip_restore";
  var restoredFlagKey = keyBase + ":tab:" + tabId + ":restored";
  var hubTeamKey = "ibplHubTeam";

  var idleExpired = false;
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
      ".reconnect-dialog"
    ];
    for (var i = 0; i < selectors.length; i++) {
      var nodes = document.querySelectorAll(selectors[i]);
      for (var j = 0; j < nodes.length; j++) {
        nodes[j].style.setProperty("display", "none", "important");
        nodes[j].style.setProperty("pointer-events", "none", "important");
      }
    }
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
        '<span>Session paused \u2014 resuming on activity.</span>' +
        '<button type="button" id="ibpl-idle-fresh">Start fresh</button>';
      document.body.appendChild(pill);
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
      if (msg && msg.url) storeBookmarkUrl(msg.url + "&ibpl_v=" + stateVersion);
    });
    window.Shiny.addCustomMessageHandler("ibpl-store-hub-team", function(msg) {
      if (msg && msg.teamId) safeLocalSet(hubTeamKey, String(msg.teamId));
    });
    return true;
  }

  function handleConnected() {
    registerMessageHandlers();
    toggleNativeDisconnectUi(false);
    sendActivity(true);
    if (window.Shiny && typeof window.Shiny.setInputValue === "function") {
      window.Shiny.setInputValue(
        "hub_remembered_team",
        safeLocalGet(hubTeamKey) || ""
      );
    }
  }

  function bindActivity() {
    var events = ["mousemove", "mousedown", "keydown", "scroll", "touchstart", "click"];
    for (var i = 0; i < events.length; i++) {
      document.addEventListener(events[i], function() {
        if (idleExpired) { restoreOnReturn(); return; }
        markActivity(false);
      }, { passive: true });
    }
    document.addEventListener("visibilitychange", function() {
      if (document.visibilityState !== "visible") return;
      if (idleExpired || !shinyReadyForRestore()) { restoreOnReturn(); return; }
      markActivity(true);
    });
    if (window.jQuery) {
      window.jQuery(document).on("shiny:connected", handleConnected);
      window.jQuery(document).on("shiny:disconnected", handleDisconnected);
    } else {
      document.addEventListener("shiny:connected", handleConnected);
      document.addEventListener("shiny:disconnected", handleDisconnected);
    }
    registerMessageHandlers();
    sendActivity(true);
    if (timerId) window.clearInterval(timerId);
    timerId = window.setInterval(checkIdleState, 1000);
  }

  clearBookmarkParams();
  if (safeSessionGet(skipRestoreKey)) safeSessionRemove(skipRestoreKey);
  if (safeSessionGet(restoredFlagKey)) {
    safeSessionRemove(restoredFlagKey);
    window.setTimeout(showRestoredNotice, 400);
  }

  window.ibplDebugSavedSession = function() {
    return { url: loadBookmarkUrl(), idleExpired: idleExpired, tabId: tabId };
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
    { tab: "compare", inputId: "cmp_mode", items: ["Teams", "Lineups", "Players"], def: "Teams" }
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
