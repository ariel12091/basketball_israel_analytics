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
  var lastSent = 0;
  var lastActivity = Date.now();
  var minIntervalMs = 15000;
  var timerId = null;
  var idleExpired = false;
  var saveTimer = null;
  var restoreSent = false;
  var pendingRestoreState = null;
  var lastKnownTab = null;
  var applyingRestoreValues = false;
  var finalBrowserApplySent = false;
  var restoreFinishFallbackId = null;
  var restoreFinishRequested = false;
  var restoreSendTimerId = null;
  var restoreSendAttempt = 0;
  var restoreMaxSendAttempts = 40;
  var restoreSendPollMs = 500;
  var restoreCompletionHoldMs = 15000;
  var dependentRestoreActive = false;
  var suppressSaveUntil = 0;
  var cfg = window.IBPL_IDLE_CONFIG || {};
  var timeoutMs = Math.max(1, Number(cfg.timeoutSec || 360)) * 1000;
  var warningMs = Math.max(1, Number(cfg.warningSec || 60)) * 1000;
  var ttlMs = Math.max(1, Number(cfg.stateTtlHours || 24)) * 60 * 60 * 1000;
  var stateVersion = Number(cfg.stateVersion || 1);
  var maxStateChars = Math.max(10000, Number(cfg.maxStateChars || 120000));
  warningMs = Math.min(warningMs, Math.max(1000, timeoutMs - 1000));
  var keyBase = "ibpl_idle_resume:" + location.pathname.replace(/\/+$/, "");
  var tabIdKey = keyBase + ":tab_id";
  var tabId = getOrCreateTabId();
  var stateKey = keyBase + ":tab:" + tabId + ":state:v" + stateVersion;
  var lastStateKey = keyBase + ":tab:" + tabId + ":last_state:v" + stateVersion;
  var lastTabKey = keyBase + ":tab:" + tabId + ":last_tab:v" + stateVersion;
  var restoreIntentKey = keyBase + ":tab:" + tabId + ":restore_intent";
  var skipRestoreKey = keyBase + ":tab:" + tabId + ":skip_restore";
  var reconnectingKey = keyBase + ":tab:" + tabId + ":reconnecting";
  var restoreCompleteKey = keyBase + ":tab:" + tabId + ":restore_complete";
  var legacyRestoreIntentKey = keyBase + ":restore_intent";
  var legacySkipRestoreKey = keyBase + ":skip_restore";
  var restorePending = !!safeSessionGet(restoreIntentKey) && !safeSessionGet(skipRestoreKey);
  var pageUnloading = false;
  var suppressDisconnectUntil = 0;
  var restoreGraceMs = 15000;
  var reconnectIntentTtlMs = 60000;
  safeLocalRemove(legacyRestoreIntentKey);
  safeLocalRemove(legacySkipRestoreKey);
  var validTabValues = {
    home: true,
    onoff: true,
    lineup_data: true,
    team_ratings: true,
    game_logs: true,
    traditional_stats: true,
    compare: true
  };
  var dateRangeIds = [
    "date_range", "ld_dates", "tr_dates", "gl_dates", "ts_dates",
    "cmp_players_dates", "cmp_player_a_dates", "cmp_player_b_dates"
  ];
  var persistIds = [
    "game_year", "home_team",
    "onoff_view_mode", "date_range", "teams", "on_num_starters_off_mode", "on_num_starters_off",
    "on_num_starters_def_mode", "on_num_starters_def", "on_game_type", "on_opponents",
    "on_home_away", "on_outcome", "on_gn_min", "on_gn_max", "on_last_n",
    "on_opp_rank_side", "on_opp_rank_n", "on_opp_rank_metric", "min_all_poss", "min_on_poss",
    "ld_view_mode", "ld_minposs", "ld_num", "ld_lineup_filter-team",
    "ld_lineup_filter-players_on", "ld_lineup_filter-players_off",
    "ld_num_starters_off_mode", "ld_num_starters_off", "ld_num_starters_def_mode",
    "ld_num_starters_def", "ld_dates", "ld_clutch_enabled", "ld_clutch_margin",
    "ld_clutch_status", "ld_clutch_minutes", "ld_clutch_ot_margin", "ld_game_type",
    "ld_opponents", "ld_home_away", "ld_outcome", "ld_gn_min", "ld_gn_max",
    "ld_last_n", "ld_opp_rank_side", "ld_opp_rank_n", "ld_opp_rank_metric",
    "tr_view_mode", "tr_trad_defense_mode", "tr_trad_display_mode", "tr_dates",
    "tr_clutch_enabled", "tr_clutch_margin", "tr_clutch_status", "tr_clutch_minutes",
    "tr_clutch_ot_margin", "tr_num_starters_off_mode", "tr_num_starters_off",
    "tr_num_starters_def_mode", "tr_num_starters_def", "tr_game_type", "tr_opponents",
    "tr_home_away", "tr_outcome", "tr_gn_min", "tr_gn_max", "tr_last_n",
    "tr_opp_rank_side", "tr_opp_rank_n", "tr_opp_rank_metric",
    "gl_view_mode", "gl_team", "gl_dates", "gl_num_starters_off_mode", "gl_num_starters_off",
    "gl_num_starters_def_mode", "gl_num_starters_def", "gl_game_type", "gl_opponents",
    "gl_home_away", "gl_outcome", "gl_gn_min", "gl_gn_max", "gl_last_n",
    "ts_dates", "ts_teams", "ts_players", "ts_display_mode", "ts_min_gp_slider", "ts_min_gp",
    "ts_show_ineligible", "ts_clutch_enabled", "ts_clutch_margin", "ts_clutch_status",
    "ts_clutch_minutes", "ts_clutch_ot_margin", "ts_game_type", "ts_opponents",
    "ts_home_away", "ts_outcome", "ts_gn_min", "ts_gn_max", "ts_last_n",
    "ts_opp_rank_side", "ts_opp_rank_n", "ts_opp_rank_metric",
    "cmp_mode", "cmp_preset", "cmp_min_poss", "cmp_split_date", "cmp_split_gn",
    "cmp_player_compare_mode", "cmp_players_dates", "cmp_players_gn_min", "cmp_players_gn_max",
    "cmp_player_a_dates", "cmp_player_a_gn_min", "cmp_player_a_gn_max",
    "cmp_player_a_list_team_filter", "cmp_player_a", "cmp_player_a_team",
    "cmp_a_starters_mode", "cmp_a_starters_val", "cmp_a_opp_starters_mode",
    "cmp_a_opp_starters_val", "cmp_a_teams", "cmp_a_home_away", "cmp_a_outcome",
    "cmp_a_clutch", "cmp_a_clutch_margin", "cmp_a_clutch_minutes", "cmp_a_opponents",
    "cmp_a_game_type", "cmp_a_opp_rank_side", "cmp_a_opp_rank_n", "cmp_a_opp_rank_metric",
    "cmp_player_b_list_team_filter", "cmp_player_b", "cmp_player_b_team",
    "cmp_player_b_dates", "cmp_player_b_gn_min", "cmp_player_b_gn_max",
    "cmp_b_starters_mode", "cmp_b_starters_val", "cmp_b_opp_starters_mode",
    "cmp_b_opp_starters_val", "cmp_b_teams", "cmp_b_home_away", "cmp_b_outcome",
    "cmp_b_clutch", "cmp_b_clutch_margin", "cmp_b_clutch_minutes", "cmp_b_opponents",
    "cmp_b_game_type", "cmp_b_opp_rank_side", "cmp_b_opp_rank_n", "cmp_b_opp_rank_metric",
    "cmp_lu_num", "cmp_lu_filter-team", "cmp_lu_filter-players_on", "cmp_lu_filter-players_off",
    "cmp_team_player_rate_mode", "cmp_rate_mode"
  ];
  var dependentPlayerGroups = [
    {
      team: "ld_lineup_filter-team",
      players: ["ld_lineup_filter-players_on", "ld_lineup_filter-players_off"]
    },
    {
      team: "cmp_lu_filter-team",
      players: ["cmp_lu_filter-players_on", "cmp_lu_filter-players_off"]
    }
  ];
  var delayedPlayerRestoreTimers = [];

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

  function runWhenIdle(fn, timeout) {
    if (typeof window.requestIdleCallback === "function") {
      window.requestIdleCallback(fn, { timeout: timeout || 1000 });
      return;
    }
    window.setTimeout(fn, 0);
  }

  function normalizeTabValue(value) {
    if (typeof value !== "string") return null;
    return validTabValues[value] ? value : null;
  }

  function tabValueFromElement(el) {
    if (!el) return null;
    var val = normalizeTabValue(el.getAttribute("data-value"));
    if (val) return val;

    var target = el.getAttribute("data-bs-target") || el.getAttribute("href") || "";
    if (target.charAt(0) === "#") {
      var pane = document.querySelector(target);
      val = pane ? normalizeTabValue(pane.getAttribute("data-value")) : null;
      if (val) return val;
    }
    return null;
  }

  function rememberActiveTab(value) {
    var tab = normalizeTabValue(value);
    if (!tab) return false;
    lastKnownTab = tab;
    safeSessionSet(lastTabKey, tab);
    return true;
  }

  function selectorValue(value) {
    return String(value).replace(/\\/g, "\\\\").replace(/"/g, "\\\"");
  }

  function findMainTabLink(tab) {
    var quoted = selectorValue(tab);
    return document.querySelector([
      ".navbar a[data-value=\"" + quoted + "\"]",
      ".navbar button[data-value=\"" + quoted + "\"]",
      ".navbar .nav-link[data-value=\"" + quoted + "\"]"
    ].join(", "));
  }

  function activateMainTab(tab) {
    tab = normalizeTabValue(tab);
    if (!tab) return false;
    var link = findMainTabLink(tab);
    if (!link) return false;
    rememberActiveTab(tab);

    if (link.classList && link.classList.contains("active")) return true;
    try {
      if (window.bootstrap && window.bootstrap.Tab && typeof window.bootstrap.Tab.getOrCreateInstance === "function") {
        window.bootstrap.Tab.getOrCreateInstance(link).show();
        return true;
      }
    } catch (e) {}
    try {
      if (window.jQuery && window.jQuery.fn && typeof window.jQuery.fn.tab === "function") {
        window.jQuery(link).tab("show");
        return true;
      }
    } catch (e2) {}
    if (typeof link.click === "function") {
      link.click();
      return true;
    }
    return false;
  }

  function activateRestoreTab(state) {
    var tab = state && state.values ? normalizeTabValue(state.values.main_tabs) : null;
    if (!tab) return;
    activateMainTab(tab);
    window.setTimeout(function() { activateMainTab(tab); }, 250);
    window.setTimeout(function() { activateMainTab(tab); }, 1200);
  }

  function activeTabValue() {
    var inputValues = window.Shiny && window.Shiny.shinyapp && window.Shiny.shinyapp.$inputValues;
    var shinyTab = inputValues ? (inputValues.main_tabs || inputValues["main_tabs:shiny.tabinput"]) : null;
    shinyTab = normalizeTabValue(shinyTab);
    if (shinyTab) {
      lastKnownTab = shinyTab;
      return shinyTab;
    }

    var active = document.querySelector([
      ".navbar .nav-link.active[data-value]",
      ".navbar .nav-item.active > .nav-link[data-value]",
      ".navbar li.active > a[data-value]",
      ".nav-tabs .nav-link.active[data-value]",
      ".nav-tabs .nav-item.active > .nav-link[data-value]",
      ".nav-tabs li.active > a[data-value]",
      ".tab-content .tab-pane.active[data-value]"
    ].join(", "));
    var activeTab = tabValueFromElement(active);
    if (activeTab) lastKnownTab = activeTab;
    var storedTab = normalizeTabValue(safeSessionGet(lastTabKey));
    if (storedTab) lastKnownTab = storedTab;
    return activeTab || storedTab || lastKnownTab;
  }

  function isDateRangeId(id) {
    return dateRangeIds.indexOf(id) !== -1;
  }

  function shinyInputValue(id) {
    var inputValues = window.Shiny && window.Shiny.shinyapp && window.Shiny.shinyapp.$inputValues;
    if (!inputValues) return undefined;
    if (Object.prototype.hasOwnProperty.call(inputValues, id)) return inputValues[id];

    var prefixes = [
      id + ":shiny.",
      id + ":"
    ];
    for (var p = 0; p < prefixes.length; p++) {
      for (var key in inputValues) {
        if (Object.prototype.hasOwnProperty.call(inputValues, key) && key.indexOf(prefixes[p]) === 0) {
          return inputValues[key];
        }
      }
    }
    return undefined;
  }

  function isInvalidPersistedToken(value) {
    if (typeof value !== "string") return false;
    var token = value.trim().toLowerCase();
    return token === "undefined" || token === "null" || token === "nan" || token === "na";
  }

  function sanitizePersistedValue(value) {
    if (value === undefined || value === null) return null;
    if (Array.isArray(value)) {
      var values = [];
      for (var i = 0; i < value.length; i++) {
        var item = sanitizePersistedValue(value[i]);
        if (Array.isArray(item)) {
          values = values.concat(item);
        } else if (item !== null) {
          values.push(item);
        }
      }
      return values;
    }
    if (typeof value === "object" && Object.prototype.hasOwnProperty.call(value, "value")) {
      return sanitizePersistedValue(value.value);
    }
    if (isInvalidPersistedToken(value)) return null;
    return value;
  }

  function sanitizeStateValues(values) {
    var out = {};
    if (!values) return out;
    for (var key in values) {
      if (!Object.prototype.hasOwnProperty.call(values, key)) continue;
      var value = sanitizePersistedValue(values[key]);
      if (value === null) continue;
      out[key] = value;
    }
    return out;
  }

  function normalizeCachedInputValue(value) {
    if (value === undefined || value === null) return null;
    if (Array.isArray(value)) return sanitizePersistedValue(value);
    if (typeof value === "boolean" || typeof value === "number") return value;
    if (typeof value === "string") return isInvalidPersistedToken(value) ? null : value;
    if (typeof value === "object" && Object.prototype.hasOwnProperty.call(value, "value")) {
      return normalizeCachedInputValue(value.value);
    }
    return null;
  }

  function readInputValue(id) {
    var cached = normalizeCachedInputValue(shinyInputValue(id));
    if (cached != null) return cached;

    var radios = document.querySelectorAll("input[type=\"radio\"][name=\"" + id + "\"]");
    if (radios.length) {
      for (var r = 0; r < radios.length; r++) {
        if (radios[r].checked) return radios[r].value;
      }
      return null;
    }

    if (isDateRangeId(id)) {
      var range = document.getElementById(id);
      if (!range) return null;
      var rangeInputs = range.querySelectorAll("input");
      if (rangeInputs.length < 2) return null;
      return [rangeInputs[0].value || "", rangeInputs[1].value || ""];
    }

    var el = document.getElementById(id);
    if (!el) return null;

    if (el.type === "checkbox") return !!el.checked;
    if (el.selectize) {
      var value = el.selectize.getValue();
      return Array.isArray(value) ? value : String(value || "");
    }
    if (el.tagName === "SELECT" && el.multiple) {
      return Array.prototype.slice.call(el.selectedOptions).map(function(opt) { return opt.value; });
    }
    if (window.jQuery) {
      var slider = window.jQuery(el).data("ionRangeSlider");
      if (slider && slider.result) return slider.result.from;
    }
    return el.value;
  }

  function notifyShinyInput(id, value) {
    if (!shinyReadyForRestore()) return false;
    try {
      window.Shiny.setInputValue(id, value, { priority: "event" });
      return true;
    } catch (e) {
      return false;
    }
  }

  function valueArray(value) {
    value = sanitizePersistedValue(value);
    if (Array.isArray(value)) {
      return value.map(function(v) { return String(v); }).filter(function(v) {
        return !isInvalidPersistedToken(v);
      });
    }
    if (value === null || value === undefined || value === "") return [];
    return [String(value)];
  }

  function maybeEmitInput(id, value, emit) {
    if (!emit) return;
    notifyShinyInput(id, value);
  }

  function applyDateRangeValue(id, value, emit) {
    if (!isDateRangeId(id) || !Array.isArray(value) || value.length < 2) return false;
    var range = document.getElementById(id);
    if (!range) return false;
    var inputs = range.querySelectorAll("input");
    if (inputs.length < 2) return false;
    inputs[0].value = value[0] || "";
    inputs[1].value = value[1] || "";
    if (emit) notifyShinyInput(id, [inputs[0].value, inputs[1].value]);
    return true;
  }

  function applyRadioValue(id, value, emit) {
    var radios = document.getElementsByName(id);
    if (!radios || !radios.length) return false;
    var selected = String(value == null ? "" : value);
    var applied = false;
    for (var r = 0; r < radios.length; r++) {
      if (radios[r].type !== "radio") continue;
      radios[r].checked = radios[r].value === selected;
      if (radios[r].checked) applied = true;
    }
    if (applied && emit) notifyShinyInput(id, selected);
    return applied;
  }

  function ensureSelectOption(el, value) {
    var values = valueArray(value);
    for (var i = 0; i < values.length; i++) {
      var val = values[i];
      if (!val) continue;
      var exists = false;
      for (var j = 0; j < el.options.length; j++) {
        if (el.options[j].value === val) {
          exists = true;
          break;
        }
      }
      if (!exists) el.add(new Option(val, val));
    }
  }

  function ensureSelectizeOption(selectize, value) {
    var values = valueArray(value);
    for (var i = 0; i < values.length; i++) {
      var val = values[i];
      if (!val || Object.prototype.hasOwnProperty.call(selectize.options, val)) continue;
      selectize.addOption({ value: val, text: val });
    }
    selectize.refreshOptions(false);
  }

  function selectizeHasValue(selectize, value) {
    var values = valueArray(value);
    if (!values.length) return true;
    for (var i = 0; i < values.length; i++) {
      if (!Object.prototype.hasOwnProperty.call(selectize.options, values[i])) return false;
    }
    return true;
  }

  function applyInputValue(id, value, emit, options) {
    value = sanitizePersistedValue(value);
    if (value === null || value === undefined) return false;
    emit = !!emit;
    options = options || {};
    if (applyDateRangeValue(id, value, emit)) return true;
    if (applyRadioValue(id, value, emit)) return true;

    var el = document.getElementById(id);
    if (!el) {
      if (emit) notifyShinyInput(id, value);
      return false;
    }

    if (el.type === "checkbox") {
      el.checked = value === true || String(value).toLowerCase() === "true" || String(value) === "1";
      maybeEmitInput(id, el.checked, emit);
      return true;
    }

    if (el.selectize) {
      if (options.requireExistingSelectizeOption && !selectizeHasValue(el.selectize, value)) {
        return false;
      }
      var selectValues = valueArray(value);
      var selectValue = el.multiple ? selectValues : (selectValues.length ? selectValues[0] : "");
      if (!options.requireExistingSelectizeOption) {
        ensureSelectizeOption(el.selectize, selectValue);
      }
      el.selectize.setValue(selectValue, true);
      if (emit) notifyShinyInput(id, selectValue);
      return true;
    }

    if (el.tagName === "SELECT") {
      ensureSelectOption(el, value);
      if (el.multiple) {
        var vals = valueArray(value);
        for (var o = 0; o < el.options.length; o++) {
          el.options[o].selected = vals.indexOf(el.options[o].value) !== -1;
        }
        maybeEmitInput(id, vals, emit);
      } else {
        el.value = String(value);
        maybeEmitInput(id, el.value, emit);
      }
      return true;
    }

    if (window.jQuery) {
      var slider = window.jQuery(el).data("ionRangeSlider");
      if (slider && typeof slider.update === "function") {
        var sliderValue = Number(Array.isArray(value) ? value[0] : value);
        if (isFinite(sliderValue)) {
          slider.update({ from: sliderValue });
          if (emit) notifyShinyInput(id, sliderValue);
          return true;
        }
      }
    }

    el.value = Array.isArray(value) ? value[0] : value;
    maybeEmitInput(id, el.value, emit);
    return true;
  }

  function clearDelayedPlayerRestores() {
    for (var i = 0; i < delayedPlayerRestoreTimers.length; i++) {
      window.clearTimeout(delayedPlayerRestoreTimers[i]);
    }
    delayedPlayerRestoreTimers = [];
  }

  function clearRestoreSendTimer() {
    if (restoreSendTimerId) {
      window.clearTimeout(restoreSendTimerId);
      restoreSendTimerId = null;
    }
  }

  function isDependentLineupInput(id) {
    for (var g = 0; g < dependentPlayerGroups.length; g++) {
      if (dependentPlayerGroups[g].team === id) return true;
      if (dependentPlayerGroups[g].players.indexOf(id) !== -1) return true;
    }
    return false;
  }

  function reapplyDependentPlayerInputs(values, emit) {
    if (!values) return;
    clearDelayedPlayerRestores();
    dependentRestoreActive = false;
    var attempts = 0;
    var maxAttempts = 12;

    function hasRestoreValue(id) {
      return Object.prototype.hasOwnProperty.call(values, id) && valueArray(values[id]).length > 0;
    }

    function hasAnyDependentRestoreValue() {
      for (var g = 0; g < dependentPlayerGroups.length; g++) {
        var group = dependentPlayerGroups[g];
        if (hasRestoreValue(group.team)) return true;
        for (var p = 0; p < group.players.length; p++) {
          if (hasRestoreValue(group.players[p])) return true;
        }
      }
      return false;
    }

    function completeDependentRestore() {
      dependentRestoreActive = false;
      if (restoreFinishRequested) {
        requestRestoreFinish(false);
      }
    }

    if (!hasAnyDependentRestoreValue()) return;
    dependentRestoreActive = true;

    function attemptRestore() {
      attempts += 1;
      var pending = false;
      applyingRestoreValues = true;
      suppressSaveUntil = Date.now() + 1500;
      try {
        // These choices are server-owned; wait for real options instead of creating saved values as new options.
        dependentPlayerGroups.forEach(function(group) {
          if (Object.prototype.hasOwnProperty.call(values, group.team)) {
            var teamApplied = applyInputValue(group.team, values[group.team], emit, {
              requireExistingSelectizeOption: true
            });
            if (!teamApplied && hasRestoreValue(group.team)) pending = true;
          }
          group.players.forEach(function(id) {
            if (Object.prototype.hasOwnProperty.call(values, id)) {
              var playerApplied = applyInputValue(id, values[id], emit, {
                requireExistingSelectizeOption: true
              });
              if (!playerApplied && hasRestoreValue(id)) pending = true;
            }
          });
        });
      } finally {
        applyingRestoreValues = false;
      }

      if (pending && attempts < maxAttempts) {
        delayedPlayerRestoreTimers.push(window.setTimeout(attemptRestore, 750));
      } else {
        completeDependentRestore();
      }
    }

    delayedPlayerRestoreTimers.push(window.setTimeout(attemptRestore, 700));
  }

  function applyRestoreValues(values, emit, includeGameYear) {
    if (!values) return;
    applyingRestoreValues = true;
    suppressSaveUntil = Date.now() + 1500;
    try {
      if (includeGameYear !== false && Object.prototype.hasOwnProperty.call(values, "game_year")) {
        applyInputValue("game_year", values.game_year, emit);
      }
      if (Object.prototype.hasOwnProperty.call(values, "main_tabs")) {
        activateMainTab(values.main_tabs);
      }
      for (var key in values) {
        if (!Object.prototype.hasOwnProperty.call(values, key)) continue;
        if (key === "main_tabs" || key === "game_year") continue;
        if (isDependentLineupInput(key)) continue;
        applyInputValue(key, values[key], emit);
      }
      reapplyDependentPlayerInputs(values, emit);
    } finally {
      applyingRestoreValues = false;
    }
  }

  function compactValue(value) {
    value = sanitizePersistedValue(value);
    if (value == null) return null;
    if (Array.isArray(value)) {
      return value.map(function(v) { return String(v).slice(0, 200); }).filter(function(v) {
        return !isInvalidPersistedToken(v);
      }).slice(0, 80);
    }
    if (typeof value === "boolean" || typeof value === "number") return value;
    value = String(value).slice(0, 200);
    return isInvalidPersistedToken(value) ? null : value;
  }

  function readState() {
    var values = {};
    var tab = activeTabValue();
    if (tab) values.main_tabs = tab;
    persistIds.forEach(function(id) {
      var value = compactValue(readInputValue(id));
      if (value == null) return;
      values[id] = value;
    });
    return {
      version: stateVersion,
      path: location.pathname,
      tabId: tabId,
      savedAt: Date.now(),
      values: values
    };
  }

  function serializeState(state) {
    var serialized = JSON.stringify(state);
    if (serialized.length > maxStateChars) return null;
    return serialized;
  }

  function saveState(force, persistLast) {
    if (restorePending && !force) return;
    var state = readState();
    var serialized = serializeState(state);
    if (!serialized) return;
    safeSessionSet(stateKey, serialized);
    if (persistLast) safeLocalSet(lastStateKey, serialized);
  }

  function scheduleSave() {
    if (applyingRestoreValues) return;
    if (Date.now() < suppressSaveUntil) return;
    if (restorePending) return;
    if (saveTimer) window.clearTimeout(saveTimer);
    saveTimer = window.setTimeout(function() {
      saveTimer = null;
      runWhenIdle(function() { saveState(false, false); }, 1000);
    }, 350);
  }

  function parseStoredState(raw, removeFn) {
    try {
      var state = JSON.parse(raw);
      if (!state || state.version !== stateVersion || !state.savedAt || !state.values) return null;
      if ((Date.now() - Number(state.savedAt)) > ttlMs) {
        if (typeof removeFn === "function") removeFn();
        return null;
      }
      state.values = sanitizeStateValues(state.values);
      return state;
    } catch (e) {
      if (typeof removeFn === "function") removeFn();
      return null;
    }
  }

  function loadState(allowLast) {
    var raw = safeSessionGet(stateKey);
    if (raw) {
      var sessionState = parseStoredState(raw, function() { safeSessionRemove(stateKey); });
      if (sessionState) return sessionState;
    }
    if (!allowLast) return null;
    raw = safeLocalGet(lastStateKey);
    if (!raw) return null;
    return parseStoredState(raw, function() { safeLocalRemove(lastStateKey); });
  }

  function sessionNumber(key) {
    var val = Number(safeSessionGet(key));
    return isFinite(val) ? val : 0;
  }

  function restoreCompletedRecently() {
    var completedAt = sessionNumber(restoreCompleteKey);
    return completedAt > 0 && (Date.now() - completedAt) < restoreGraceMs;
  }

  function markRestoreIntent() {
    var now = String(Date.now());
    safeSessionSet(restoreIntentKey, now);
    safeSessionSet(reconnectingKey, now);
    safeSessionRemove(skipRestoreKey);
    restorePending = true;
    toggleNativeDisconnectUi(true);
  }

  function reconnectIntentActive() {
    var startedAt = sessionNumber(reconnectingKey);
    return startedAt > 0 && (Date.now() - startedAt) < reconnectIntentTtlMs;
  }

  function shinyReadyForRestore() {
    if (!window.Shiny || typeof window.Shiny.setInputValue !== "function") return false;
    var shinyapp = window.Shiny.shinyapp;
    if (!shinyapp) return false;
    var socket = shinyapp.$socket || shinyapp.socket;
    if (socket && typeof socket.readyState === "number" && socket.readyState !== 1) return false;
    return true;
  }

  function restoreTargetInputsReady(state) {
    var values = state && state.values ? state.values : {};
    var tab = normalizeTabValue(values.main_tabs);
    if (tab && !findMainTabLink(tab)) return false;

    function selectizeReady(id) {
      if (!Object.prototype.hasOwnProperty.call(values, id)) return true;
      var el = document.getElementById(id);
      return !!(el && el.selectize);
    }

    if (tab === "lineup_data") {
      return selectizeReady("ld_lineup_filter-team") &&
        selectizeReady("ld_lineup_filter-players_on") &&
        selectizeReady("ld_lineup_filter-players_off");
    }

    if (tab === "compare") {
      return selectizeReady("cmp_lu_filter-team") &&
        selectizeReady("cmp_lu_filter-players_on") &&
        selectizeReady("cmp_lu_filter-players_off");
    }

    return true;
  }

  function scheduleRestoreSend(delayMs) {
    clearRestoreSendTimer();
    restoreSendTimerId = window.setTimeout(attemptRestoreSend, delayMs);
  }

  function attemptRestoreSend() {
    restoreSendTimerId = null;
    if (restoreSent || !shouldRestoreState()) return;
    pendingRestoreState = pendingRestoreState || loadState(true);
    if (!pendingRestoreState) {
      safeSessionRemove(restoreIntentKey);
      safeSessionRemove(reconnectingKey);
      restorePending = false;
      toggleNativeDisconnectUi(false);
      return;
    }

    activateRestoreTab(pendingRestoreState);
    if (!shinyReadyForRestore()) {
      restoreSendAttempt += 1;
      scheduleRestoreSend(restoreSendPollMs);
      return;
    }

    if (!restoreTargetInputsReady(pendingRestoreState) && restoreSendAttempt < restoreMaxSendAttempts) {
      restoreSendAttempt += 1;
      scheduleRestoreSend(restoreSendPollMs);
      return;
    }

    if (sendRestoreState("final")) {
      scheduleRestoreFinishFallback();
      return;
    }

    restoreSendAttempt += 1;
    if (restoreSendAttempt < restoreMaxSendAttempts) {
      scheduleRestoreSend(restoreSendPollMs);
    }
  }

  function finishRestoreCycle() {
    clearRestoreSendTimer();
    if (restoreFinishFallbackId) {
      window.clearTimeout(restoreFinishFallbackId);
      restoreFinishFallbackId = null;
    }
    clearDelayedPlayerRestores();
    dependentRestoreActive = false;
    restoreFinishRequested = false;
    safeSessionRemove(restoreIntentKey);
    safeSessionRemove(reconnectingKey);
    safeSessionSet(restoreCompleteKey, String(Date.now()));
    suppressDisconnectUntil = Date.now() + restoreGraceMs;
    restorePending = false;
    restoreSent = false;
    finalBrowserApplySent = false;
    restoreSendAttempt = 0;
    pendingRestoreState = null;
    clearIdleOverlay();
    sendActivity(true);
  }

  function requestRestoreFinish(force) {
    if (dependentRestoreActive && !force) {
      restoreFinishRequested = true;
      suppressSaveUntil = Date.now() + 1500;
      return;
    }

    if (restoreFinishFallbackId) {
      window.clearTimeout(restoreFinishFallbackId);
      restoreFinishFallbackId = null;
    }

    if (force) {
      finishRestoreCycle();
      return;
    }

    restoreFinishRequested = false;
    suppressDisconnectUntil = Date.now() + restoreCompletionHoldMs + restoreGraceMs;
    restoreFinishFallbackId = window.setTimeout(function() {
      finishRestoreCycle();
    }, restoreCompletionHoldMs);
  }

  function scheduleRestoreFinishFallback() {
    if (restoreFinishFallbackId) window.clearTimeout(restoreFinishFallbackId);
    restoreFinishFallbackId = window.setTimeout(function() {
      requestRestoreFinish(true);
    }, Math.max(25000, restoreCompletionHoldMs + 10000));
  }

  function clearSavedState() {
    clearRestoreSendTimer();
    safeSessionRemove(stateKey);
    safeLocalRemove(lastStateKey);
    safeSessionRemove(lastTabKey);
    safeSessionRemove(restoreIntentKey);
    safeSessionRemove(reconnectingKey);
    safeSessionRemove(restoreCompleteKey);
    safeSessionSet(skipRestoreKey, String(Date.now()));
    restorePending = false;
    toggleNativeDisconnectUi(false);
  }

  function shouldRestoreState() {
    if (safeSessionGet(skipRestoreKey)) {
      safeSessionRemove(skipRestoreKey);
      return false;
    }
    return !!safeSessionGet(restoreIntentKey);
  }

  function sendRestoreState(stage) {
    if (!shinyReadyForRestore()) return false;
    var state = pendingRestoreState || loadState(true);
    if (!state) return false;
    try {
      window.Shiny.setInputValue("ibpl_restore_state", {
        stage: stage || "full",
        sentAt: Date.now(),
        values: state.values
      }, { priority: "event" });
      restoreSent = true;
    } catch (e) {
      restoreSent = false;
      return false;
    }
    if (stage === "final" && !finalBrowserApplySent) {
      finalBrowserApplySent = true;
      window.setTimeout(function() {
        applyRestoreValues(state.values, false, false);
      }, 1200);
    }
    return true;
  }

  function requestRestore() {
    if (restoreSent || !shouldRestoreState()) return;
    pendingRestoreState = loadState(true);
    if (!pendingRestoreState) {
      safeSessionRemove(restoreIntentKey);
      safeSessionRemove(reconnectingKey);
      restorePending = false;
      toggleNativeDisconnectUi(false);
      return;
    }
    toggleNativeDisconnectUi(true);
    finalBrowserApplySent = false;
    restoreFinishRequested = false;
    restoreSendAttempt = 0;
    dependentRestoreActive = false;
    clearDelayedPlayerRestores();
    clearRestoreSendTimer();
    restorePending = true;
    activateRestoreTab(pendingRestoreState);
    scheduleRestoreSend(700);
  }

  function formatSeconds(ms) {
    return Math.max(0, Math.ceil(ms / 1000));
  }

  function hideNativeDisconnectNodes() {
    [
      "#shiny-disconnected-overlay",
      ".shiny-disconnected-overlay",
      "#shiny-disconnected-dialog",
      ".shiny-disconnected-dialog",
      "#shiny-notification-reconnect",
      "#shiny-reconnect-dialog",
      ".shiny-reconnect-dialog",
      ".reconnect-dialog"
    ].forEach(function(selector) {
      var nodes = document.querySelectorAll(selector);
      for (var i = 0; i < nodes.length; i++) {
        nodes[i].style.setProperty("display", "none", "important");
        nodes[i].style.setProperty("pointer-events", "none", "important");
      }
    });
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

  function clearIdleOverlay() {
    idleExpired = false;
    lastActivity = Date.now();
    toggleNativeDisconnectUi(false);
    var overlay = document.getElementById("ibpl-idle-overlay");
    if (!overlay) return;
    overlay.classList.remove("visible", "expired");
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
        '<p class="idle-copy" id="ibpl-idle-copy">This session will pause soon to keep the app responsive.</p>' +
        '<div class="idle-countdown" id="ibpl-idle-countdown"></div>' +
        '<div class="idle-actions">' +
          '<button type="button" class="btn btn-primary idle-keep-btn" id="ibpl-idle-keep">Keep working</button>' +
          '<button type="button" class="btn btn-primary idle-reconnect-btn" id="ibpl-idle-reconnect">Reconnect and restore</button>' +
          '<button type="button" class="btn btn-outline-secondary idle-fresh-btn" id="ibpl-idle-fresh">Start fresh</button>' +
        '</div>' +
      '</div>';
    document.body.appendChild(overlay);

    var keepBtn = document.getElementById("ibpl-idle-keep");
    var reconnectBtn = document.getElementById("ibpl-idle-reconnect");
    var freshBtn = document.getElementById("ibpl-idle-fresh");
    if (keepBtn) {
      keepBtn.addEventListener("click", function() {
        markActivity(true);
        keepBtn.blur();
      });
    }
    if (reconnectBtn) {
      reconnectBtn.addEventListener("click", function() {
        saveState(true, true);
        markRestoreIntent();
        pageUnloading = true;
        toggleNativeDisconnectUi(true);
        window.location.reload();
      });
    }
    if (freshBtn) {
      freshBtn.addEventListener("click", function() {
        clearSavedState();
        window.location.reload();
      });
    }
    return overlay;
  }

  function setOverlayState(state, secondsLeft) {
    var overlay = ensureIdleOverlay();
    var title = document.getElementById("ibpl-idle-title");
    var copy = document.getElementById("ibpl-idle-copy");
    var countdown = document.getElementById("ibpl-idle-countdown");
    var keepBtn = document.getElementById("ibpl-idle-keep");
    var reconnectBtn = document.getElementById("ibpl-idle-reconnect");
    var freshBtn = document.getElementById("ibpl-idle-fresh");

    overlay.classList.add("visible");
    overlay.classList.toggle("expired", state === "expired");
    toggleNativeDisconnectUi(state === "expired");

    if (state === "expired") {
      if (title) title.textContent = "Session paused";
      if (copy) copy.textContent = "Reconnect to restore your last tab and filters, or start with a clean view.";
      if (countdown) countdown.textContent = "";
      if (keepBtn) keepBtn.style.display = "none";
      if (reconnectBtn) reconnectBtn.style.display = "";
      if (freshBtn) freshBtn.style.display = "";
      return;
    }

    if (title) title.textContent = "Still working?";
    if (copy) copy.textContent = "Your tab and filters are saved locally. Keep working to prevent this session from pausing.";
    if (countdown) countdown.textContent = "Pausing in " + secondsLeft + " seconds";
    if (keepBtn) keepBtn.style.display = "";
    if (reconnectBtn) reconnectBtn.style.display = "none";
    if (freshBtn) freshBtn.style.display = "none";
  }

  function hideIdleWarning() {
    var overlay = document.getElementById("ibpl-idle-overlay");
    if (!overlay || idleExpired) return;
    overlay.classList.remove("visible");
    toggleNativeDisconnectUi(false);
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
    if (idleExpired) return;
    lastActivity = Date.now();
    hideIdleWarning();
    sendActivity(force);
  }

  function checkIdleState() {
    if (idleExpired) return;
    var idleMs = Date.now() - lastActivity;
    var remainingMs = timeoutMs - idleMs;
    if (remainingMs <= 0) {
      idleExpired = true;
      saveState(true, true);
      markRestoreIntent();
      setOverlayState("expired", 0);
      return;
    }
    if (remainingMs <= warningMs) {
      setOverlayState("warning", formatSeconds(remainingMs));
    } else {
      hideIdleWarning();
    }
  }

  function shouldSuppressDisconnectOverlay() {
    return pageUnloading ||
      restorePending ||
      restoreSent ||
      reconnectIntentActive() ||
      restoreCompletedRecently() ||
      Date.now() < suppressDisconnectUntil;
  }

  function handleDisconnected() {
    if (shouldSuppressDisconnectOverlay()) {
      toggleNativeDisconnectUi(true);
      if (restorePending) {
        restoreSent = false;
        suppressDisconnectUntil = Date.now() + restoreCompletionHoldMs + restoreGraceMs;
      }
      return;
    }

    saveState(true, true);
    markRestoreIntent();
    idleExpired = true;
    setOverlayState("expired", 0);
  }

  function patchShinyDisconnectNotifier() {
    var shinyapp = window.Shiny && window.Shiny.shinyapp;
    if (!shinyapp || shinyapp.__ibplNotifyDisconnectedPatched) return;
    var original = shinyapp.$notifyDisconnected;
    if (typeof original !== "function") return;

    shinyapp.__ibplNotifyDisconnectedPatched = true;
    shinyapp.__ibplNotifyDisconnectedOriginal = original;
    shinyapp.$notifyDisconnected = function() {
      if (shouldSuppressDisconnectOverlay()) {
        toggleNativeDisconnectUi(true);
        return;
      }
      return original.apply(this, arguments);
    };
  }

  function bindActivity() {
    window.addEventListener("beforeunload", function() {
      pageUnloading = true;
    });

    var events = ["mousemove", "mousedown", "keydown", "scroll", "touchstart", "click"];
    for (var i = 0; i < events.length; i++) {
      document.addEventListener(events[i], function() { markActivity(false); }, { passive: true });
    }
    document.addEventListener("visibilitychange", function() {
      if (document.visibilityState === "visible") markActivity(true);
    });
    document.addEventListener("change", scheduleSave, true);
    document.addEventListener("input", scheduleSave, true);
    document.addEventListener("click", function(e) {
      var tabLink = e.target.closest(".nav-link[data-value], a[data-value]");
      if (tabLink && rememberActiveTab(tabValueFromElement(tabLink))) {
        scheduleSave();
      }
    }, true);
    document.addEventListener("shown.bs.tab", function(e) {
      if (e && rememberActiveTab(tabValueFromElement(e.target))) {
        scheduleSave();
      }
    }, true);
    if (window.jQuery) {
      window.jQuery(document).on("shiny:inputchanged", function(evt) {
        if (!evt) return;
        if ((evt.name === "main_tabs" || evt.name === "main_tabs:shiny.tabinput") && rememberActiveTab(evt.value)) {
          scheduleSave();
          return;
        }
        if (persistIds.indexOf(evt.name) !== -1) scheduleSave();
      });
      window.jQuery(document).on("shiny:connected", function() {
        pageUnloading = false;
        patchShinyDisconnectNotifier();
        if (restorePending) {
          toggleNativeDisconnectUi(true);
        } else {
          clearIdleOverlay();
        }
        saveState(false, false);
        window.setTimeout(requestRestore, 900);
      });
      window.jQuery(document).on("shiny:disconnected", function() {
        handleDisconnected();
      });
    } else {
      document.addEventListener("shiny:connected", function() {
        pageUnloading = false;
        patchShinyDisconnectNotifier();
        if (restorePending) {
          toggleNativeDisconnectUi(true);
        } else {
          clearIdleOverlay();
        }
        saveState(false, false);
        window.setTimeout(requestRestore, 900);
      });
      document.addEventListener("shiny:disconnected", function() {
        handleDisconnected();
      });
    }
    patchShinyDisconnectNotifier();
    markActivity(true);
    if (restorePending) toggleNativeDisconnectUi(true);
    saveState(false, false);
    if (timerId) window.clearInterval(timerId);
    timerId = window.setInterval(checkIdleState, 1000);
  }

  window.ibplClearSavedSession = function() {
    clearSavedState();
  };

  window.ibplSaveSessionState = function(force) {
    if (restorePending) return;
    saveState(force, false);
  };

  window.ibplIsRestorePending = function() {
    return !!restorePending;
  };

  window.ibplFinishRestoreCycle = function() {
    requestRestoreFinish(false);
  };

  window.ibplRestoreSavedSession = function() {
    saveState(true, true);
    markRestoreIntent();
    restoreSent = false;
    requestRestore();
  };

  window.ibplDebugSavedSession = function() {
    var state = loadState(true);
    var values = state && state.values ? state.values : {};
    return {
      activeTab: activeTabValue(),
      lastKnownTab: lastKnownTab,
      storedTab: safeSessionGet(lastTabKey),
      restorePending: restorePending,
      savedTab: values.main_tabs || null,
      savedKeys: Object.keys(values),
      savedValueCount: Object.keys(values).length,
      state: state
    };
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bindActivity);
  } else {
    bindActivity();
  }
})();

(function() {
  function registerRestoreHandler() {
    if (!window.Shiny || typeof window.Shiny.addCustomMessageHandler !== "function") return false;
    if (window.__ibplRestoreAppliedHandlerRegistered) return true;
    window.__ibplRestoreAppliedHandlerRegistered = true;
    window.Shiny.addCustomMessageHandler("ibpl_restore_applied", function() {
      if (typeof window.ibplFinishRestoreCycle === "function") {
        window.setTimeout(window.ibplFinishRestoreCycle, 1500);
      }
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
            if (typeof window.ibplClearSavedSession === "function") window.ibplClearSavedSession();
            window.location.reload();
          });
        }
      }
      notice.classList.add("visible");
      window.setTimeout(function() { notice.classList.remove("visible"); }, 6000);
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", registerRestoreHandler);
  } else {
    registerRestoreHandler();
  }
  document.addEventListener("shiny:connected", registerRestoreHandler);
})();

(function() {
  var CFG = [
    { tab: "onoff", inputId: "onoff_view_mode", items: ["Summary", "Four Factors"], def: "Summary" },
    { tab: "lineup_data", inputId: "ld_view_mode", items: ["Summary", "Four Factors"], def: "Summary" },
    { tab: "team_ratings", inputId: "tr_view_mode", items: ["Summary", "Four Factors", "Traditional"], def: "Summary" },
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
