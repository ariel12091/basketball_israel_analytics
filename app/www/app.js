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
    if (owner && owner !== league && !opts.noRedirect) {
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
        writeSelect(desiredValue());
      });
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
