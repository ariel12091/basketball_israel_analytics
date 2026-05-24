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
  var minIntervalMs = 15000;

  function sendActivity() {
    var now = Date.now();
    if ((now - lastSent) < minIntervalMs) return;
    lastSent = now;
    if (!window.Shiny || typeof window.Shiny.setInputValue !== "function") return;
    window.Shiny.setInputValue("idle_activity_ts", now, { priority: "event" });
  }

  function bindActivity() {
    var events = ["mousemove", "mousedown", "keydown", "scroll", "touchstart", "click"];
    for (var i = 0; i < events.length; i++) {
      document.addEventListener(events[i], sendActivity, { passive: true });
    }
    document.addEventListener("visibilitychange", function() {
      if (document.visibilityState === "visible") sendActivity();
    });
    sendActivity();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bindActivity);
  } else {
    bindActivity();
  }
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
