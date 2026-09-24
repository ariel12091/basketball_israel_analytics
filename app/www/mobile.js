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
  // Touch screens up to 1399px -- every iPad, including a 12.9" one in
  // landscape at 1366px -- also get the collapsed menu: the desktop view
  // menus open on :hover, which a finger cannot do. The filter sidebar
  // stacks by width alone, because a landscape iPad has room for it.
  var collapsedNavQuery = window.matchMedia && window.matchMedia(
    "(max-width: 991.98px), (hover: none) and (pointer: coarse) and (max-width: 1399.98px)"
  );
  var stackedFiltersQuery = window.matchMedia && window.matchMedia("(max-width: 991.98px)");

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
    var on = !!(collapsedNavQuery && collapsedNavQuery.matches);
    document.body.classList.toggle("ibpl-collapsed-nav", on);
    document.body.classList.toggle(
      "ibpl-stacked-filters", !!(stackedFiltersQuery && stackedFiltersQuery.matches)
    );
    relocateCluster(on);
  }

  // The collapsed menu fills most of an iPad screen, so leave it open only
  // while the user still has a choice to make. Tapping a tab keeps it open to
  // show that tab's views; picking a view (or a tab with no views, i.e. Home)
  // is the last step, so close it. Capture phase: app.js stops propagation
  // on .thm-item.
  function closeCollapsedNav() {
    var collapse = document.querySelector(".navbar-collapse.show");
    if (!collapse) return;
    if (window.bootstrap && window.bootstrap.Collapse) {
      window.bootstrap.Collapse.getOrCreateInstance(collapse, { toggle: false }).hide();
    } else if (window.jQuery) {
      window.jQuery(collapse).collapse("hide");
    }
  }

  document.addEventListener("click", function (e) {
    if (!document.body.classList.contains("ibpl-collapsed-nav")) return;
    var t = e.target;
    if (!t || !t.closest) return;
    var done = t.closest(".navbar-collapse .tab-hover-menu .thm-item");
    if (!done) {
      var link = t.closest("#main_tabs .nav-link");
      if (link && !link.closest(".tab-has-dropdown")) done = link;
    }
    if (done) closeCollapsedNav();
  }, true);

  document.addEventListener("ibpl:mobilechange", sync);
  [collapsedNavQuery, stackedFiltersQuery].forEach(function (q) {
    if (!q) return;
    if (q.addEventListener) {
      q.addEventListener("change", sync);
    } else if (q.addListener) {
      q.addListener(sync);
    }
  });
  if (window.jQuery) window.jQuery(document).on("shown.bs.tab shiny:value", sync);
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", sync);
  } else {
    sync();
  }
})();

/* ---- Gameflow: show the full-size ribbon in the page -------------------- */
(function () {
  function panelFor(link) {
    var inputId = link && link.dataset.inputId || "gl_ribbon_click";
    var prefix = inputId.replace(/_ribbon_click$/, "");
    if (prefix !== "gl" && prefix !== "eurogl") return null;
    return document.getElementById(prefix + "_ribbon_inline_panel");
  }

  /* Keep the names and margin scale visible in both the quarter cards and
     the optional full timeline. The copy carries no data-clip or tabindex;
     app.js reaches it through svg.ibplPin and pin.ibplSvg. */
  var SVG_NS = "http://www.w3.org/2000/svg";
  var PIN_SELECTOR = ".ibpl-ribbon-name, .ibpl-ribbon-pm, .ibpl-ribbon-team, " +
    ".ibpl-ribbon-scale-label, .ibpl-ribbon-zero-label";

  function pinGutter(svg) {
    var scroller = svg.closest(".ibpl-ribbon-inline-scroll");
    var gutter = Number(svg.dataset.detailX);
    var vb = svg.viewBox && svg.viewBox.baseVal;
    if (!scroller || !isFinite(gutter) || gutter <= 0 || !vb) return;
    var old = scroller.querySelector(".ibpl-ribbon-pin");
    if (old) old.parentNode.removeChild(old);

    var height = svg.getBoundingClientRect().height;
    // A quarter card draws the chart scaled; the pin must share that scale
    // or its names drift off their rows. The full timeline is 1:1.
    var scale = height / vb.height;
    var pin = document.createElementNS(SVG_NS, "svg");
    pin.setAttribute("class", "ibpl-ribbon-pin");
    pin.setAttribute("aria-hidden", "true");
    pin.setAttribute("viewBox", "0 0 " + gutter + " " + vb.height);
    pin.style.width = gutter * scale + "px";
    pin.style.height = height + "px";
    pin.style.marginBottom = -height + "px";

    var bg = document.createElementNS(SVG_NS, "rect");
    bg.setAttribute("class", "ibpl-ribbon-pin-bg");
    bg.setAttribute("width", gutter);
    bg.setAttribute("height", vb.height);
    pin.appendChild(bg);

    var texts = svg.querySelectorAll(PIN_SELECTOR);
    var teams = [];
    for (var i = 0; i < texts.length; i++) {
      var copy = texts[i].cloneNode(true);
      if (copy.hasAttribute("data-clip")) {
        copy.setAttribute("data-pin-clip", copy.getAttribute("data-clip"));
        copy.removeAttribute("data-clip");
      }
      copy.removeAttribute("tabindex");
      copy.removeAttribute("aria-label");
      pin.appendChild(copy);
      if (copy.classList.contains("ibpl-ribbon-team")) teams.push(copy);
    }

    scroller.insertBefore(pin, svg);
    // A team name is wider than the gutter and overflows onto the chart;
    // back it so period labels swiping underneath do not show through.
    teams.forEach(function (t) {
      var bb = t.getBBox();
      var back = document.createElementNS(SVG_NS, "rect");
      back.setAttribute("class", "ibpl-ribbon-pin-bg");
      back.setAttribute("x", 0);
      back.setAttribute("y", bb.y - 2);
      back.setAttribute("width", Math.max(gutter, bb.width + 10));
      back.setAttribute("height", bb.height + 4);
      pin.insertBefore(back, t);
    });

    svg.classList.add("has-pin");
    svg.ibplPin = pin;
    pin.ibplSvg = svg;
  }

  function quarterLabel(index) {
    return index < 4 ? "Q" + (index + 1) : "OT" + (index - 3);
  }

  // A regulation quarter fills the card; wider screens stop growing here.
  var MAX_QUARTER_SCALE = 1.25;

  function fitQuarter(svg) {
    var frame = svg.closest(".ibpl-ribbon-quarter-frame");
    var bounds = (svg.dataset.periodBounds || "").split(",").map(Number);
    var index = Number(svg.dataset.quarterIndex);
    var gutter = Number(svg.dataset.detailX);
    var fullWidth = Number(svg.dataset.fullWidth);
    var height = Number(svg.dataset.baseHeight);
    if (!frame || !bounds.length || !isFinite(index) || !isFinite(gutter) ||
        !isFinite(fullWidth) || !isFinite(height)) return;
    var width = frame.clientWidth;
    if (width <= gutter + 20) return;
    var start = index ? bounds[index - 1] : 0;
    var perSecond = (fullWidth - gutter) / bounds[bounds.length - 1];
    var span = (bounds[index] - start) * perSecond;
    // One uniform scale for every card of the game, taken from Q1. A
    // per-card fit stretched text sideways (1.7x on a 5-minute OT) and made
    // a bar's width mean different minutes in different cards; now an OT
    // card is simply narrower.
    var scale = Math.min(width / (gutter + bounds[0] * perSecond), MAX_QUARTER_SCALE);
    svg.setAttribute("viewBox", [start * perSecond, 0, gutter + span, height].join(" "));
    svg.setAttribute("width", (gutter + span) * scale);
    svg.setAttribute("height", height * scale);
    svg.style.width = (gutter + span) * scale + "px";
    svg.style.height = height * scale + "px";
    pinGutter(svg);
  }

  function shiftY(element, dy) {
    if (!dy) return;
    if (element.tagName === "line") {
      element.setAttribute("y1", Number(element.getAttribute("y1")) - dy);
      element.setAttribute("y2", Number(element.getAttribute("y2")) - dy);
    } else if (element.tagName === "path") {
      // Only the margin curves. Their clip paths live in the curve's own
      // user space, so translating the path keeps each player's minutes
      // clipped correctly.
      element.setAttribute("transform", "translate(0 " + -dy + ")");
    } else if (element.hasAttribute("y")) {
      element.setAttribute("y", Number(element.getAttribute("y")) - dy);
    }
  }

  // Drop the rows of players who sat out this period. Rows are one per
  // player at a fixed pitch; everything below a removed row moves up by
  // rewriting y values, never with a transform: app.js strips transforms
  // from the shift layers on every deselect and hit-tests raw rect y.
  function collapseRows(svg, source) {
    var shifts = { own: 0, opp: 0 };
    var rowShift = Object.create(null);
    ["own", "opp"].forEach(function (side) {
      var yOf = function (lane) { return Number(lane.querySelector("rect").getAttribute("y")); };
      var unique = function (values) {
        return Array.from(new Set(values)).sort(function (a, b) { return a - b; });
      };
      var all = unique(Array.from(source.querySelectorAll(".ibpl-ribbon-lane.is-" + side), yOf));
      var kept = svg.querySelectorAll(".ibpl-ribbon-lane.is-" + side);
      var used = unique(Array.from(kept, yOf));
      if (all.length < 2) return;
      var pitch = (all[all.length - 1] - all[0]) / (all.length - 1);
      kept.forEach(function (lane) {
        rowShift[lane.dataset.clip] = yOf(lane) - all[used.indexOf(yOf(lane))];
      });
      shifts[side] = (all.length - used.length) * pitch;
    });

    svg.querySelectorAll(".ibpl-ribbon-lane rect, .ibpl-ribbon-lane .ibpl-ribbon-num, " +
                         ".ibpl-ribbon-name").forEach(function (element) {
      var owner = element.closest(".ibpl-ribbon-lane") || element;
      shiftY(element, rowShift[owner.dataset.clip] || 0);
    });
    svg.querySelectorAll(".ibpl-ribbon-margin-layer > *, .ibpl-ribbon-opp-layer text, " +
                         ".ibpl-ribbon-opp-layer rect").forEach(function (element) {
      shiftY(element, shifts.own);
    });
    var below = shifts.own + shifts.opp;
    svg.querySelectorAll(".ibpl-ribbon-bottom-layer text").forEach(function (element) {
      shiftY(element, below);
    });
    // Gridlines carry their extent in y2, the alternating bands in height.
    svg.querySelectorAll("[data-base-y2]").forEach(function (mark) {
      mark.dataset.baseY2 = Number(mark.dataset.baseY2) - below;
      if (mark.tagName === "rect") {
        mark.setAttribute("height",
          Math.max(Number(mark.dataset.baseY2) - Number(mark.getAttribute("y")), 0));
      } else {
        mark.setAttribute("y2", mark.dataset.baseY2);
      }
    });
    svg.dataset.baseHeight = Number(svg.dataset.baseHeight) - below;
    svg.dataset.ownDetailY = Number(svg.dataset.ownDetailY) - shifts.own;
    svg.dataset.oppDetailY = Number(svg.dataset.oppDetailY) - below;
  }

  // A card shows each bar as it was in this period. The server's detail for
  // that period (data-period-detail, from ribbon_period_details) replaces the
  // whole stint's window, +/-, points, lineup segments and accessible name,
  // so the tap detail card and its lineups describe this quarter only. The
  // rect is cut at the period edges too: app.js maps segment times onto the
  // rect through data-start/data-end, which now hold the period window.
  // Every bar with room gets a number; blank still means only "too narrow",
  // the same width rule as the server's ribbon_number_fits().
  function clipBarsToPeriod(svg, index, plotStart, plotEnd) {
    svg.querySelectorAll(".ibpl-ribbon-lane").forEach(function (lane) {
      var rect = lane.querySelector("rect");
      var details = null;
      try { details = JSON.parse(lane.dataset.periodDetail || "null"); } catch (e) {}
      var detail = details && details[index];
      if (!rect || !detail) return;
      var x = Number(rect.getAttribute("x"));
      var left = Math.max(x, plotStart);
      // ribbon_geometry()'s 0.75-unit floor keeps a seconds-long stint
      // visible; clipping at a period edge must not undercut it.
      var width = Math.max(Math.min(x + Number(rect.getAttribute("width")), plotEnd) - left, 0.75);
      rect.setAttribute("x", left);
      rect.setAttribute("width", width);
      lane.dataset.start = detail.start;
      lane.dataset.end = detail.end;
      lane.dataset.window = detail.window;
      lane.dataset.pm = detail.pm;
      lane.dataset.pf = detail.pf;
      lane.dataset.pa = detail.pa;
      lane.dataset.segments = detail.segments;
      lane.setAttribute("aria-label", detail.label);
      lane.removeAttribute("data-period-detail");
      var stale = lane.querySelector(".ibpl-ribbon-num");
      if (stale) stale.remove();
      var label = detail.pm;
      if (!label || width < label.length * 0.6 * 9 + 6) return;
      var num = document.createElementNS(SVG_NS, "text");
      num.setAttribute("class", "ibpl-ribbon-num");
      num.setAttribute("x", left + width / 2);
      num.setAttribute("y", Number(rect.getAttribute("y")) + Number(rect.getAttribute("height")) / 2 + 3);
      num.setAttribute("text-anchor", "middle");
      num.textContent = label;
      rect.parentNode.insertBefore(num, rect.nextSibling);
    });
  }

  function quarterSvg(source, index, bounds) {
    var svg = source.cloneNode(true);
    var start = index ? bounds[index - 1] : 0;
    var end = bounds[index];
    var suffix = "-quarter-" + (index + 1);
    var ids = Object.create(null);
    var gutter = Number(source.dataset.detailX);
    var perSecond = (source.viewBox.baseVal.width - gutter) / bounds[bounds.length - 1];
    svg.dataset.quarterIndex = index;
    svg.dataset.fullWidth = source.viewBox.baseVal.width;
    svg.setAttribute("aria-label", source.getAttribute("aria-label") + ", " + quarterLabel(index));

    // Each card is a view onto the same game, but hidden stints must not be
    // focusable, and duplicated clip IDs would resolve to the wrong card.
    svg.querySelectorAll(".ibpl-ribbon-lane").forEach(function (lane) {
      if (Number(lane.dataset.end) <= start || Number(lane.dataset.start) >= end) lane.remove();
    });
    var visibleClips = new Set(Array.from(svg.querySelectorAll(".ibpl-ribbon-lane"),
      function (lane) { return lane.dataset.clip; }));
    svg.querySelectorAll(".ibpl-ribbon-name, .ibpl-ribbon-pm").forEach(function (label) {
      if (!visibleClips.has(label.dataset.clip) || label.classList.contains("ibpl-ribbon-pm")) {
        label.remove();
      } else {
        // A quarter card does not show the gutter's full-game +/- total.
        label.setAttribute("x", gutter - 6);
      }
    });
    // The card heading names the period. The top marker row shares the team
    // name's header row, and in a narrow OT card the pinned name's backing
    // cut it into a stray sliver; the bottom row still marks the period start.
    svg.querySelectorAll(".ibpl-ribbon-top-layer .ibpl-ribbon-period-label").forEach(function (label) {
      label.remove();
    });
    collapseRows(svg, source);
    clipBarsToPeriod(svg, index, gutter + start * perSecond, gutter + end * perSecond);
    svg.querySelectorAll("clipPath[id]").forEach(function (clip) {
      if (!visibleClips.has(clip.id)) clip.remove();
      else { ids[clip.id] = clip.id + suffix; clip.id = ids[clip.id]; }
    });
    svg.querySelectorAll("[data-clip]").forEach(function (element) {
      if (ids[element.dataset.clip]) element.dataset.clip = ids[element.dataset.clip];
    });
    return svg;
  }

  function showQuarterCards(result) {
    var cards = result.querySelector(".ibpl-ribbon-quarters");
    var full = result.querySelector(".ibpl-ribbon-full-timeline");
    var toggle = result.querySelector(".ibpl-ribbon-full-toggle");
    if (!cards || !full || !toggle) return;
    cards.hidden = false;
    full.hidden = true;
    toggle.textContent = "View full timeline";
    toggle.setAttribute("aria-expanded", "false");
    cards.querySelectorAll("svg.ibpl-ribbon").forEach(fitQuarter);
  }

  function buildQuarterCards(source) {
    var scroller = source.closest(".ibpl-ribbon-inline-scroll");
    var result = source.closest(".ibpl-ribbon-inline-result");
    var bounds = (source.dataset.periodBounds || "").split(",").map(Number);
    if (!scroller || !result || bounds.length < 4 ||
        bounds.some(function (value, i) { return !isFinite(value) || value <= (i ? bounds[i - 1] : 0); })) {
      return false;
    }

    var cards = document.createElement("div");
    cards.className = "ibpl-ribbon-quarters";
    bounds.forEach(function (_end, index) {
      var card = document.createElement("section");
      card.className = "ibpl-ribbon-quarter-card";
      card.dataset.quarter = index + 1;
      var heading = document.createElement("h4");
      heading.className = "ibpl-ribbon-quarter-heading";
      heading.textContent = quarterLabel(index);
      var frame = document.createElement("div");
      frame.className = "ibpl-ribbon-inline-scroll ibpl-ribbon-quarter-frame";
      frame.setAttribute("aria-label", quarterLabel(index) + " gameflow");
      frame.appendChild(quarterSvg(source, index, bounds));
      card.appendChild(heading);
      card.appendChild(frame);
      cards.appendChild(card);
    });

    var toggle = document.createElement("button");
    toggle.type = "button";
    toggle.className = "ibpl-ribbon-full-toggle";
    toggle.textContent = "View full timeline";
    toggle.setAttribute("aria-expanded", "false");
    var full = document.createElement("div");
    full.className = "ibpl-ribbon-full-timeline";
    full.hidden = true;
    var shell = document.createElement("div");
    shell.className = "ibpl-ribbon-mobile-charts";
    scroller.parentNode.insertBefore(toggle, scroller);
    scroller.parentNode.insertBefore(shell, scroller);
    shell.appendChild(cards);
    shell.appendChild(full);
    full.appendChild(scroller);
    cards.querySelectorAll("svg.ibpl-ribbon").forEach(fitQuarter);
    var first = result.querySelector('.ibpl-ribbon-quarter-jump[data-quarter="1"]');
    if (first) first.classList.add("is-active");
    return true;
  }

  function updateQuarterNav(result, index) {
    result.querySelectorAll(".ibpl-ribbon-quarter-jump").forEach(function (button) {
      button.classList.toggle("is-active", Number(button.dataset.quarter) === index);
    });
  }

  document.addEventListener("click", function (e) {
    var jump = e.target.closest && e.target.closest(".ibpl-ribbon-quarter-jump");
    var toggle = e.target.closest && e.target.closest(".ibpl-ribbon-full-toggle");
    var control = jump || toggle;
    if (!control) return;
    var result = control.closest(".ibpl-ribbon-inline-result");
    if (!result) return;
    if (jump) {
      showQuarterCards(result);
      var index = Number(jump.dataset.quarter);
      var card = result.querySelector('.ibpl-ribbon-quarter-card[data-quarter="' + index + '"]');
      if (card) card.scrollIntoView({ block: "start", behavior: "smooth" });
      updateQuarterNav(result, index);
      return;
    }
    var full = result.querySelector(".ibpl-ribbon-full-timeline");
    var cards = result.querySelector(".ibpl-ribbon-quarters");
    if (!full || !cards) return;
    if (!full.hidden) { showQuarterCards(result); return; }
    cards.hidden = true;
    full.hidden = false;
    toggle.textContent = "Show quarter cards";
    toggle.setAttribute("aria-expanded", "true");
    var svg = full.querySelector("svg.ibpl-ribbon");
    if (svg) window.requestAnimationFrame(function () { pinGutter(svg); });
  });

  // A phone fires resize whenever its address bar shows or hides; only a
  // width change moves the cards.
  var fittedWidth = window.innerWidth;
  window.addEventListener("resize", function () {
    if (window.innerWidth === fittedWidth) return;
    fittedWidth = window.innerWidth;
    document.querySelectorAll(".ibpl-ribbon-quarter-card svg.ibpl-ribbon").forEach(fitQuarter);
  });

  // The highlighted jump button follows the card being read, not only the
  // last button pressed. A card counts as current once its top passes this
  // far down the screen (its scroll-margin-top is 68px).
  var CURRENT_CARD_TOP = 120;
  var navQueued = false;
  window.addEventListener("scroll", function () {
    if (navQueued) return;
    navQueued = true;
    window.requestAnimationFrame(function () {
      navQueued = false;
      document.querySelectorAll(".ibpl-ribbon-quarters:not([hidden])").forEach(function (cards) {
        var result = cards.closest(".ibpl-ribbon-inline-result");
        if (!result || !cards.offsetParent) return;
        var current = 1;
        cards.querySelectorAll(".ibpl-ribbon-quarter-card").forEach(function (card) {
          if (card.getBoundingClientRect().top <= CURRENT_CARD_TOP) current = Number(card.dataset.quarter);
        });
        updateQuarterNav(result, current);
      });
    });
  }, { passive: true });

  document.addEventListener("click", function (e) {
    if (!document.body.classList.contains("ibpl-mobile")) return;
    var close = e.target.closest && e.target.closest(".ibpl-ribbon-inline-close");
    if (close) {
      var openPanel = close.closest(".ibpl-ribbon-inline-panel");
      if (openPanel) openPanel.hidden = true;
      return;
    }
    var link = e.target.closest && e.target.closest(".ribbon-link");
    if (!link || !window.Shiny) return;
    var panel = panelFor(link);
    if (!panel) return;
    panel.dataset.gameId = link.dataset.gameId;
    panel.hidden = false;
    panel.classList.add("is-loading");
    // Shiny suspends an output it believes is hidden and never sends its
    // value. The panel starts [hidden], and Shiny only re-checks visibility
    // on a "shown" event, so without this the first gameflow opened stayed
    // on "Loading gameflow..." forever (reproduced at 390px, 2026-09-13:
    // .clientdata_output_gl_ribbon_inline_hidden stayed true).
    if (window.jQuery) window.jQuery(panel).trigger("shown");
    window.requestAnimationFrame(function () {
      panel.scrollIntoView({ block: "start", behavior: "smooth" });
    });
  }, true);

  if (window.jQuery) {
    window.jQuery(document).on("shiny:value", function (e) {
      var id = e.target && e.target.id || "";
      if (id !== "gl_ribbon_inline" && id !== "eurogl_ribbon_inline") return;
      var panel = document.getElementById(id + "_panel");
      if (panel) window.requestAnimationFrame(function () {
        var result = panel.querySelector(".ibpl-ribbon-inline-result");
        if (result && result.dataset.gameId === panel.dataset.gameId) {
          panel.classList.remove("is-loading");
          var svg = result.querySelector("svg.ibpl-ribbon.is-compact");
          if (svg) window.requestAnimationFrame(function () {
            if (!buildQuarterCards(svg)) pinGutter(svg);
          });
        }
      });
    });
  }

  document.addEventListener("ibpl:mobilechange", function (e) {
    if (e.detail && e.detail.mobile) return;
    var panels = document.querySelectorAll(".ibpl-ribbon-inline-panel");
    for (var i = 0; i < panels.length; i++) panels[i].hidden = true;
  });
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

/* ---- Column/label explanation: inline strip, never a popup -------------
   R6 (2026-09-13 hardening) replaces the bottom sheet entirely -- "no
   popups on mobile" was already the rule everywhere else in this file (R2,
   R3); the sheet was the one remaining exception, a fixed-position overlay
   with a backdrop. Both taps now push a small strip into the page flow
   instead, dismissed by tapping the close button or (for the header dot)
   the same dot again.

   This also carries the fix for the header info-dot's own hit area: it
   rendered at 14x14 against this file's own --ibpl-m-tap: 44px minimum, so
   a thumb reliably missed it, landed on the th instead, and sorted the
   column with no explanation shown -- see .ibpl-m-th-info in mobile.css.

   Two tooltip mechanisms, two selectors:
   - th[title]       native title, written by HEADER_TOOLTIP_JS (global.R:212)
   - [data-tooltip]  CSS ::after on :hover, written by tt() (global.R:209)
   The stat-filter popover (helpers.R:546) is a third, unrelated inline
   component (see "Stat filter add: inline, not a sheet" below) -- it never
   routed through this one.
   --------------------------------------------------------------------- */
(function () {
  // Phones, and any touch-primary screen at any width: an iPad has no
  // hover, so th[title] and the [data-tooltip] bubble are unreachable there
  // exactly as on a phone. A mouse keeps the hover tooltips at every width.
  var TIPS_CLASS = "ibpl-touch-tips";
  var tipsQuery = window.matchMedia && window.matchMedia(window.IBPL_MOBILE_MQ);
  var touchQuery = window.matchMedia && window.matchMedia("(hover: none) and (pointer: coarse)");
  var tipsOn = null;

  function syncTips() {
    var on = !!((tipsQuery && tipsQuery.matches) || (touchQuery && touchQuery.matches));
    if (on === tipsOn) return;
    tipsOn = on;
    document.body.classList.toggle(TIPS_CLASS, on);
    if (on) {
      addInfoMarks();
    } else {
      removeInfoMarks();
      removeLabelStrips();
    }
  }

  function buildStrip(text, onClose) {
    var strip = document.createElement("div");
    strip.className = "ibpl-m-strip";
    var textEl = document.createElement("span");
    textEl.className = "ibpl-m-strip-text";
    textEl.textContent = text;
    var closeBtn = document.createElement("button");
    closeBtn.type = "button";
    closeBtn.className = "ibpl-m-strip-close";
    closeBtn.setAttribute("aria-label", "Close explanation");
    closeBtn.textContent = "✕";
    closeBtn.addEventListener("click", onClose);
    strip.appendChild(textEl);
    strip.appendChild(closeBtn);
    return strip;
  }

  // ---- Table header strip: one per table, content swapped per column ----
  // "Immediately above the table, outside .dataTables_scrollBody" -- the
  // scroll body is the part that scrolls horizontally (scrollX: true on
  // every datatable() call), so a strip inside it would slide out of view
  // together with the table instead of staying put. .dataTables_scrollHead
  // sits above .dataTables_scrollBody as a sibling inside
  // .dataTables_wrapper, so anchor on that (falling back to the bare
  // <table> for the rare dom: "t" case with no scroll split at all, e.g.
  // Compare's cmp_table) -- the strip lands as a wrapper-level sibling,
  // never a scrollBody child, either way.
  function headerStripFor(th) {
    var wrapper = th.closest(".dataTables_wrapper");
    if (!wrapper) return null;
    if (wrapper.ibplThStrip) return wrapper.ibplThStrip;

    var strip = buildStrip("", function () { closeHeaderStrip(wrapper); });
    strip.className += " ibpl-m-th-strip";
    wrapper.ibplThStrip = strip;

    var scrollHead = wrapper.querySelector(".dataTables_scrollHead");
    var anchor = scrollHead || wrapper.querySelector("table.dataTable");
    if (anchor && anchor.parentNode) {
      anchor.parentNode.insertBefore(strip, anchor);
    } else {
      wrapper.insertBefore(strip, wrapper.firstChild);
    }
    return strip;
  }

  function closeHeaderStrip(wrapper) {
    var strip = wrapper && wrapper.ibplThStrip;
    if (!strip) return;
    strip.classList.remove("ibpl-m-strip-open");
    strip.ibplLabel = null;
  }

  function toggleHeaderStrip(th, label, text) {
    var strip = headerStripFor(th);
    if (!strip) return;
    if (strip.ibplLabel === label && strip.classList.contains("ibpl-m-strip-open")) {
      // Tapping the SAME dot again dismisses it.
      closeHeaderStrip(th.closest(".dataTables_wrapper"));
      return;
    }
    strip.querySelector(".ibpl-m-strip-text").textContent = label + " — " + text;
    strip.ibplLabel = label;
    strip.classList.add("ibpl-m-strip-open");
  }

  function removeHeaderStrips() {
    var wrappers = document.querySelectorAll(".dataTables_wrapper");
    for (var i = 0; i < wrappers.length; i++) {
      var strip = wrappers[i].ibplThStrip;
      if (strip && strip.parentNode) strip.parentNode.removeChild(strip);
      wrappers[i].ibplThStrip = null;
    }
  }

  // ---- Sidebar label strip: one per label, independent of the others ----
  // "The filter panel is already inline now" (R2), so this only needs to
  // insert a sibling right after the tapped label -- no relocation, no
  // sheet, nothing to restore on desktop.
  function toggleLabelStrip(label, text) {
    if (label.ibplStrip && label.ibplStrip.parentNode) {
      label.ibplStrip.parentNode.removeChild(label.ibplStrip);
      label.ibplStrip = null;
      return;
    }
    var strip = buildStrip(text, function () {
      if (strip.parentNode) strip.parentNode.removeChild(strip);
      label.ibplStrip = null;
    });
    strip.className += " ibpl-m-label-strip ibpl-m-strip-open";
    // Two filters side by side (a .row of col-sm-6) leave each column ~130px
    // in a landscape iPad sidebar, which wrapped the strip one word per line.
    // There the strip goes below the whole row; a phone stacks those columns
    // full width, so it keeps its place right after the label.
    var col = label.closest(".row > [class*='col-']");
    var anchor = col && col.getBoundingClientRect().width < 240 ? col.parentNode : label;
    anchor.parentNode.insertBefore(strip, anchor.nextSibling);
    label.ibplStrip = strip;
  }

  // Real bug, found live while verifying this fix: .ibpl-m-strip's CSS is
  // scoped to body.ibpl-mobile (like everything else in mobile.css), so a
  // label strip left in the DOM when the viewport crosses back over the
  // breakpoint falls back to the browser default (display: block) instead
  // of disappearing -- verified live, a strip opened on a phone-width
  // viewport was still visible, taking up real layout space, after resizing
  // to desktop. Unlike the header strip (one per table, reused/cleared on
  // every mode switch via removeHeaderStrips()), a label strip is a
  // one-off DOM node with no other owner, so it has to be swept explicitly.
  function removeLabelStrips() {
    var strips = document.querySelectorAll(".ibpl-m-label-strip");
    for (var i = 0; i < strips.length; i++) {
      if (strips[i].parentNode) strips[i].parentNode.removeChild(strips[i]);
    }
  }

  // DataTables re-renders the header on every draw, so the affordance has to
  // be re-injected on every draw too. HEADER_TOOLTIP_JS (global.R:212)
  // writes the native title attribute; this only reads it. The visible "i"
  // glyph is drawn entirely in CSS (::before on .ibpl-m-th-info) so the
  // button itself carries no text node -- th.textContent below needs no
  // stripping to recover the plain column label.
  function addInfoMarks() {
    if (!document.body.classList.contains(TIPS_CLASS)) return;
    var ths = document.querySelectorAll("table.dataTable thead th[title]");
    for (var i = 0; i < ths.length; i++) {
      if (ths[i].querySelector(".ibpl-m-th-info")) continue;
      var b = document.createElement("button");
      b.type = "button";
      b.className = "ibpl-m-th-info";
      b.setAttribute("aria-label", "What this column means");
      ths[i].appendChild(b);
    }
  }

  function removeInfoMarks() {
    var marks = document.querySelectorAll(".ibpl-m-th-info");
    for (var i = 0; i < marks.length; i++) {
      if (marks[i].parentNode) marks[i].parentNode.removeChild(marks[i]);
    }
    removeHeaderStrips();
  }

  function bind() {
    if (!window.jQuery) return;
    var $ = window.jQuery;

    $(document).on("draw.dt", addInfoMarks);
    syncTips();
    [tipsQuery, touchQuery].forEach(function (q) {
      if (!q) return;
      if (q.addEventListener) {
        q.addEventListener("change", syncTips);
      } else if (q.addListener) {
        q.addListener(syncTips);
      }
    });

    // [data-tooltip] stays bubble phase and skips preventDefault(): several
    // tt() labels wrap a real checkbox/radio <input> (e.g. ts_clutch_enabled),
    // and clicking anywhere in that <label> is what toggles it -- a browser
    // default action, decided only by whether ANY listener called
    // preventDefault() during the whole dispatch, independent of phase order.
    // Calling it here suppressed the toggle entirely (verified live: tapping
    // "Enable clutch filter" left the checkbox unchecked). Omitting it lets
    // the native toggle proceed exactly as before; the strip opens too,
    // which is a bonus, not a conflict, since nothing here owns "toggle vs.
    // explain" -- both can happen from one tap.
    //
    // isTrusted guard added for R4 (2026-09-13 rework): a view-mode choice's
    // <label> (e.g. onoff_view_mode's "Four Factors") is itself a
    // data-tooltip target ("eFG%, OREB%, TOV%, FTR breakdown"), and the
    // burger-menu row for that choice drives the real radio with a
    // synthetic radio.click() (app.js updateInput()) -- a click on the
    // <input> bubbles through its ancestor <label>, straight into this
    // delegated handler, popping the strip open on every mode switch made
    // from the menu. Verified live (reproduced with a capture logger: the
    // four expected synthetic clicks were burger-toggle, thm-item, nav-link
    // and the radio input -- none of them a real tap on the tooltip label --
    // yet the sheet opened anyway, back when this routed through the
    // sheet). A real finger tap on a tooltip-bearing label is always
    // isTrusted; app.js's own programmatic .click()/.setValue() calls never
    // are, so this excludes exactly the synthetic case without touching the
    // real one.
    //
    // e.originalEvent.isTrusted, NOT e.isTrusted (real bug, found live while
    // building the R6 inline strip): jQuery's Event object does not forward
    // isTrusted onto itself -- e.isTrusted reads back undefined for every
    // click, real tap or synthetic alike, on this jQuery/browser combination.
    // With the bare e.isTrusted check this guard silently discarded every
    // click, so the entire [data-tooltip] strip never opened for a real user
    // either; verified live with an instrumented handler (e.isTrusted:
    // undefined, e.originalEvent.isTrusted: true, for the same tap).
    $(document).on("click", "[data-tooltip]", function (e) {
      if (!document.body.classList.contains(TIPS_CLASS)) return;
      if (!e.originalEvent || !e.originalEvent.isTrusted) return;
      var tip = this.getAttribute("data-tooltip");
      if (!tip) return;
      toggleLabelStrip(this, tip);
    });

    // CAPTURE phase, not $(document).on() (bubble phase): a column header's
    // tap already means SORT, in DataTables' own click.DT listener bound
    // DIRECTLY on the th (verified: $._data(th, 'events') lists 'click').
    // That listener fires at the target phase, before a bubble-phase
    // document handler ever runs, so e.stopPropagation() there is too late
    // -- it only stops the event reaching document, after DataTables already
    // reacted. Verified live: an info-dot tap toggled the column's sort
    // order every time, in addition to opening the explanation. Capturing
    // on document intercepts before the event ever reaches the th, so
    // DataTables' listener never runs. Enlarging the hit area (see
    // .ibpl-m-th-info in mobile.css) makes this MORE important, not less --
    // the bigger target overlaps more of the sortable header. (The
    // stat-filter popover trigger shares this exact hazard with Bootstrap's
    // own popover toggle; its capture-phase handler lives with the rest of
    // that component below.)
    document.addEventListener("click", function (e) {
      if (!document.body.classList.contains(TIPS_CLASS)) return;
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
      var label = (th.textContent || "").trim();
      toggleHeaderStrip(th, label, tip);
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
   leak, and the div itself can just be moved (not cloned) inline -- moving
   rather than cloning is what keeps the selectInput / radioButtons /
   numericInput inside it on their original ids, so apply_stat_filters() and
   every server observer keep working untouched.

   Desktop must keep working exactly as before, so a moved panel is always
   restored to its original spot in <bslib-popover> before mobile mode is
   left -- restore(), below.

   The chips row that hosts the trigger re-renders wholesale on every
   Add/Remove (output$*_filter_chips is one renderUI covering the whole
   chip list, helpers.R:520 stat_filter_chips_ui()), which rebuilds a fresh
   <bslib-popover> carrying the SAME ids as whatever this component is
   currently holding inline. Detect it the same way the header/label strips
   above detect a stat-filter re-render: when shiny:value fires and the held
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
