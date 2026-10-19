async (page) => {
  const browser = page.context().browser();
  const config = await page.evaluate(() => Object.fromEntries(
    window.location.hash.slice(1).split(",").filter(Boolean).map(part => {
      const separator = part.indexOf("=");
      return [part.slice(0, separator), decodeURIComponent(part.slice(separator + 1))];
    })
  ));
  const appUrl = config.app_url ||
    "https://arieltaieb-basketball-israel-analytics.share.connect.posit.cloud/";
  const users = Number(config.users || 1);
  const cadenceSeconds = Number(config.cadence || 10);
  const cadenceChoices = config.cadences
    ? config.cadences.split("-").map(Number).filter(Number.isFinite)
    : [cadenceSeconds];
  const actionsPerUser = Number(config.actions || 4);
  const seed = Number(config.seed || 20260910);

  const tabs = [
    { key: "home", name: /Home$/ },
    { key: "onoff", name: /On\/Off Impact$/ },
    { key: "lineups", name: /Lineup Data$/ },
    { key: "teams", name: /Team Ratings$/ },
    { key: "games", name: /Game Logs$/ },
    { key: "players", name: /Player Stats$/ },
    { key: "compare", name: /Compare$/ }
  ];

  const rngFor = initialSeed => {
    let state = initialSeed >>> 0;
    return () => {
      state += 0x6D2B79F5;
      let value = state;
      value = Math.imul(value ^ (value >>> 15), value | 1);
      value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
      return ((value ^ (value >>> 14)) >>> 0) / 4294967296;
    };
  };
  const quantile = (values, p) => {
    if (!values.length) return null;
    const sorted = [...values].sort((a, b) => a - b);
    const index = Math.min(sorted.length - 1, Math.ceil(p * sorted.length) - 1);
    return sorted[index];
  };

  const assignmentRandom = rngFor(seed ^ 0x5f3759df);
  const cadenceAssignments = Array.from(
    { length: users }, (_, i) => cadenceChoices[i % cadenceChoices.length]
  );
  for (let i = cadenceAssignments.length - 1; i > 0; i -= 1) {
    const j = Math.floor(assignmentRandom() * (i + 1));
    [cadenceAssignments[i], cadenceAssignments[j]] = [cadenceAssignments[j], cadenceAssignments[i]];
  }

  const runUser = async userIndex => {
    const random = rngFor(seed + userIndex * 1009);
    const userCadenceSeconds = cadenceAssignments[userIndex];
    const result = {
      user: userIndex + 1,
      cadence_seconds: userCadenceSeconds,
      ok: false,
      navigation_ms: null,
      actions: [],
      http_errors: [],
      request_failures: [],
      console_errors: [],
      fatal_error: null
    };
    const context = await browser.newContext();
    const userPage = await context.newPage();

    userPage.on("response", response => {
      if (response.status() >= 400 && result.http_errors.length < 10) {
        result.http_errors.push({ status: response.status(), url: response.url() });
      }
    });
    userPage.on("requestfailed", request => {
      if (result.request_failures.length < 10) {
        result.request_failures.push({
          url: request.url(),
          error: request.failure() ? request.failure().errorText : "unknown"
        });
      }
    });
    userPage.on("console", message => {
      if (message.type() === "error" && result.console_errors.length < 10) {
        result.console_errors.push(message.text().slice(0, 500));
      }
    });

    try {
      await userPage.waitForTimeout(random() * 2000);
      const navigationStart = Date.now();
      const response = await userPage.goto(appUrl, {
        waitUntil: "domcontentloaded",
        timeout: 120000
      });
      if (!response || response.status() >= 400) {
        throw new Error(`Navigation failed with status ${response ? response.status() : "none"}`);
      }
      await userPage.waitForFunction(() => {
        const app = window.Shiny && window.Shiny.shinyapp;
        return app && app.$socket && app.$socket.readyState === 1;
      }, null, { timeout: 120000 });
      await userPage.waitForFunction(() =>
        !document.documentElement.classList.contains("shiny-busy"),
        null, { timeout: 60000 });
      result.navigation_ms = Date.now() - navigationStart;

      await userPage.evaluate(() => {
        window.__ibplLoadBusyCount = 0;
        document.addEventListener("shiny:busy", () => { window.__ibplLoadBusyCount += 1; });
      });

      let currentTab = "home";
      for (let actionIndex = 0; actionIndex < actionsPerUser; actionIndex += 1) {
        const jitterMultiplier = 0.65 + random() * 0.70;
        const thinkMs = Math.round(userCadenceSeconds * 1000 * jitterMultiplier);
        await userPage.waitForTimeout(thinkMs);

        const candidates = tabs.filter(tab => tab.key !== currentTab);
        const target = candidates[Math.floor(random() * candidates.length)];
        const busyBefore = await userPage.evaluate(() => window.__ibplLoadBusyCount || 0);
        const actionStart = Date.now();
        await userPage.getByRole("tab", { name: target.name }).click({ timeout: 30000 });

        await userPage.waitForFunction(count =>
          (window.__ibplLoadBusyCount || 0) > count,
          busyBefore, { timeout: 5000 }).catch(() => null);
        await userPage.waitForFunction(() =>
          !document.documentElement.classList.contains("shiny-busy"),
          null, { timeout: 60000 });
        await userPage.waitForTimeout(250);

        result.actions.push({
          index: actionIndex + 1,
          tab: target.key,
          think_ms: thinkMs,
          latency_ms: Date.now() - actionStart
        });
        currentTab = target.key;
      }
      result.ok = result.actions.length === actionsPerUser &&
        result.http_errors.length === 0 && result.request_failures.length === 0;
    } catch (error) {
      result.fatal_error = String(error && error.stack ? error.stack : error).slice(0, 2000);
    } finally {
      await context.close();
    }
    return result;
  };

  const startedAt = new Date().toISOString();
  const results = await Promise.all(Array.from({ length: users }, (_, i) => runUser(i)));
  const navigationTimes = results.map(x => x.navigation_ms).filter(Number.isFinite);
  const actionTimes = results.flatMap(x => x.actions.map(action => action.latency_ms));
  const httpErrorCount = results.reduce((sum, x) => sum + x.http_errors.length, 0);
  const requestFailureCount = results.reduce((sum, x) => sum + x.request_failures.length, 0);
  const consoleErrorCount = results.reduce((sum, x) => sum + x.console_errors.length, 0);

  return JSON.stringify({
    config: {
      app_url: appUrl,
      users,
      cadence_seconds: cadenceSeconds,
      cadence_choices_seconds: cadenceChoices,
      cadence_assignments_seconds: cadenceAssignments,
      actions_per_user: actionsPerUser,
      seed
    },
    started_at: startedAt,
    finished_at: new Date().toISOString(),
    summary: {
      successful_users: results.filter(x => x.ok).length,
      failed_users: results.filter(x => !x.ok).length,
      completed_actions: actionTimes.length,
      expected_actions: users * actionsPerUser,
      navigation_ms: {
        median: quantile(navigationTimes, 0.5),
        p90: quantile(navigationTimes, 0.9),
        max: quantile(navigationTimes, 1)
      },
      action_latency_ms: {
        median: quantile(actionTimes, 0.5),
        p90: quantile(actionTimes, 0.9),
        p95: quantile(actionTimes, 0.95),
        max: quantile(actionTimes, 1)
      },
      http_error_count: httpErrorCount,
      request_failure_count: requestFailureCount,
      console_error_count: consoleErrorCount
    },
    users: results
  });
}
