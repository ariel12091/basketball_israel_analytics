import { expect, test, type Page } from "@playwright/test";

type Pending = {
  controller: ReadableStreamDefaultController<Uint8Array>;
  body: ReadableStream<Uint8Array>;
  aborted: boolean;
  cancels: number;
};
declare global {
  interface Window {
    askStreams: Pending[];
  }
}

async function open(page: Page, league: string) {
  await page.addInitScript(() => {
    window.askStreams = [];
    const original = window.fetch;
    window.fetch = async (input, init) => {
      if (String(input) !== "/api/v1/ask") return original(input, init);
      const pending = { aborted: false, cancels: 0 } as Pending;
      pending.body = new ReadableStream<Uint8Array>({
        start(controller) {
          pending.controller = controller;
        },
        cancel() {
          pending.cancels++;
          // Cleanup must not hold the reader lock while remote teardown stalls.
          return new Promise<void>(() => {});
        },
      });
      // Deliberately do not abort the body for the client: it must own cleanup.
      init?.signal?.addEventListener("abort", () => {
        pending.aborted = true;
      });
      window.askStreams.push(pending);
      return new Response(pending.body, {
        headers: { "content-type": "text/event-stream" },
      });
    };
  });
  await page.route("**/api/v1/**", (route) =>
    route.fulfill({
      json: { leagues: [], rows: [], teams: [], meta: { version: "test" } },
    }),
  );
  await page.goto(`/${league}/home?team_id=all`);
  await page.getByRole("button", { name: "Ask", exact: true }).click();
  return page.getByRole("dialog", { name: "Ask a question" });
}
async function send(page: Page, question: string) {
  await page.getByLabel("Your question").fill(question);
  await page.getByLabel("Your question").press("Enter");
}
async function deliver(page: Page, index: number, text: string) {
  const final = {
    type: "final",
    answer: {
      text,
      lang: "en",
      results: [],
      evidence: [],
      links: [],
      fallback: false,
      usage: { inputTokens: 0, outputTokens: 0, costUsd: 0 },
    },
  };
  await page.evaluate(
    ({ index, final }) => {
      // A bad trailing event must never overwrite a terminal answer.
      window.askStreams[index].controller.enqueue(
        new TextEncoder().encode(
          `data: ${JSON.stringify(final)}\n\ndata: ${JSON.stringify({ type: "error", error: "stale error" })}\n\n`,
        ),
      );
    },
    { index, final },
  );
}
async function expectReleased(page: Page, index: number) {
  await expect
    .poll(() =>
      page.evaluate((i) => {
        const r = window.askStreams[i];
        return {
          aborted: r.aborted,
          cancels: r.cancels,
          locked: r.body.locked,
        };
      }, index),
    )
    .toEqual({ aborted: true, cancels: 1, locked: false });
}

for (const league of ["il", "euroleague", "eurocup"]) {
  test(`Ask ${league}: terminal answers release the reader and cannot overwrite a new conversation`, async ({
    page,
  }) => {
    const panel = await open(page, league);
    await send(page, "first question");
    await expect
      .poll(() => page.evaluate(() => window.askStreams.length))
      .toBe(1);
    await deliver(page, 0, "First answer");
    await expect(
      panel.getByText("First answer", { exact: true }),
    ).toBeVisible();
    await expectReleased(page, 0);
    await panel.getByRole("button", { name: "New question" }).click();
    await send(page, "new question");
    await expect
      .poll(() => page.evaluate(() => window.askStreams.length))
      .toBe(2);
    await deliver(page, 1, "New answer");
    await expect(panel.getByText("New answer", { exact: true })).toBeVisible();
    await expect(panel.getByText("First answer", { exact: true })).toHaveCount(
      0,
    );
    await expect(panel.getByRole("alert")).toHaveCount(0);
    await expectReleased(page, 1);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
  });

  test(`Ask ${league}: a stalled stream times out, releases resources and allows retry`, async ({
    page,
  }) => {
    const panel = await open(page, league);
    await page.clock.install();
    await send(page, "stalled question");
    await expect
      .poll(() => page.evaluate(() => window.askStreams.length))
      .toBe(1);
    await page.clock.fastForward(45_000);
    await expect(panel.getByRole("alert")).toHaveText(
      "The answer timed out; try again",
    );
    await expectReleased(page, 0);
    await send(page, "try again");
    await expect
      .poll(() => page.evaluate(() => window.askStreams.length))
      .toBe(2);
    await deliver(page, 1, "Recovered answer");
    await expect(
      panel.getByText("Recovered answer", { exact: true }),
    ).toBeVisible();
    await expectReleased(page, 1);
  });
}
