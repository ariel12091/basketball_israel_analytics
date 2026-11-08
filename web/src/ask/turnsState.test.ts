import { describe, it, expect } from "vitest";
import type { AskAnswer } from "./turn";
import {
  CUT_OFF,
  INTERRUPTED,
  restoreTurns,
  turnsReducer as reduce,
  type AskTurnView,
} from "./turnsState";

const answer = { text: "42", results: [] } as unknown as AskAnswer;
const done: AskTurnView = {
  id: "a",
  question: "q1",
  status: [],
  answer,
  error: null,
  pending: false,
};
const start = (turns: AskTurnView[], id: string, keep: boolean) =>
  reduce(turns, { type: "start", id, question: `q-${id}`, keep });

describe("Ask turns reducer", () => {
  it("a follow-up keeps the conversation and interrupts a turn still pending", () => {
    const one = start([done], "b", true);
    const two = start(one, "c", true);
    expect(two.map((t) => t.id)).toEqual(["a", "b", "c"]);
    expect(two[0]).toEqual(done);
    expect(two[1]).toMatchObject({ pending: false, error: INTERRUPTED });
    expect(two[2]).toMatchObject({ pending: true, status: [], answer: null });
  });

  it("a fresh question starts a new conversation", () => {
    expect(start([done], "b", false).map((t) => t.id)).toEqual(["b"]);
  });

  it("status lines append, then the final answer ends the turn", () => {
    let ts = start([done], "b", true);
    ts = reduce(ts, { type: "status", id: "b", text: "Looking up" });
    ts = reduce(ts, { type: "status", id: "b", text: "Writing" });
    expect(ts[1].status).toEqual(["Looking up", "Writing"]);
    ts = reduce(ts, { type: "final", id: "b", answer });
    expect(ts[1]).toMatchObject({ pending: false, answer, error: null });
    expect(ts[0]).toEqual(done);
  });

  it("a failure ends the turn; a cut-off stream fails only a pending turn", () => {
    const failed = reduce(start([], "b", true), {
      type: "fail",
      id: "b",
      error: "Network error; try again",
    });
    expect(failed[0]).toMatchObject({
      pending: false,
      error: "Network error; try again",
    });
    expect(reduce(failed, { type: "cutOff", id: "b" })).toEqual(failed);
    expect(
      reduce(start([], "b", true), { type: "cutOff", id: "b" })[0],
    ).toMatchObject({ pending: false, error: CUT_OFF });
  });

  it("a reload interrupts a pending turn; reset empties the conversation", () => {
    const pending = start([done], "b", true);
    expect(restoreTurns(pending)[1]).toMatchObject({
      pending: false,
      error: INTERRUPTED,
    });
    expect(restoreTurns(undefined)).toEqual([]);
    expect(reduce(pending, { type: "reset" })).toEqual([]);
  });
});
