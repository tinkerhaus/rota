import { describe, expect, it } from "vitest";

import { buildMetadata } from "../dist/common.js";

describe("auth metadata", () => {
  it("adds the bearer and x-rota-token headers", () => {
    const md = buildMetadata({ "x-app": "demo" }, "secret");

    expect(md.get("x-app")).toEqual(["demo"]);
    expect(md.get("authorization")).toEqual(["Bearer secret"]);
    expect(md.get("x-rota-token")).toEqual(["secret"]);
  });
});
