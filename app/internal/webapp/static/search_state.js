// The URL is the complete search input. Add new parameters here and to the
// round-trip tests together; never drop an invalid explicit scope into "all".
import { PRIORITIES } from "./api.js";

export const MODES = ["hybrid", "keyword", "exact"];

export function searchParams(input) {
  const params = new URLSearchParams({
    q: input.query, mode: input.mode, max_results: String(input.max_results),
  });
  for (const key of ["priorities", "sources"]) {
    if (input[key]) params.set(key, input[key].join(","));
  }
  if (input.since) params.set("since", input.since);
  return params;
}

export function readSearch(params) {
  for (const key of ["q", "mode", "priorities", "sources", "since", "max_results"]) {
    if (params.getAll(key).length > 1) throw new Error("duplicate " + key);
  }
  const input = {
    query: (params.get("q") || "").trim(),
    mode: params.get("mode") ?? "hybrid",
    max_results: Number(params.get("max_results") ?? 30),
  };
  if (!MODES.includes(input.mode)) throw new Error("invalid mode");
  if (!Number.isInteger(input.max_results) || input.max_results < 1 || input.max_results > 200) {
    throw new Error("depth must be an integer from 1 to 200");
  }
  for (const key of ["priorities", "sources"]) {
    if (!params.has(key)) continue;
    const values = params.get(key).split(",").map((value) => value.trim());
    if (values.some((value) => key === "priorities" ? !PRIORITIES.includes(value) : !/^[a-z0-9_-]+$/i.test(value))) {
      throw new Error("invalid " + key);
    }
    input[key] = [...new Set(values)];
  }
  if (params.has("since")) {
    const since = params.get("since");
    const date = new Date(since);
    if (!/^\d{4}-\d{2}-\d{2}$/.test(since) || !Number.isFinite(date.getTime()) || date.toISOString().slice(0, 10) !== since) {
      throw new Error("since must be a valid YYYY-MM-DD date");
    }
    input.since = since;
  }
  return input;
}
