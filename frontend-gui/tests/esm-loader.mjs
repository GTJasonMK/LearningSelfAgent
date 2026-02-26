import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const TARGET_MODULE_SUFFIXES = [
  "/src/renderer/need_input.js",
  "/src/renderer/run_status.js"
];

export async function load(url, context, defaultLoad) {
  if (TARGET_MODULE_SUFFIXES.some((suffix) => String(url).endsWith(suffix))) {
    const source = await readFile(fileURLToPath(url), "utf8");
    return {
      format: "module",
      source,
      shortCircuit: true
    };
  }
  return defaultLoad(url, context, defaultLoad);
}
