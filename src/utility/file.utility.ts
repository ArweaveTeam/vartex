import { existsSync, mkdirSync, readdirSync, unlinkSync } from "node:fs";
import { join } from "node:path";

export function mkdir(path: string) {
  if (!existsSync(path)) {
    mkdirSync(path);
  }
}

export function clean(path: string) {
  const files = readdirSync(path);
  for (const file in files) {
    unlinkSync(join(path, file));
  }
}
