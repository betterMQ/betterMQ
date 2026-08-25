import { createAvatar } from "@dicebear/core";
import * as glass from "@dicebear/glass";

const cache = new Map<string, string>();

export function glassAvatar(seed: string): string {
  const key = seed.trim() || "local";
  const hit = cache.get(key);
  if (hit) return hit;
  const uri = createAvatar(glass, {
    seed: key,
    size: 72,
    radius: 24,
  }).toDataUri();
  cache.set(key, uri);
  return uri;
}
