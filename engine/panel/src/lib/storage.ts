const TOKEN_PREFIX = "bettermq_api_token:";
const API_BASE_KEY = "bettermq_api_base";
const CELL_KEY = "bettermq_cell_id";
const LEGACY_TOKEN_KEY = "bettermq_api_token";

export function tokenStore(): Storage {
  try {
    return sessionStorage;
  } catch {
    return localStorage;
  }
}

export function defaultApiBase(): string {
  if (location.protocol === "http:" || location.protocol === "https:") {
    return location.origin;
  }
  return "http://127.0.0.1:8080";
}

export function allowlistedApiBase(raw: string): string {
  const v = (raw || "").trim().replace(/\/$/, "");
  if (location.protocol === "http:" || location.protocol === "https:") {
    try {
      if (v && new URL(v).origin === location.origin) return new URL(v).origin;
    } catch {
      /* ignore */
    }
    return location.origin;
  }
  return v || defaultApiBase();
}

export function loadSavedApiBase(): string {
  return allowlistedApiBase(localStorage.getItem(API_BASE_KEY) || defaultApiBase());
}

export function saveApiBase(base: string) {
  localStorage.setItem(API_BASE_KEY, allowlistedApiBase(base));
}

export function tokenStorageKey(apiBase: string): string {
  return TOKEN_PREFIX + apiBase;
}

export function loadSavedToken(apiBase: string): string {
  const key = tokenStorageKey(apiBase);
  let t = tokenStore().getItem(key);
  if (!t) {
    t = localStorage.getItem(key) || localStorage.getItem(LEGACY_TOKEN_KEY);
    if (t) {
      tokenStore().setItem(key, t);
      localStorage.removeItem(key);
      localStorage.removeItem(LEGACY_TOKEN_KEY);
    }
  }
  return t || "";
}

export function saveToken(apiBase: string, token: string) {
  const t = normalizeToken(token);
  if (t) tokenStore().setItem(tokenStorageKey(apiBase), t);
  else tokenStore().removeItem(tokenStorageKey(apiBase));
}

export function normalizeToken(raw: string): string {
  let t = (raw || "").trim();
  if (/^bearer\s+/i.test(t)) t = t.replace(/^bearer\s+/i, "").trim();
  return t;
}

export function loadSavedCellId(): string {
  return localStorage.getItem(CELL_KEY) || "";
}

export function saveCellId(id: string) {
  localStorage.setItem(CELL_KEY, id);
}

export type ThemeMode = "system" | "light" | "dark";

export function readThemeMode(): ThemeMode {
  const stored = localStorage.getItem("themeMode");
  if (stored === "dark" || stored === "light" || stored === "system") return stored;
  return "system";
}

export function applyTheme(mode: ThemeMode) {
  const dark =
    mode === "dark" || (mode === "system" && window.matchMedia("(prefers-color-scheme: dark)").matches);
  document.documentElement.classList.toggle("dark", dark);
  document.documentElement.dataset.themeMode = mode;
  localStorage.setItem("themeMode", mode);
}
