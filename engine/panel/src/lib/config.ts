export type PanelGlobals = {
  __BETTERMQ_EXTERNAL_AUTH__?: boolean;
  __BETTERMQ_PANEL_MODE__?: string;
  __BETTERMQ_ADMIN_API__?: string;
  __BETTERMQ_CELL_LABEL__?: string;
  __BETTERMQ_FEATURE_FLAGS__?: string;
};

declare global {
  interface Window extends PanelGlobals {}
}

export function panelMode(): string {
  return window.__BETTERMQ_PANEL_MODE__ || "embedded";
}

export function adminApiRoot(): string {
  return window.__BETTERMQ_ADMIN_API__ || "/admin/v1";
}

export function isExternalAuth(): boolean {
  return Boolean(window.__BETTERMQ_EXTERNAL_AUTH__);
}
