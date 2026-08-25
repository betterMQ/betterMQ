import { reactive } from "vue";
import { isExternalAuth } from "@/lib/config";
import { adminUrl, apiGet, apiPost, publicUrl } from "@/lib/api";
import { refreshFlows, refreshGroups, refreshQueues } from "@/lib/catalog";
import {
  applyTheme,
  loadSavedApiBase,
  loadSavedCellId,
  loadSavedToken,
  readThemeMode,
  saveApiBase,
  saveCellId,
  saveToken,
  type ThemeMode,
} from "@/lib/storage";

export type AuthPhase = "loading" | "setup" | "gate" | "reveal" | "app";

export type CellRecord = {
  id: string;
  region: string;
  controllerUrl: string;
  label?: string | null;
};

export type CellMember = {
  profile: string;
  reachUrl: string;
  advertiseUrl: string;
  name: string;
  cellId: string;
};

export type AuthConfig = {
  mode: string;
  configured: boolean;
  setup_open?: boolean;
  setup_closes_in_secs?: number | null;
};

export type ClusterNode = {
  id?: string;
  addr?: string;
  name?: string;
  healthy?: boolean;
  is_self?: boolean;
  led_shards?: number[];
  preferred_shards?: number[];
};

export type ClusterStatus = {
  cluster_id?: string;
  generation?: number;
  enabled?: boolean;
  node_count?: number;
  healthy_count?: number;
  scheduler_leader_id?: string;
  this_node_scheduler_leader?: boolean;
  nodes?: ClusterNode[];
};

export const session = reactive({
  phase: "loading" as AuthPhase,
  apiBase: loadSavedApiBase(),
  token: "",
  revealedToken: "",
  auth: null as AuthConfig | null,
  health: "…",
  healthOk: false,
  clusterLabel: "—",
  cluster: null as ClusterStatus | null,
  cells: [] as CellRecord[],
  members: [] as CellMember[],
  selectedCellId: loadSavedCellId(),
  themeMode: readThemeMode() as ThemeMode,
  splitAuth: false,
  joinToken: "",
  gateMessage: "",
  isSeed: false,
});

session.token = loadSavedToken(session.apiBase);

let healthTimer: ReturnType<typeof setInterval> | null = null;
let clusterTimer: ReturnType<typeof setInterval> | null = null;

export function selectedCell(): CellRecord | null {
  return session.cells.find((c) => c.id === session.selectedCellId) || session.cells[0] || null;
}

export function setApiBase(base: string) {
  saveApiBase(base);
  session.apiBase = loadSavedApiBase();
  session.token = loadSavedToken(session.apiBase);
  void initAuth();
}

export function setToken(token: string) {
  session.token = token;
  saveToken(session.apiBase, token);
}

export function setCellId(id: string) {
  session.selectedCellId = id;
  saveCellId(id);
}

export function setTheme(mode: ThemeMode) {
  session.themeMode = mode;
  applyTheme(mode);
}

export async function initAuth() {
  applyTheme(session.themeMode);
  session.phase = "loading";
  session.splitAuth = false;
  session.gateMessage = "";
  try {
    const a = await fetch(publicUrl("/v1/auth/config")).then((r) => r.json() as Promise<AuthConfig>);
    const b = await fetch(publicUrl("/v1/auth/config")).then((r) => r.json() as Promise<AuthConfig>);
    if (a.configured !== b.configured || a.mode !== b.mode) {
      session.splitAuth = true;
      session.phase = "gate";
      session.auth = a;
      session.gateMessage =
        "Load balancer is returning different auth state from different brokers. Use one BetterMQ process for local dev.";
      return;
    }
    session.auth = a;
    if (isExternalAuth() && a.mode === "control_plane") {
      startApp();
      return;
    }
    if (!a.configured) {
      setToken("");
      session.phase = "setup";
      return;
    }
    if (!session.token) {
      session.phase = "gate";
      return;
    }
    try {
      await apiGet(publicUrl("/v1/queues"));
      startApp();
    } catch (e) {
      setToken("");
      session.phase = "gate";
      session.gateMessage = e instanceof Error ? e.message : "Invalid API token for this broker.";
    }
  } catch {
    session.phase = "gate";
    session.gateMessage = `Cannot reach broker at ${session.apiBase}. Check that BetterMQ is running.`;
  }
}

export async function setupPassword(password: string) {
  const res = await apiPost<{ token: string }>(publicUrl("/v1/local-auth/setup"), { password });
  setToken(res.token);
  session.revealedToken = res.token;
  session.phase = "reveal";
}

export async function regenerateToken(password: string) {
  const res = await apiPost<{ token: string }>(publicUrl("/v1/local-auth/regenerate"), { password });
  setToken(res.token);
  session.revealedToken = res.token;
  session.phase = "reveal";
}

export function finishReveal() {
  session.revealedToken = "";
  startApp();
}

export function submitGateToken(raw: string) {
  const t = raw.trim();
  if (!t) throw new Error("Paste your sk_local_… token.");
  if (!t.startsWith("sk_local_")) throw new Error("Expected a sk_local_… token from this broker's setup.");
  setToken(t);
}

export function startApp() {
  session.phase = "app";
  void refreshCells();
  void refreshQueues();
  void refreshGroups();
  void refreshFlows();
  void pollHealth();
  void pollCluster();
  if (healthTimer) clearInterval(healthTimer);
  if (clusterTimer) clearInterval(clusterTimer);
  healthTimer = setInterval(() => void pollHealth(), 15000);
  clusterTimer = setInterval(() => void pollCluster(), 5000);
}

export async function refreshCells() {
  try {
    const reg = await apiGet<{ cells?: CellRecord[]; members?: CellMember[] }>(adminUrl("/cells"));
    session.cells = reg.cells || [];
    session.members = reg.members || [];
    if (!session.cells.some((c) => c.id === session.selectedCellId)) {
      session.selectedCellId = session.cells[0]?.id || "local";
    }
  } catch {
    session.cells = [];
    session.members = [];
  }
}

export async function pollHealth() {
  try {
    const r = await fetch(publicUrl("/healthz"));
    session.healthOk = r.ok;
    const j = (await r.json().catch(() => ({}))) as { status?: string; version?: string };
    session.health = j.version || (r.ok ? "ok" : "down");
  } catch {
    session.healthOk = false;
    session.health = "down";
  }
}

export async function pollCluster() {
  try {
    const c = await apiGet<ClusterStatus>(publicUrl("/v1/cluster"));
    session.cluster = c;
    if (c && (c.node_count || (c.nodes || []).length)) {
      const n = c.node_count || (c.nodes || []).length;
      const h = c.healthy_count ?? n;
      session.clusterLabel = `${h}/${n}`;
    } else {
      session.clusterLabel = "standalone";
    }
  } catch {
    session.cluster = null;
    session.clusterLabel = "—";
  }
}

export function setupHint(cfg: AuthConfig | null): string {
  if (!cfg) return "Choose a password to claim this broker.";
  if (cfg.setup_open) {
    const secs = cfg.setup_closes_in_secs;
    if (typeof secs === "number") {
      const mins = Math.max(1, Math.ceil(secs / 60));
      return `Open for about ${mins} more minute${mins === 1 ? "" : "s"}. Then restart BetterMQ to open setup again.`;
    }
    return "Anyone who can open this page can claim admin until a password is set.";
  }
  return "Setup window closed. Restart BetterMQ, then refresh this page.";
}
