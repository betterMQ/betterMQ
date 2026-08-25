<script setup lang="ts">
import { computed, onMounted, reactive, ref } from "vue";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetFooter,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import Field from "@/components/form/Field.vue";
import ActionMessage from "@/components/form/ActionMessage.vue";
import AppSelect from "@/components/form/AppSelect.vue";
import { adminUrl, apiGet, apiPost, apiPut, publicUrl } from "@/lib/api";
import {
  dataForCell,
  queryCells,
  type AdminCluster,
  type AdminNode,
  type AdminShard,
} from "@/lib/federation";
import { useActionStatus } from "@/composables/useActionStatus";
import { errText } from "@/lib/payload";
import { refreshCells, selectedCell, session, type CellMember, type CellRecord } from "@/lib/session";

type SectionId = "overview" | "nodes" | "cells" | "cluster" | "this-node" | "storage";

type NodeRow = {
  key: string;
  name: string;
  role: string;
  addr: string;
  healthy: boolean;
  self: boolean;
  cellId: string;
  cellLabel: string;
  broker?: AdminNode;
  member?: CellMember;
};

const sections: { id: SectionId; label: string }[] = [
  { id: "overview", label: "Overview" },
  { id: "nodes", label: "Nodes" },
  { id: "cells", label: "Cells" },
  { id: "cluster", label: "Cluster" },
  { id: "this-node", label: "This node" },
  { id: "storage", label: "Storage" },
];

const section = ref<SectionId>("overview");
const selectedNodeKey = ref("");

const nodesByCell = reactive<Record<string, AdminNode[]>>({});
const shardsByCell = reactive<Record<string, AdminShard[]>>({});
const clusterByCell = reactive<Record<string, AdminCluster>>({});
const restartMsg = ref("");
const rfMsg = ref("");
const infraStatus = ref<Record<string, unknown> | null>(null);
const joinToken = ref("");

const createOpen = ref(false);
const cellForm = reactive({ id: "", region: "", controllerUrl: "", label: "" });

const attachOpen = ref(false);
const attach = reactive({
  profile: "all",
  name: "",
  reach: "",
  split: false,
  advertise: "",
  token: "",
  error: "",
});

const nodeForm = reactive({ name: "", publicUrl: "" });
const storage = reactive({
  mode: "local",
  endpoint: "",
  bucket: "",
  payloadBucket: "",
  accessKey: "",
  secretKey: "",
  region: "auto",
  test: "",
});

const profileOptions = [
  { value: "all", label: "all — broker + local dispatch" },
  { value: "broker", label: "broker" },
  { value: "controller", label: "controller (health only)" },
  { value: "dispatch", label: "dispatch fleet" },
  { value: "gateway", label: "gateway" },
  { value: "panel", label: "panel" },
];

const storageOptions = [
  { value: "local", label: "Local disk (WAL + RocksDB)" },
  { value: "slate", label: "SlateDB (S3 / MinIO / R2)" },
];

const fedStatus = useActionStatus();
const clusterStatus = useActionStatus();
const cellStatus = useActionStatus();
const nodeStatus = useActionStatus();
const storageStatus = useActionStatus();
const detailStatus = useActionStatus();
const attachOk = ref("");

const profileHint = computed(() => {
  const hints: Record<string, string> = {
    all: "Joins the replica set (WAL). Extra all nodes add shard capacity after 3, not RF=4.",
    broker: "Joins the replica set. Pair with a dispatch fleet for delivery.",
    controller: "Registered for health only — controller voter APIs are separate.",
    dispatch: "Fleet worker. Not a replica. Process must use --profile dispatch.",
    gateway: "Ingest router. Not a replica. Process must use --profile gateway.",
    panel: "Registers another management URL. Does not join Raft.",
  };
  return hints[attach.profile] || "";
});

const needsToken = computed(() => attach.profile === "all" || attach.profile === "broker");

const rfBanner = computed(() => {
  const ov = currentCluster.value;
  if (!ov) return "";
  const n = ov.nodeCount || 0;
  const rf = ov.replicationFactor || 0;
  if (n === 2) return "RF=2 / minISR=2 — both nodes must be up. Add a third broker for HA.";
  if (n >= 3 && rf < 3) return "Expanding replicas to RF=3…";
  if (n >= 4 && rf === 3) return "RF stays 3. Extra brokers add shard capacity, not a fourth copy.";
  return "";
});

const listedCells = computed<CellRecord[]>(() =>
  session.cells.length
    ? session.cells
    : [{ id: "local", region: "local", controllerUrl: "", label: "local" }],
);

const nodeRows = computed<NodeRow[]>(() => {
  const rows: NodeRow[] = [];
  for (const cell of listedCells.value) {
    const brokers = nodesByCell[cell.id] || [];
    const seen = new Set<string>();
    for (const n of brokers) {
      const addr = (n.addr || "").replace(/\/$/, "");
      if (addr) seen.add(addr);
      rows.push({
        key: `broker:${cell.id}:${n.id || n.addr || n.name}`,
        name: n.name || shortId(n.id) || n.addr || "broker",
        role: "broker",
        addr: n.addr || "",
        healthy: n.alive ?? n.healthy ?? true,
        self: Boolean(n.self ?? n.is_self),
        cellId: cell.id,
        cellLabel: cell.label || cell.id,
        broker: n,
      });
    }
    for (const m of session.members.filter((member) => member.cellId === cell.id)) {
      const url = (m.reachUrl || "").replace(/\/$/, "");
      if (url && seen.has(url)) continue;
      rows.push({
        key: `member:${cell.id}:${m.name}:${m.reachUrl}`,
        name: m.name || m.profile,
        role: m.profile,
        addr: m.reachUrl,
        healthy: true,
        self: false,
        cellId: cell.id,
        cellLabel: cell.label || cell.id,
        member: m,
      });
    }
  }
  return rows;
});

const selectedRow = computed(() => nodeRows.value.find((row) => row.key === selectedNodeKey.value) || null);

const currentCluster = computed(() => {
  const cell = selectedCell();
  if (cell && clusterByCell[cell.id]) return clusterByCell[cell.id];
  return Object.values(clusterByCell)[0] || null;
});

const shardRows = computed(() => {
  const cell = selectedCell();
  if (cell && shardsByCell[cell.id]) return shardsByCell[cell.id];
  const first = Object.keys(shardsByCell)[0];
  return first ? shardsByCell[first] || [] : [];
});

const kpiNode = computed(() => String(infraStatus.value?.node_name || "—"));
const kpiUrl = computed(() => String(infraStatus.value?.public_url || "—"));
const kpiStorage = computed(() => String(infraStatus.value?.active_storage || "—"));

onMounted(() => {
  void load();
});

async function load() {
  await refreshCells();
  await Promise.all([loadFed(), loadInfra()]);
  if (selectedNodeKey.value && !nodeRows.value.some((row) => row.key === selectedNodeKey.value)) {
    selectedNodeKey.value = "";
  }
}

async function loadFed() {
  fedStatus.reset();
  try {
    const [nodes, shards, cluster] = await Promise.all([
      queryCells<{ nodes?: AdminNode[] }>("nodes"),
      queryCells<{ shards?: AdminShard[] }>("shards"),
      queryCells<AdminCluster>("cluster"),
    ]);
    for (const key of Object.keys(nodesByCell)) delete nodesByCell[key];
    for (const row of nodes.ok || []) nodesByCell[row.cell] = row.data.nodes || [];
    for (const key of Object.keys(shardsByCell)) delete shardsByCell[key];
    for (const row of shards.ok || []) shardsByCell[row.cell] = row.data.shards || [];
    for (const key of Object.keys(clusterByCell)) delete clusterByCell[key];
    for (const row of cluster.ok || []) clusterByCell[row.cell] = row.data;
    const failed = [...(nodes.failed || []), ...(shards.failed || []), ...(cluster.failed || [])];
    if (failed.length) fedStatus.fail(`Federated query failed: ${failed[0].cell} — ${failed[0].error}`);
    const cell = selectedCell();
    if (cell) {
      const ov = dataForCell(cluster, cell.id);
      if (ov) rfMsg.value = "";
    }
  } catch (e) {
    fedStatus.fail(e);
  }
}

async function loadInfra() {
  try {
    const st = await apiGet<Record<string, unknown>>(publicUrl("/v1/infra/status"));
    infraStatus.value = st;
    session.isSeed = Boolean(st.is_cluster_seed);
    if (st.needs_restart) {
      restartMsg.value = `Pending ${st.pending_storage || "config"} storage — restart broker to apply.`;
    } else {
      restartMsg.value = "";
    }
  } catch {
    infraStatus.value = null;
  }
  try {
    const cfg = await apiGet<{
      node?: { name?: string; publicUrl?: string };
      storage?: { mode?: string; s3?: Record<string, string> };
    }>(publicUrl("/v1/infra/config"));
    nodeForm.name = cfg.node?.name || "";
    nodeForm.publicUrl = cfg.node?.publicUrl || "";
    if (cfg.storage?.mode === "slate" && cfg.storage.s3) {
      storage.mode = "slate";
      storage.endpoint = cfg.storage.s3.endpoint || "";
      storage.bucket = cfg.storage.s3.bucket || "";
      storage.payloadBucket = cfg.storage.s3.payloadBucket || "";
      storage.accessKey = cfg.storage.s3.accessKey || "";
      storage.region = cfg.storage.s3.region || "auto";
    } else {
      storage.mode = "local";
    }
  } catch {
    /* ignore */
  }
}

async function createCell() {
  cellStatus.reset();
  try {
    await apiPost(adminUrl("/cells"), {
      id: cellForm.id,
      region: cellForm.region,
      controllerUrl: cellForm.controllerUrl,
      label: cellForm.label || undefined,
    });
    cellStatus.succeed(`Registered cell ${cellForm.id}`);
    createOpen.value = false;
    cellForm.id = cellForm.region = cellForm.controllerUrl = cellForm.label = "";
    await load();
  } catch (e) {
    cellStatus.fail(e);
  }
}

async function probe() {
  attach.error = "";
  attachOk.value = "";
  try {
    const res = await apiPost<{ ok?: boolean; error?: string; profileHint?: string; ready?: boolean }>(
      adminUrl("/probe"),
      { reachUrl: attach.reach },
    );
    if (res.ok) attachOk.value = `Reachable (${res.profileHint || "unknown"})${res.ready ? ", ready" : ", not ready"}`;
    else attach.error = res.error || "not reachable";
  } catch (e) {
    attach.error = errText(e);
  }
}

async function attachNode() {
  attach.error = "";
  attachOk.value = "";
  const cell = selectedCell();
  const advertise = attach.split && attach.advertise.trim() ? attach.advertise.trim() : attach.reach;
  try {
    const res = await apiPost<{
      joined?: boolean;
      profile?: string;
      expand?: { warning?: string };
      enroll?: { needs_restart?: boolean; message?: string };
    }>(adminUrl("/attach"), {
      profile: attach.profile,
      reachUrl: attach.reach,
      advertiseUrl: advertise,
      nodeName: attach.name,
      joinToken: attach.token || joinToken.value,
      seedUrl: nodeForm.publicUrl || session.apiBase,
      cellId: cell?.id || "local",
    });
    attachOk.value = res.joined ? `Joined ${res.profile}` : `Registered ${res.profile}`;
    if (res.expand?.warning) rfMsg.value = res.expand.warning;
    if (res.enroll?.needs_restart) restartMsg.value = res.enroll.message || "Restart cluster nodes after join.";
    attachOpen.value = false;
    attach.split = false;
    attach.advertise = "";
    await load();
  } catch (e) {
    attach.error = errText(e);
  }
}

async function createCluster() {
  clusterStatus.reset();
  try {
    const res = await apiPost<{ join_token?: string; message?: string; needs_restart?: boolean }>(
      publicUrl("/v1/infra/cluster/create"),
      {},
    );
    joinToken.value = res.join_token || "";
    attach.token = joinToken.value;
    session.joinToken = joinToken.value;
    clusterStatus.succeed(res.message || "Cluster created");
    if (res.needs_restart) restartMsg.value = res.message || "Restart required.";
    await load();
  } catch (e) {
    clusterStatus.fail(e);
  }
}

async function syncCluster() {
  clusterStatus.reset();
  try {
    const res = await apiPost<{ nodes?: unknown[] }>(publicUrl("/v1/infra/cluster/sync"), {});
    clusterStatus.succeed(`Synced ${(res.nodes || []).length} nodes from seed`);
    restartMsg.value = "Restart recommended after sync.";
    await load();
  } catch (e) {
    clusterStatus.fail(e);
  }
}

async function removeNode(name: string) {
  if (!confirm(`Remove ${name} from the cluster?`)) return;
  detailStatus.reset();
  try {
    const res = await apiPost<{ message?: string; needs_restart?: boolean }>(
      publicUrl("/v1/infra/cluster/remove-node"),
      { node_name: name },
    );
    detailStatus.succeed(res.message || "Removed");
    if (res.needs_restart) restartMsg.value = res.message || "Restart required.";
    selectedNodeKey.value = "";
    await load();
  } catch (e) {
    detailStatus.fail(e);
  }
}

async function saveNode() {
  nodeStatus.reset();
  try {
    const res = await apiPut<{ message?: string; needs_restart?: boolean }>(publicUrl("/v1/infra/node"), {
      name: nodeForm.name,
      public_url: nodeForm.publicUrl,
      listen: "0.0.0.0:8080",
    });
    nodeStatus.succeed(res.message || "Node settings saved");
    if (res.needs_restart) restartMsg.value = res.message || "Restart required.";
    await loadInfra();
  } catch (e) {
    nodeStatus.fail(e);
  }
}

async function testS3() {
  storageStatus.reset();
  try {
    const res = await apiPost<{ ok?: boolean; message?: string }>(publicUrl("/v1/infra/storage/test"), {
      endpoint: storage.endpoint,
      bucket: storage.bucket,
      access_key: storage.accessKey,
      secret_key: storage.secretKey || "••••••••",
      region: storage.region,
    });
    storage.test = res.message || "";
    if (res.ok) storageStatus.succeed(storage.test);
    else storageStatus.fail(storage.test);
  } catch (e) {
    storage.test = errText(e);
    storageStatus.fail(storage.test);
  }
}

async function saveStorage() {
  storageStatus.reset();
  const body: Record<string, unknown> = { mode: storage.mode };
  if (storage.mode === "slate") {
    body.s3 = {
      endpoint: storage.endpoint,
      bucket: storage.bucket,
      payloadBucket: storage.payloadBucket || null,
      accessKey: storage.accessKey,
      secretKey: storage.secretKey || "••••••••",
      region: storage.region,
    };
  }
  try {
    const res = await apiPut<{ message?: string; needs_restart?: boolean }>(publicUrl("/v1/infra/storage"), body);
    storageStatus.succeed(res.message || "Saved");
    if (res.needs_restart) restartMsg.value = res.message || "Restart required.";
    await loadInfra();
  } catch (e) {
    storageStatus.fail(e);
  }
}

function shortId(id?: string) {
  if (!id) return "";
  return id.length > 8 ? id.slice(0, 8) : id;
}

function selectNode(row: NodeRow) {
  selectedNodeKey.value = row.key;
  detailStatus.reset();
}

function openAddNode() {
  section.value = "nodes";
  attachOpen.value = true;
}
</script>

<template>
  <div class="space-y-4">
    <div>
      <h1 class="text-lg font-semibold tracking-tight">Infrastructure</h1>
      <p class="text-sm text-muted-foreground">Cells, brokers, storage, and replication</p>
    </div>

    <Alert v-if="restartMsg">
      <AlertTitle>Restart required</AlertTitle>
      <AlertDescription>{{ restartMsg }}</AlertDescription>
    </Alert>
    <Alert v-if="rfBanner || rfMsg">
      <AlertTitle>Replication</AlertTitle>
      <AlertDescription>{{ rfMsg || rfBanner }}</AlertDescription>
    </Alert>
    <ActionMessage :error="fedStatus.error" />

    <div class="flex flex-col gap-6 lg:flex-row">
      <nav class="flex gap-1 overflow-x-auto lg:w-44 lg:shrink-0 lg:flex-col lg:overflow-visible">
        <button
          v-for="item in sections"
          :key="item.id"
          type="button"
          class="shrink-0 rounded-xl px-3 py-1.5 text-left text-sm transition-colors"
          :class="
            section === item.id
              ? 'bg-muted font-medium text-foreground'
              : 'text-muted-foreground hover:bg-muted/60 hover:text-foreground'
          "
          @click="section = item.id"
        >
          {{ item.label }}
        </button>
      </nav>

      <div class="min-w-0 flex-1 space-y-4">
        <template v-if="section === 'overview'">
          <div class="grid gap-3 sm:grid-cols-3">
            <Card>
              <CardHeader class="pb-2">
                <CardDescription>This node</CardDescription>
                <CardTitle>{{ kpiNode }}</CardTitle>
              </CardHeader>
              <CardContent class="truncate font-mono text-xs text-muted-foreground">{{ kpiUrl }}</CardContent>
            </Card>
            <Card>
              <CardHeader class="pb-2">
                <CardDescription>Storage</CardDescription>
                <CardTitle class="capitalize">{{ kpiStorage }}</CardTitle>
              </CardHeader>
            </Card>
            <Card>
              <CardHeader class="pb-2">
                <CardDescription>Cluster</CardDescription>
                <CardTitle>{{ session.clusterLabel }}</CardTitle>
              </CardHeader>
              <CardContent class="text-xs text-muted-foreground">
                {{ nodeRows.length }} node{{ nodeRows.length === 1 ? "" : "s" }}
                <span v-if="currentCluster"> · RF {{ currentCluster.replicationFactor || "—" }}</span>
              </CardContent>
            </Card>
          </div>
          <Card>
            <CardHeader>
              <CardTitle>Nodes</CardTitle>
              <CardDescription>Select a node for details, or open the Nodes section.</CardDescription>
            </CardHeader>
            <CardContent>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Name</TableHead>
                    <TableHead>Role</TableHead>
                    <TableHead>Health</TableHead>
                    <TableHead>Address</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  <TableRow v-if="!nodeRows.length">
                    <TableCell colspan="4" class="whitespace-normal text-muted-foreground">
                      No brokers registered yet. Add a node to this cell.
                    </TableCell>
                  </TableRow>
                  <TableRow
                    v-for="row in nodeRows"
                    :key="row.key"
                    class="cursor-pointer"
                    @click="selectNode(row); section = 'nodes'"
                  >
                    <TableCell>
                      {{ row.name }}
                      <Badge v-if="row.self" variant="secondary" class="ml-1">this</Badge>
                    </TableCell>
                    <TableCell class="capitalize">{{ row.role }}</TableCell>
                    <TableCell>
                      <Badge :variant="row.healthy ? 'outline' : 'destructive'">
                        {{ row.healthy ? "Healthy" : "Down" }}
                      </Badge>
                    </TableCell>
                    <TableCell class="max-w-[16rem] truncate font-mono text-xs">{{ row.addr || "—" }}</TableCell>
                  </TableRow>
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </template>

        <template v-else-if="section === 'nodes'">
          <div class="flex flex-wrap items-center justify-between gap-2">
            <div>
              <h2 class="text-base font-medium">Nodes</h2>
              <p class="text-sm text-muted-foreground">Brokers and other processes in this deployment</p>
            </div>
            <Button size="sm" @click="openAddNode">Add node</Button>
          </div>
          <div class="grid gap-4 xl:grid-cols-[minmax(0,1fr)_20rem]">
            <Card>
              <CardContent class="pt-4">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Name</TableHead>
                      <TableHead>Role</TableHead>
                      <TableHead>Cell</TableHead>
                      <TableHead>Health</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    <TableRow v-if="!nodeRows.length">
                      <TableCell colspan="4" class="whitespace-normal text-muted-foreground">
                        No nodes yet. Add a node after the process is listening.
                      </TableCell>
                    </TableRow>
                    <TableRow
                      v-for="row in nodeRows"
                      :key="row.key"
                      class="cursor-pointer"
                      :class="selectedNodeKey === row.key ? 'bg-muted/70' : ''"
                      @click="selectNode(row)"
                    >
                      <TableCell>
                        {{ row.name }}
                        <Badge v-if="row.self" variant="secondary" class="ml-1">this</Badge>
                      </TableCell>
                      <TableCell class="capitalize">{{ row.role }}</TableCell>
                      <TableCell>{{ row.cellLabel }}</TableCell>
                      <TableCell>
                        <Badge :variant="row.healthy ? 'outline' : 'destructive'">
                          {{ row.healthy ? "Healthy" : "Down" }}
                        </Badge>
                      </TableCell>
                    </TableRow>
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
            <Card>
              <template v-if="selectedRow">
                <CardHeader>
                  <CardTitle>{{ selectedRow.name }}</CardTitle>
                  <CardDescription class="capitalize">{{ selectedRow.role }} · {{ selectedRow.cellLabel }}</CardDescription>
                </CardHeader>
                <CardContent class="space-y-3 text-sm">
                  <div>
                    <p class="text-xs text-muted-foreground">Address</p>
                    <p class="break-all font-mono text-xs">{{ selectedRow.addr || "—" }}</p>
                  </div>
                  <div>
                    <p class="text-xs text-muted-foreground">Health</p>
                    <p>{{ selectedRow.healthy ? "Healthy" : "Down" }}</p>
                  </div>
                  <div v-if="selectedRow.broker?.id">
                    <p class="text-xs text-muted-foreground">Node ID</p>
                    <p class="break-all font-mono text-xs">{{ selectedRow.broker.id }}</p>
                  </div>
                  <div v-if="selectedRow.broker?.led_shards?.length">
                    <p class="text-xs text-muted-foreground">Led shards</p>
                    <p class="font-mono text-xs">{{ selectedRow.broker.led_shards.join(", ") }}</p>
                  </div>
                  <div v-if="shardRows.length">
                    <p class="text-xs text-muted-foreground">Shards</p>
                    <p class="text-xs text-muted-foreground">{{ shardRows.length }} in this cell</p>
                  </div>
                </CardContent>
                <CardFooter
                  v-if="session.isSeed && selectedRow.broker && !selectedRow.self"
                  class="flex-wrap"
                >
                  <ActionMessage :error="detailStatus.error" :ok="detailStatus.ok" />
                  <Button
                    variant="destructive"
                    size="sm"
                    @click="removeNode(selectedRow.broker.name || selectedRow.broker.addr || selectedRow.name)"
                  >
                    Remove
                  </Button>
                </CardFooter>
              </template>
              <CardHeader v-else>
                <CardTitle>Details</CardTitle>
                <CardDescription>Select a node to see address, health, and shards.</CardDescription>
              </CardHeader>
            </Card>
          </div>
        </template>

        <template v-else-if="section === 'cells'">
          <div class="flex flex-wrap items-center justify-between gap-2">
            <div>
              <h2 class="text-base font-medium">Cells</h2>
              <p class="text-sm text-muted-foreground">Register another region’s controller. This does not start VMs.</p>
            </div>
            <Button size="sm" @click="createOpen = true">Create cell</Button>
          </div>
          <Card>
            <CardContent class="pt-4">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>ID</TableHead>
                    <TableHead>Label</TableHead>
                    <TableHead>Region</TableHead>
                    <TableHead>Controller</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  <TableRow v-for="cell in listedCells" :key="cell.id">
                    <TableCell class="font-mono text-xs">{{ cell.id }}</TableCell>
                    <TableCell>{{ cell.label || "—" }}</TableCell>
                    <TableCell>{{ cell.region || "—" }}</TableCell>
                    <TableCell class="max-w-[16rem] truncate font-mono text-xs">
                      {{ cell.controllerUrl || "—" }}
                    </TableCell>
                  </TableRow>
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </template>

        <template v-else-if="section === 'cluster'">
          <Card>
            <CardHeader>
              <CardTitle>Cluster</CardTitle>
              <CardDescription>Create a seed, then add brokers from Nodes.</CardDescription>
            </CardHeader>
            <CardContent class="space-y-3">
              <p class="text-sm text-muted-foreground">
                Status: {{ session.clusterLabel }}
                <span v-if="currentCluster">
                  · {{ currentCluster.nodeCount || 0 }} nodes · RF {{ currentCluster.replicationFactor || "—" }}
                </span>
              </p>
              <div class="flex flex-wrap items-center gap-2">
                <Button size="sm" @click="createCluster">Create cluster</Button>
                <Button size="sm" variant="outline" @click="syncCluster">Re-sync</Button>
                <ActionMessage :error="clusterStatus.error" :ok="clusterStatus.ok" />
              </div>
            </CardContent>
            <CardFooter v-if="joinToken" class="block space-y-2">
              <p class="text-sm font-medium">Join token — 1 hour</p>
              <pre class="overflow-x-auto rounded-2xl border bg-muted p-3 font-mono text-xs">{{ joinToken }}</pre>
            </CardFooter>
          </Card>
        </template>

        <template v-else-if="section === 'this-node'">
          <Card>
            <CardHeader>
              <CardTitle>This node</CardTitle>
              <CardDescription>Name and URL other brokers use to reach this process</CardDescription>
            </CardHeader>
            <CardContent class="grid gap-3">
              <Field label="Node name"><Input v-model="nodeForm.name" placeholder="broker1" /></Field>
              <Field label="Public URL">
                <Input v-model="nodeForm.publicUrl" class="font-mono" placeholder="http://localhost:8080" />
              </Field>
            </CardContent>
            <CardFooter>
              <Button size="sm" @click="saveNode">Save node</Button>
              <ActionMessage :error="nodeStatus.error" :ok="nodeStatus.ok" />
            </CardFooter>
          </Card>
        </template>

        <template v-else>
          <Card>
            <CardHeader>
              <CardTitle>Storage</CardTitle>
              <CardDescription>WAL backend for this broker</CardDescription>
            </CardHeader>
            <CardContent class="grid gap-3">
              <Field label="Mode"><AppSelect v-model="storage.mode" :options="storageOptions" /></Field>
              <template v-if="storage.mode === 'slate'">
                <Field label="Endpoint"><Input v-model="storage.endpoint" class="font-mono text-xs" /></Field>
                <Field label="Bucket"><Input v-model="storage.bucket" class="font-mono" /></Field>
                <Field label="Payload bucket"><Input v-model="storage.payloadBucket" class="font-mono" /></Field>
                <Field label="Access key"><Input v-model="storage.accessKey" class="font-mono" /></Field>
                <Field label="Secret key">
                  <Input v-model="storage.secretKey" type="password" placeholder="leave blank to keep" />
                </Field>
                <Field label="Region"><Input v-model="storage.region" class="font-mono" /></Field>
              </template>
              <p v-if="storage.test" class="text-xs text-muted-foreground">{{ storage.test }}</p>
            </CardContent>
            <CardFooter class="gap-2">
              <Button v-if="storage.mode === 'slate'" variant="outline" size="sm" @click="testS3">Test connection</Button>
              <Button size="sm" @click="saveStorage">Save storage</Button>
              <ActionMessage :error="storageStatus.error" :ok="storageStatus.ok" />
            </CardFooter>
          </Card>
        </template>
      </div>
    </div>
  </div>

  <Dialog v-model:open="createOpen">
    <DialogContent>
      <DialogHeader>
        <DialogTitle>Create cell</DialogTitle>
        <DialogDescription>
          Register another region’s controller. This does not start VMs or join Raft.
        </DialogDescription>
      </DialogHeader>
      <div class="grid gap-3">
        <Field label="ID"><Input v-model="cellForm.id" placeholder="eu-west" /></Field>
        <Field label="Region"><Input v-model="cellForm.region" placeholder="eu-west" /></Field>
        <Field label="Controller URL">
          <Input v-model="cellForm.controllerUrl" class="font-mono text-xs" placeholder="http://10.0.0.9:8090" />
        </Field>
        <Field label="Label"><Input v-model="cellForm.label" placeholder="EU" /></Field>
      </div>
      <DialogFooter>
        <ActionMessage :error="cellStatus.error" :ok="cellStatus.ok" />
        <Button variant="outline" size="sm" @click="createOpen = false">Cancel</Button>
        <Button size="sm" @click="createCell">Register</Button>
      </DialogFooter>
    </DialogContent>
  </Dialog>

  <Sheet v-model:open="attachOpen">
    <SheetContent class="overflow-y-auto sm:max-w-md">
      <SheetHeader>
        <SheetTitle>Add node</SheetTitle>
        <SheetDescription>
          Start the new broker, then paste the URL this panel can open.
        </SheetDescription>
      </SheetHeader>
      <div class="grid gap-3 px-4">
        <Field label="Role"><AppSelect v-model="attach.profile" :options="profileOptions" /></Field>
        <Field label="Name"><Input v-model="attach.name" placeholder="broker2" /></Field>
        <Field label="This panel connects here">
          <Input v-model="attach.reach" class="font-mono text-xs" placeholder="http://127.0.0.1:8082" />
        </Field>
        <div class="flex items-start gap-2">
          <Checkbox id="attach-split" v-model="attach.split" />
          <Label for="attach-split" class="font-normal leading-5 text-foreground">
            Other brokers cannot use that URL
          </Label>
        </div>
        <Field v-if="attach.split" label="Other brokers connect here" hint="Compose DNS, not localhost.">
          <Input v-model="attach.advertise" class="font-mono text-xs" placeholder="http://broker2:8080" />
        </Field>
        <Field v-if="needsToken" label="Join token">
          <Input v-model="attach.token" class="font-mono" placeholder="from Create cluster" />
        </Field>
        <p class="text-xs text-muted-foreground">{{ profileHint }}</p>
        <ActionMessage :error="attach.error" :ok="attachOk" />
      </div>
      <SheetFooter>
        <Button variant="outline" size="sm" @click="probe">Test connection</Button>
        <Button size="sm" @click="attachNode">Add node</Button>
      </SheetFooter>
    </SheetContent>
  </Sheet>
</template>
