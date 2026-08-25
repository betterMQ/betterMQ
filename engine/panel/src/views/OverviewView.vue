<script setup lang="ts">
import { onMounted, reactive } from "vue";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import Field from "@/components/form/Field.vue";
import ActionMessage from "@/components/form/ActionMessage.vue";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { useActionStatus } from "@/composables/useActionStatus";
import { apiPost, publicUrl } from "@/lib/api";
import { catalog, refreshFlows, refreshGroups, refreshQueues } from "@/lib/catalog";
import { mergeOutbound, parseBody } from "@/lib/payload";
import { pollCluster, pollHealth, session } from "@/lib/session";

const publishStatus = useActionStatus();

const kpis = reactive({
  queues: "—",
  groups: "—",
  flows: "—",
  schedules: "—",
  dlq: "—",
});

const pub = reactive({
  url: "https://webhook.site",
  secret: "whsec_demo",
  body: '{"from":"dashboard"}',
});

onMounted(async () => {
  await Promise.all([pollHealth(), pollCluster(), refreshQueues(), refreshGroups(), refreshFlows()]);
  kpis.queues = String(catalog.queues.length);
  kpis.groups = String(catalog.groups.length);
  kpis.flows = String(catalog.flows.length);
  try {
    const [delayed, crons] = await Promise.all([
      fetchJson("/v1/delayed"),
      fetchJson("/v1/crons"),
    ]);
    kpis.schedules = String(((delayed.delayed as unknown[]) || []).length + ((crons.crons as unknown[]) || []).length);
  } catch {
    kpis.schedules = "—";
  }
  try {
    const sources = (await fetchJson("/v1/dlq/sources")) as { count?: number }[] | { sources?: { count?: number }[] };
    const list = Array.isArray(sources) ? sources : sources.sources || [];
    kpis.dlq = String(list.reduce((s, x) => s + (x.count || 0), 0));
  } catch {
    kpis.dlq = "—";
  }
});

async function fetchJson(path: string) {
  const { apiGet } = await import("@/lib/api");
  return apiGet<Record<string, unknown>>(publicUrl(path));
}

async function quickPublish() {
  const payload: Record<string, unknown> = {
    url: pub.url,
    secret: pub.secret,
    key: `dashboard-${Date.now()}`,
    body: parseBody(pub.body),
    priority: 5,
  };
  const headersError = mergeOutbound(payload);
  if (headersError) {
    publishStatus.fail(headersError);
    return;
  }
  try {
    const r = await apiPost<{ message_id?: string }>(publicUrl("/v1/publish"), payload);
    publishStatus.succeed(`Published ${r.message_id || "ok"}`);
  } catch (e) {
    publishStatus.fail(e);
  }
}

const clusterNodes = () => session.cluster?.nodes || [];
</script>

<template>
  <div class="space-y-4">
    <div>
      <h1 class="text-lg font-semibold tracking-tight">Overview</h1>
      <p class="text-sm text-muted-foreground">Broker health and resource counts</p>
    </div>
    <div class="grid gap-2 sm:grid-cols-2 lg:grid-cols-5">
      <Card size="sm">
        <CardHeader class="pb-1">
          <CardDescription>Status</CardDescription>
          <CardTitle class="font-mono text-lg">{{ session.health }}</CardTitle>
        </CardHeader>
      </Card>
      <Card size="sm">
        <CardHeader class="pb-1">
          <CardDescription>Queues</CardDescription>
          <CardTitle class="text-lg">{{ kpis.queues }}</CardTitle>
        </CardHeader>
      </Card>
      <Card size="sm">
        <CardHeader class="pb-1">
          <CardDescription>Groups</CardDescription>
          <CardTitle class="text-lg">{{ kpis.groups }}</CardTitle>
        </CardHeader>
      </Card>
      <Card size="sm">
        <CardHeader class="pb-1">
          <CardDescription>Schedules</CardDescription>
          <CardTitle class="text-lg">{{ kpis.schedules }}</CardTitle>
        </CardHeader>
      </Card>
      <Card size="sm">
        <CardHeader class="pb-1">
          <CardDescription>DLQ</CardDescription>
          <CardTitle class="text-lg">{{ kpis.dlq }}</CardTitle>
        </CardHeader>
      </Card>
    </div>

    <Card v-if="session.cluster?.enabled || clusterNodes().length > 1">
      <CardHeader>
        <CardTitle>Cluster</CardTitle>
        <CardDescription>{{ session.clusterLabel }} · compact summary</CardDescription>
      </CardHeader>
      <CardContent>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Node</TableHead>
              <TableHead>Health</TableHead>
              <TableHead>Led shards</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            <TableRow v-for="n in clusterNodes()" :key="n.id || n.addr">
              <TableCell class="font-mono text-xs">{{ n.name || n.addr }}</TableCell>
              <TableCell>{{ n.healthy === false ? "down" : "ok" }}</TableCell>
              <TableCell class="font-mono text-xs">{{ (n.led_shards || []).join(", ") || "—" }}</TableCell>
            </TableRow>
          </TableBody>
        </Table>
      </CardContent>
    </Card>

    <div class="grid gap-3 lg:grid-cols-2">
      <Card>
        <CardHeader>
          <CardTitle>Delivery model</CardTitle>
          <CardDescription>How messages leave BetterMQ</CardDescription>
        </CardHeader>
        <CardContent class="space-y-1.5 text-sm">
          <p><strong>Publish</strong> — one-off webhook.</p>
          <p><strong>Enqueue</strong> — named queue. No key = standard; a key = FIFO for that entity.</p>
          <p><strong>Groups</strong> — fan-out to member webhooks.</p>
          <p><strong>DLQ</strong> — failed deliveries after retries.</p>
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>Quick publish</CardTitle>
          <CardDescription>One direct webhook from the console</CardDescription>
        </CardHeader>
        <CardContent class="grid gap-2">
          <Field label="URL"><Input v-model="pub.url" class="font-mono text-xs" /></Field>
          <Field label="Secret"><Input v-model="pub.secret" class="font-mono" /></Field>
          <Field label="Body"><Input v-model="pub.body" class="font-mono text-xs" /></Field>
        </CardContent>
        <CardFooter>
          <Button size="sm" @click="quickPublish">Publish</Button>
          <ActionMessage :error="publishStatus.error" :ok="publishStatus.ok" />
        </CardFooter>
      </Card>
    </div>
  </div>
</template>
