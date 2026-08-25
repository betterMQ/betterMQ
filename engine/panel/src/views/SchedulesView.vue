<script setup lang="ts">
import { computed, onMounted, reactive, ref } from "vue";
import { RouterLink } from "vue-router";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardFooter, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import Field from "@/components/form/Field.vue";
import ActionMessage from "@/components/form/ActionMessage.vue";
import AppSelect from "@/components/form/AppSelect.vue";
import RetryFields from "@/components/form/RetryFields.vue";
import { useActionStatus } from "@/composables/useActionStatus";
import { apiDelete, apiGet, apiPost, publicUrl } from "@/lib/api";
import { catalog, flowId, refreshFlows } from "@/lib/catalog";
import { defaultRetry, mergeOutbound, mergeRetry, parseBody } from "@/lib/payload";

type Delayed = {
  schedule_id?: string;
  id?: string;
  url?: string;
  queue?: string;
  key?: string;
  run_at_ms?: number;
  deliver_at_ms?: number;
};
type Cron = {
  cron_id?: string;
  id?: string;
  url?: string;
  destination_url?: string;
  queue?: string;
  cron?: string;
  every_seconds?: number;
  paused?: boolean;
  next_run_ms?: number;
  next_run_at_ms?: number;
};

const form = reactive({
  type: "cron",
  key: "cron-1",
  url: "https://webhook.site",
  secret: "whsec_demo",
  priority: 5,
  expr: "*/1 * * * *",
  every: 10,
  flowId: "none",
  body: '{"tick":true}',
  retry: defaultRetry(),
});

const flowOptions = computed(() => [
  { value: "none", label: "— none —" },
  ...catalog.flows.map((f) => ({
    value: flowId(f),
    label: `${f.key || flowId(f)} · ${f.parallelism ?? 1} in-flight · ${f.rate ?? 0}/${f.period_secs ?? 60}s`,
  })),
]);
const selectedFlow = computed(() => catalog.flows.find((f) => flowId(f) === form.flowId) || null);

const delayed = ref<Delayed[]>([]);
const crons = ref<Cron[]>([]);
const loadStatus = useActionStatus();
const createStatus = useActionStatus();
const listStatus = useActionStatus();

onMounted(() => {
  void load();
});

async function load() {
  loadStatus.reset();
  try {
    const [d, c] = await Promise.all([
      apiGet<{ delayed?: Delayed[] }>(publicUrl("/v1/delayed")),
      apiGet<{ crons?: Cron[] }>(publicUrl("/v1/crons")),
      refreshFlows(),
    ]);
    delayed.value = d.delayed || [];
    crons.value = c.crons || [];
  } catch (e) {
    loadStatus.fail(e);
  }
}

async function create() {
  createStatus.reset();
  if (!form.url.trim() || !form.secret.trim()) {
    createStatus.fail("Schedule needs destination URL and secret");
    return;
  }
  const payload: Record<string, unknown> = {
    url: form.url.trim(),
    secret: form.secret.trim(),
    key: form.key,
    body: parseBody(form.body),
    priority: Number(form.priority) || 5,
  };
  if (form.type === "interval") payload.every_seconds = Number(form.every) || 10;
  else payload.cron = form.expr;
  if (form.flowId && form.flowId !== "none") payload.flow_id = form.flowId;
  mergeRetry(payload, form.retry);
  const headersError = mergeOutbound(payload);
  if (headersError) {
    createStatus.fail(headersError);
    return;
  }
  try {
    const r = await apiPost<{ cron_id?: string }>(publicUrl("/v1/crons"), payload);
    createStatus.succeed(`Schedule created ${r.cron_id}`);
    await load();
  } catch (e) {
    createStatus.fail(e);
  }
}

async function act(kind: string, id: string) {
  listStatus.reset();
  try {
    if (kind === "delayed") await apiDelete(publicUrl(`/v1/delayed/${id}`));
    else if (kind === "cron-pause") await apiPost(publicUrl(`/v1/crons/${id}/pause`));
    else if (kind === "cron-resume") await apiPost(publicUrl(`/v1/crons/${id}/resume`));
    else if (kind === "cron-delete") await apiDelete(publicUrl(`/v1/crons/${id}`));
    listStatus.succeed(`${kind} ok`);
    await load();
  } catch (e) {
    listStatus.fail(e);
  }
}

function fmt(ms?: number) {
  if (!ms) return "—";
  return new Date(ms).toLocaleString();
}
</script>

<template>
  <div class="space-y-4">
    <div>
      <h1 class="text-lg font-semibold tracking-tight">Schedules</h1>
      <p class="text-sm text-muted-foreground">
        Recurring jobs — cron (UTC unless you prefix CRON_TZ=) or a fixed interval, delivered as a direct webhook.
      </p>
      <code class="mt-1 block font-mono text-xs text-muted-foreground">POST /v1/crons</code>
    </div>
    <Card>
      <CardContent class="grid gap-3 pt-6 sm:grid-cols-2">
        <Field label="Type">
          <AppSelect
            v-model="form.type"
            :options="[
              { value: 'cron', label: 'Cron' },
              { value: 'interval', label: 'Interval' },
            ]"
          />
        </Field>
        <Field label="Routing key"><Input v-model="form.key" /></Field>
        <Field label="Destination URL" class="sm:col-span-2">
          <Input v-model="form.url" class="font-mono text-xs" />
        </Field>
        <Field label="Secret"><Input v-model="form.secret" class="font-mono" /></Field>
        <Field label="Priority"><Input v-model="form.priority" type="number" /></Field>
        <Field
          v-if="form.type === 'cron'"
          label="Cron"
          hint="UTC by default. Prefix CRON_TZ=America/New_York for local time."
        >
          <Input v-model="form.expr" class="font-mono" placeholder="*/5 * * * *" />
        </Field>
        <Field v-else label="Every (sec)"><Input v-model="form.every" type="number" /></Field>
        <Field
          label="Flow profile (optional)"
          :hint="selectedFlow?.key ? `Lane ${selectedFlow.key}` : 'Saved rate + parallelism from Flows.'"
        >
          <AppSelect v-model="form.flowId" :options="flowOptions" placeholder="— none —" />
        </Field>
        <p v-if="catalog.flows.length === 0" class="text-xs text-muted-foreground">
          No profiles yet.
          <RouterLink to="/flows" class="text-primary underline-offset-4 hover:underline">Create one on Flows</RouterLink>
        </p>
        <div class="sm:col-span-2"><RetryFields v-model="form.retry" /></div>
        <Field label="Body" class="sm:col-span-2">
          <Textarea v-model="form.body" class="font-mono text-xs" rows="3" />
        </Field>
      </CardContent>
      <CardFooter>
        <Button size="sm" @click="create">Create schedule</Button>
        <ActionMessage :error="createStatus.error" :ok="createStatus.ok" />
      </CardFooter>
    </Card>
    <Card>
      <CardHeader><CardTitle>Jobs</CardTitle></CardHeader>
      <CardContent>
        <ActionMessage class="mb-2" :error="listStatus.error || loadStatus.error" :ok="listStatus.ok" />
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Type</TableHead>
              <TableHead>ID</TableHead>
              <TableHead>Target</TableHead>
              <TableHead>Spec</TableHead>
              <TableHead>Status</TableHead>
              <TableHead>Next</TableHead>
              <TableHead />
            </TableRow>
          </TableHeader>
          <TableBody>
            <TableRow v-for="d in delayed" :key="d.schedule_id || d.id">
              <TableCell><Badge variant="outline">delayed</Badge></TableCell>
              <TableCell class="font-mono text-xs">{{ d.schedule_id || d.id }}</TableCell>
              <TableCell class="max-w-[12rem] truncate font-mono text-xs">{{ d.url || d.queue }}</TableCell>
              <TableCell>{{ d.key }}</TableCell>
              <TableCell>—</TableCell>
              <TableCell>{{ fmt(d.deliver_at_ms ?? d.run_at_ms) }}</TableCell>
              <TableCell>
                <Button size="sm" variant="destructive" @click="act('delayed', d.schedule_id || d.id || '')">Cancel</Button>
              </TableCell>
            </TableRow>
            <TableRow v-for="c in crons" :key="c.cron_id || c.id">
              <TableCell><Badge variant="secondary">cron</Badge></TableCell>
              <TableCell class="font-mono text-xs">{{ c.cron_id || c.id }}</TableCell>
              <TableCell class="max-w-[12rem] truncate font-mono text-xs">{{ c.destination_url || c.url || c.queue }}</TableCell>
              <TableCell class="font-mono text-xs">{{ c.cron || (c.every_seconds ? `every ${c.every_seconds}s` : "") }}</TableCell>
              <TableCell>{{ c.paused ? "paused" : "active" }}</TableCell>
              <TableCell>{{ fmt(c.next_run_at_ms ?? c.next_run_ms) }}</TableCell>
              <TableCell class="space-x-1">
                <Button
                  size="sm"
                  :variant="c.paused ? 'success' : 'warning'"
                  @click="act(c.paused ? 'cron-resume' : 'cron-pause', c.cron_id || c.id || '')"
                >
                  {{ c.paused ? "Resume" : "Pause" }}
                </Button>
                <Button size="sm" variant="destructive" @click="act('cron-delete', c.cron_id || c.id || '')">Delete</Button>
              </TableCell>
            </TableRow>
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  </div>
</template>
