<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { Alert, AlertDescription } from "@/components/ui/alert";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import Field from "@/components/form/Field.vue";
import ActionMessage from "@/components/form/ActionMessage.vue";
import AppSelect from "@/components/form/AppSelect.vue";
import { useActionStatus } from "@/composables/useActionStatus";
import { apiDelete, apiGet, publicUrl } from "@/lib/api";
import { copyText } from "@/lib/payload";

type Source = {
  dlq_topic: string;
  kind?: string;
  label?: string;
  count?: number;
  queue?: string;
};
type Msg = {
  message_id: string;
  source_queue?: string;
  destination_url?: string;
  reason?: string;
  published_at_ms?: number;
  partition: number;
  offset: number;
  body?: unknown;
};

const sources = ref<Source[]>([]);
const sourceId = ref("");
const limit = ref(20);
const rows = ref<Msg[]>([]);
const topic = ref("");
const bodyOpen = ref(false);
const bodyText = ref("");
const bodyTitle = ref("");
const loadStatus = useActionStatus();
const copyStatus = useActionStatus();
const deleteAllOpen = ref(false);

onMounted(() => {
  void loadSources();
});

const options = computed(() =>
  sources.value.map((s) => ({
    value: s.dlq_topic,
    label: `${s.label || s.dlq_topic} (${s.count || 0})`,
  })),
);

async function loadSources() {
  loadStatus.reset();
  try {
    const data = await apiGet<Source[] | { sources?: Source[] }>(publicUrl("/v1/dlq/sources"));
    sources.value = Array.isArray(data) ? data : data.sources || [];
    if (!sourceId.value || !sources.value.some((s) => s.dlq_topic === sourceId.value)) {
      sourceId.value = sources.value[0]?.dlq_topic || "";
    }
    if (sourceId.value) {
      await load();
    } else {
      rows.value = [];
      topic.value = "";
    }
  } catch (e) {
    loadStatus.fail(e);
  }
}

function selectedSource() {
  return sources.value.find((s) => s.dlq_topic === sourceId.value);
}

async function load() {
  loadStatus.reset();
  const src = selectedSource();
  const dlqTopic = src?.dlq_topic || sourceId.value;
  if (!dlqTopic) {
    rows.value = [];
    topic.value = "";
    loadStatus.fail("Select a DLQ source");
    return;
  }
  const qs = new URLSearchParams();
  if (src?.queue) qs.set("queue", src.queue);
  else qs.set("dlq_topic", dlqTopic);
  qs.set("limit", String(limit.value || 20));
  try {
    const data = await apiGet<{ messages?: Msg[]; dlq_topic?: string }>(publicUrl(`/v1/dlq?${qs}`));
    rows.value = data.messages || [];
    topic.value = data.dlq_topic || dlqTopic;
  } catch (e) {
    loadStatus.fail(e);
  }
}

async function remove(m: Msg) {
  const dlqTopic = topic.value || selectedSource()?.dlq_topic || sourceId.value;
  if (!dlqTopic) {
    loadStatus.fail("Select a DLQ source");
    return;
  }
  const qs = new URLSearchParams({
    dlq_topic: dlqTopic,
    partition: String(m.partition),
    offset: String(m.offset),
  });
  loadStatus.reset();
  try {
    await apiDelete(publicUrl(`/v1/dlq?${qs}`));
    loadStatus.succeed("Deleted");
    await load();
  } catch (e) {
    loadStatus.fail(e);
  }
}

async function removeAll() {
  const dlqTopic = topic.value || selectedSource()?.dlq_topic || sourceId.value;
  if (!dlqTopic) {
    loadStatus.fail("Select a DLQ source");
    return;
  }
  loadStatus.reset();
  try {
    const data = await apiDelete<{ deleted?: number }>(
      publicUrl(`/v1/dlq?${new URLSearchParams({ dlq_topic: dlqTopic })}`),
    );
    deleteAllOpen.value = false;
    loadStatus.succeed(`Deleted ${data.deleted ?? 0}`);
    await loadSources();
  } catch (e) {
    loadStatus.fail(e);
  }
}

async function copyBody() {
  copyStatus.reset();
  try {
    await copyText(bodyText.value);
    copyStatus.succeed("Copied");
  } catch {
    copyStatus.fail("Copy failed");
  }
}

function view(m: Msg) {
  bodyTitle.value = `DLQ · ${m.message_id}`;
  try {
    bodyText.value = JSON.stringify(typeof m.body === "string" ? JSON.parse(m.body) : m.body, null, 2);
  } catch {
    bodyText.value = String(m.body ?? "");
  }
  bodyOpen.value = true;
}

function fmt(ms?: number) {
  if (!ms) return "—";
  return new Date(ms).toLocaleString();
}
</script>

<template>
  <div class="space-y-4">
    <div>
      <h1 class="text-lg font-semibold tracking-tight">Dead letter queue</h1>
      <p class="text-sm text-muted-foreground">
        Failed deliveries land in a per-source DLQ. Auto-expire with
        <code class="font-mono">BETTERMQ_DLQ_RETENTION_DAYS</code> (unset = keep forever).
      </p>
      <code class="mt-1 block font-mono text-xs text-muted-foreground">GET /v1/dlq</code>
    </div>
    <Alert>
      <AlertDescription>
        Queue enqueue → jobs.__dlq · Direct publish → __direct.__dlq · Group fan-out → one DLQ per member.
      </AlertDescription>
    </Alert>
    <Card>
      <CardContent class="flex flex-wrap items-end gap-3 pt-6">
        <Field label="Source" class="min-w-64 flex-1">
          <AppSelect
            v-model="sourceId"
            :options="options"
            placeholder="Select a source"
            @update:model-value="load"
          />
        </Field>
        <Field label="DLQ topic">
          <Input :model-value="topic" readonly class="font-mono text-xs" />
        </Field>
        <Field label="Limit"><Input v-model="limit" type="number" min="1" /></Field>
        <Button variant="outline" size="sm" :disabled="!sourceId" @click="load">Load</Button>
        <Button variant="destructive" size="sm" :disabled="!sourceId" @click="deleteAllOpen = true">Delete all</Button>
        <ActionMessage :error="loadStatus.error" :ok="loadStatus.ok" />
      </CardContent>
    </Card>
    <Card>
      <CardHeader><CardTitle>Messages</CardTitle></CardHeader>
      <CardContent>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Message</TableHead>
              <TableHead>Source</TableHead>
              <TableHead>Destination</TableHead>
              <TableHead>Reason</TableHead>
              <TableHead>Published</TableHead>
              <TableHead />
            </TableRow>
          </TableHeader>
          <TableBody>
            <TableRow v-if="!rows.length">
              <TableCell colspan="6" class="text-sm text-muted-foreground">
                {{ sourceId ? "No dead letters for this source." : "Select a source to load messages." }}
              </TableCell>
            </TableRow>
            <TableRow v-for="m in rows" :key="m.message_id">
              <TableCell class="font-mono text-xs">{{ m.message_id }}</TableCell>
              <TableCell>{{ m.source_queue || "—" }}</TableCell>
              <TableCell class="max-w-[12rem] truncate font-mono text-xs">{{ m.destination_url }}</TableCell>
              <TableCell class="max-w-[12rem] truncate">{{ m.reason }}</TableCell>
              <TableCell>{{ fmt(m.published_at_ms) }}</TableCell>
              <TableCell class="space-x-1">
                <Button size="sm" variant="outline" @click="view(m)">View</Button>
                <Button size="sm" variant="destructive" @click="remove(m)">Delete</Button>
              </TableCell>
            </TableRow>
          </TableBody>
        </Table>
      </CardContent>
    </Card>
    <AlertDialog :open="deleteAllOpen" @update:open="deleteAllOpen = $event">
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>Delete all dead letters?</AlertDialogTitle>
          <AlertDialogDescription>
            This removes every message on {{ topic || sourceId || "this DLQ" }}. It cannot be undone.
          </AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel>Cancel</AlertDialogCancel>
          <AlertDialogAction variant="destructive" @click="removeAll">Delete all</AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
    <Dialog v-model:open="bodyOpen">
      <DialogContent class="max-w-2xl">
        <DialogHeader><DialogTitle>{{ bodyTitle }}</DialogTitle></DialogHeader>
        <pre class="max-h-96 overflow-auto border bg-muted p-3 font-mono text-xs">{{ bodyText }}</pre>
        <DialogFooter>
          <ActionMessage :error="copyStatus.error" :ok="copyStatus.ok" />
          <Button variant="outline" size="sm" @click="copyBody">Copy</Button>
          <Button size="sm" @click="bodyOpen = false">Close</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  </div>
</template>
