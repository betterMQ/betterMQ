<script setup lang="ts">
import { onMounted, reactive } from "vue";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import Field from "@/components/form/Field.vue";
import ActionMessage from "@/components/form/ActionMessage.vue";
import AppSelect from "@/components/form/AppSelect.vue";
import { useActionStatus } from "@/composables/useActionStatus";
import { apiPost, publicUrl } from "@/lib/api";
import { catalog, queueId, refreshQueues } from "@/lib/catalog";
import { mergeOutbound, parseBody } from "@/lib/payload";

const form = reactive({
  queueId: "",
  key: "",
  priority: 5,
  delay: "",
  idempotency: "",
  body: '{"hello":"world"}',
});

const status = useActionStatus();

onMounted(() => {
  void refreshQueues();
});

async function enqueue() {
  status.reset();
  if (!form.queueId) {
    status.fail("Select a queue first");
    return;
  }
  const payload: Record<string, unknown> = {
    body: parseBody(form.body),
    priority: Number(form.priority) || 5,
  };
  const key = form.key.trim();
  if (key) payload.key = key;
  if (form.idempotency.trim()) payload.idempotency_key = form.idempotency.trim();
  const delay = parseInt(String(form.delay), 10);
  if (delay > 0) payload.delay = delay;
  const headersError = mergeOutbound(payload);
  if (headersError) {
    status.fail(headersError);
    return;
  }
  try {
    const r = await apiPost<{ message_id?: string; scheduled?: { schedule_id?: string } }>(
      publicUrl(`/v1/queues/${encodeURIComponent(form.queueId)}/enqueue`),
      payload,
    );
    status.succeed(`${delay > 0 ? "Delayed " : "Enqueued "}${r.message_id || r.scheduled?.schedule_id || "?"}`);
  } catch (e) {
    status.fail(e);
  }
}
</script>

<template>
  <div class="space-y-4">
    <div>
      <h1 class="text-lg font-semibold tracking-tight">Enqueue</h1>
      <p class="text-sm text-muted-foreground">
        Add a job to a queue. No key = standard (queue parallelism). A key = FIFO for that entity.
      </p>
      <code class="mt-1 block font-mono text-xs text-muted-foreground">POST /v1/queues/{id}/enqueue</code>
    </div>
    <Alert>
      <AlertDescription>
        Retries and default concurrency come from the queue. Pass a routing key only when this entity must stay in order
        (for example <code>user-42</code>).
      </AlertDescription>
    </Alert>
    <Card>
      <CardHeader>
        <CardTitle>Queue</CardTitle>
        <CardDescription>Frozen webhook for this job</CardDescription>
      </CardHeader>
      <CardContent>
        <Field label="Queue">
          <AppSelect
            v-model="form.queueId"
            :options="catalog.queues.map((q) => ({ value: queueId(q), label: q.queue || q.name || queueId(q) }))"
            placeholder="— create a queue first —"
          />
        </Field>
      </CardContent>
    </Card>
    <Card>
      <CardHeader>
        <CardTitle>Message</CardTitle>
        <CardDescription>Payload and delivery options</CardDescription>
      </CardHeader>
      <CardContent class="grid gap-3 sm:grid-cols-2">
        <Field
          label="Key"
          hint="Optional. Empty = standard. Set to FIFO this entity (other keys still run in parallel)."
        >
          <Input v-model="form.key" placeholder="user-42" />
        </Field>
        <Field label="Priority"><Input v-model="form.priority" type="number" /></Field>
        <Field label="Delay (ms)"><Input v-model="form.delay" type="number" /></Field>
        <Field label="Idempotency"><Input v-model="form.idempotency" /></Field>
        <Field label="Body" class="sm:col-span-2">
          <Textarea v-model="form.body" class="font-mono text-xs" rows="4" />
        </Field>
      </CardContent>
    </Card>
    <div class="flex items-center gap-2">
      <Button size="sm" @click="enqueue">Enqueue</Button>
      <ActionMessage :error="status.error" :ok="status.ok" />
    </div>
  </div>
</template>
