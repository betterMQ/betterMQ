<script setup lang="ts">
import { onMounted, reactive } from "vue";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import Field from "@/components/form/Field.vue";
import ActionMessage from "@/components/form/ActionMessage.vue";
import RetryFields from "@/components/form/RetryFields.vue";
import { useActionStatus } from "@/composables/useActionStatus";
import { apiDelete, apiPost, publicUrl } from "@/lib/api";
import { catalog, queueId, refreshQueues, type QueueRow } from "@/lib/catalog";
import { defaultRetry, mergeRetry } from "@/lib/payload";

const form = reactive({
  queue: "jobs",
  url: "https://webhook.site",
  secret: "whsec_demo",
  parallelism: "",
  retry: defaultRetry(),
});

const createStatus = useActionStatus();
const listStatus = useActionStatus();

onMounted(() => {
  void refreshQueues();
});

async function create() {
  createStatus.reset();
  try {
    const body: Record<string, unknown> = mergeRetry(
      { queue: form.queue, url: form.url, secret: form.secret },
      form.retry,
    );
    const p = parseInt(String(form.parallelism), 10);
    if (form.parallelism.trim() !== "" && Number.isFinite(p)) {
      body.parallelism = Math.max(0, p);
    }
    const r = await apiPost<{ queue?: string; queue_id?: string }>(publicUrl("/v1/queues"), body);
    createStatus.succeed(`Queue ${r.queue} → ${r.queue_id}`);
    await refreshQueues();
  } catch (e) {
    createStatus.fail(e);
  }
}

async function remove(q: QueueRow) {
  listStatus.reset();
  try {
    await apiDelete(publicUrl(`/v1/queues/${encodeURIComponent(queueId(q))}`));
    listStatus.succeed("Queue removed");
    await refreshQueues();
  } catch (e) {
    listStatus.fail(e);
  }
}
</script>

<template>
  <div class="space-y-4">
    <div>
      <h1 class="text-lg font-semibold tracking-tight">Queues</h1>
      <p class="text-sm text-muted-foreground">
        Named webhook. Retries and parallelism live here. Enqueue with no key is standard; a key is FIFO for that entity.
      </p>
      <code class="mt-1 block font-mono text-xs text-muted-foreground">POST /v1/queues</code>
    </div>
    <Card>
      <CardHeader>
        <CardTitle>Create queue</CardTitle>
        <CardDescription>Name, webhook, retries, and how many jobs can run at once</CardDescription>
      </CardHeader>
      <CardContent class="grid gap-2 sm:grid-cols-2">
        <Field label="Name"><Input v-model="form.queue" /></Field>
        <Field label="HMAC secret"><Input v-model="form.secret" class="font-mono" /></Field>
        <Field label="Webhook URL" class="sm:col-span-2"><Input v-model="form.url" class="font-mono text-xs" /></Field>
        <Field
          label="Parallelism"
          hint="Max in-flight without a key. Empty = unlimited. 1 = the whole queue is serial."
        >
          <Input v-model="form.parallelism" type="number" min="0" placeholder="unlimited" />
        </Field>
        <div class="sm:col-span-2"><RetryFields v-model="form.retry" /></div>
      </CardContent>
      <CardFooter>
        <Button size="sm" @click="create">Create</Button>
        <ActionMessage :error="createStatus.error" :ok="createStatus.ok" />
      </CardFooter>
    </Card>
    <Card>
      <CardHeader><CardTitle>Registered</CardTitle></CardHeader>
      <CardContent>
        <ActionMessage class="mb-2" :error="listStatus.error" :ok="listStatus.ok" />
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>ID</TableHead>
              <TableHead>Name</TableHead>
              <TableHead>Parallelism</TableHead>
              <TableHead>URL</TableHead>
              <TableHead />
            </TableRow>
          </TableHeader>
          <TableBody>
            <TableRow v-for="q in catalog.queues" :key="queueId(q)">
              <TableCell class="font-mono text-xs">{{ queueId(q) }}</TableCell>
              <TableCell>{{ q.queue || q.name }}</TableCell>
              <TableCell class="font-mono text-xs">{{ q.parallelism ?? "∞" }}</TableCell>
              <TableCell class="max-w-xs truncate font-mono text-xs">{{ q.url }}</TableCell>
              <TableCell><Button size="sm" variant="destructive" @click="remove(q)">Delete</Button></TableCell>
            </TableRow>
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  </div>
</template>
