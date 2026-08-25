<script setup lang="ts">
import { onMounted, reactive } from "vue";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardFooter, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import Field from "@/components/form/Field.vue";
import ActionMessage from "@/components/form/ActionMessage.vue";
import { useActionStatus } from "@/composables/useActionStatus";
import { apiDelete, apiPost, publicUrl } from "@/lib/api";
import { catalog, flowId, refreshFlows, type FlowRow } from "@/lib/catalog";

const form = reactive({ key: "user-1", parallelism: 1, rate: 100, period: 60 });
const createStatus = useActionStatus();
const listStatus = useActionStatus();

onMounted(() => {
  void refreshFlows();
});

async function create() {
  createStatus.reset();
  const key = form.key;
  const parallelism = Number(form.parallelism) || 1;
  const rate = Number(form.rate) || 0;
  const period_secs = Number(form.period) || 60;
  const existing = catalog.flows.find(
    (f) =>
      f.key === key &&
      Number(f.parallelism) === parallelism &&
      Number(f.rate) === rate &&
      Number(f.period_secs) === period_secs,
  );
  if (existing) {
    createStatus.fail(`duplicate flow ${flowId(existing)}`);
    return;
  }
  try {
    const r = await apiPost<{ flow_id?: string }>(publicUrl("/v1/flows"), {
      key,
      parallelism,
      rate,
      period_secs,
    });
    createStatus.succeed(`Flow created ${r.flow_id}`);
    await refreshFlows();
  } catch (e) {
    createStatus.fail(e);
  }
}

async function remove(f: FlowRow) {
  listStatus.reset();
  try {
    await apiDelete(publicUrl(`/v1/flows/${encodeURIComponent(flowId(f))}`));
    listStatus.succeed("Flow removed");
    await refreshFlows();
  } catch (e) {
    listStatus.fail(e);
  }
}
</script>

<template>
  <div class="space-y-4">
    <div>
      <h1 class="text-lg font-semibold tracking-tight">Flows</h1>
      <p class="text-sm text-muted-foreground">
        Named rate + parallelism limits for Publish (and groups). Queue jobs use the queue’s parallelism or a message key.
      </p>
      <code class="mt-1 block font-mono text-xs text-muted-foreground">POST /v1/flows</code>
    </div>
    <Card>
      <CardContent class="grid gap-3 pt-6 sm:grid-cols-4">
        <Field label="Lane key" hint="Usually the publish routing key">
          <Input v-model="form.key" />
        </Field>
        <Field label="Parallelism"><Input v-model="form.parallelism" type="number" min="1" /></Field>
        <Field label="Rate"><Input v-model="form.rate" type="number" /></Field>
        <Field label="Period (sec)"><Input v-model="form.period" type="number" /></Field>
      </CardContent>
      <CardFooter>
        <Button size="sm" @click="create">Create profile</Button>
        <ActionMessage :error="createStatus.error" :ok="createStatus.ok" />
      </CardFooter>
    </Card>
    <Card>
          <CardHeader><CardTitle>Profiles</CardTitle></CardHeader>
          <CardContent>
            <ActionMessage class="mb-2" :error="listStatus.error" :ok="listStatus.ok" />
            <Table>
          <TableHeader>
            <TableRow>
              <TableHead>ID</TableHead>
              <TableHead>Lane</TableHead>
              <TableHead>Par.</TableHead>
              <TableHead>Rate</TableHead>
              <TableHead>Period</TableHead>
              <TableHead />
            </TableRow>
          </TableHeader>
          <TableBody>
            <TableRow v-for="f in catalog.flows" :key="flowId(f)">
              <TableCell class="font-mono text-xs">{{ flowId(f) }}</TableCell>
              <TableCell>{{ f.key }}</TableCell>
              <TableCell>{{ f.parallelism }}</TableCell>
              <TableCell>{{ f.rate }}</TableCell>
              <TableCell>{{ f.period_secs }}</TableCell>
              <TableCell><Button size="sm" variant="destructive" @click="remove(f)">Delete</Button></TableCell>
            </TableRow>
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  </div>
</template>
