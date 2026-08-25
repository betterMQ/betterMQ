<script setup lang="ts">
import { onMounted, ref } from "vue";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import Field from "@/components/form/Field.vue";
import ActionMessage from "@/components/form/ActionMessage.vue";
import AppSelect from "@/components/form/AppSelect.vue";
import { useActionStatus } from "@/composables/useActionStatus";
import { apiGet, apiPost, publicUrl } from "@/lib/api";
import { outbound } from "@/lib/payload";

type Host = { host: string; remaining_ms?: number; remaining?: number };

const host = ref("");
const minutes = ref(30);
const blocked = ref<Host[]>([]);
const loadStatus = useActionStatus();
const blockStatus = useActionStatus();

onMounted(() => {
  void load();
});

async function load() {
  loadStatus.reset();
  try {
    const d = await apiGet<{ hosts?: Host[] } | Host[]>(publicUrl("/v1/destinations/blocked"));
    blocked.value = Array.isArray(d) ? d : d.hosts || [];
  } catch (e) {
    loadStatus.fail(e);
  }
}

async function block() {
  blockStatus.reset();
  const h = host.value.trim();
  if (!h) {
    blockStatus.fail("Host or URL required");
    return;
  }
  try {
    const res = await apiPost<{ host?: string }>(publicUrl("/v1/destinations/block"), {
      host: h,
      duration_ms: (Number(minutes.value) || 30) * 60 * 1000,
    });
    blockStatus.succeed(`Blocked ${res.host || h} for ${minutes.value}m`);
    host.value = "";
    await load();
  } catch (e) {
    blockStatus.fail(e);
  }
}

async function unblock(h: string) {
  blockStatus.reset();
  try {
    await apiPost(publicUrl("/v1/destinations/unblock"), { host: h });
    blockStatus.succeed(`Unblocked ${h}`);
    await load();
  } catch (e) {
    blockStatus.fail(e);
  }
}

function remaining(h: Host) {
  const ms = h.remaining_ms ?? h.remaining ?? 0;
  if (!ms) return "—";
  if (ms < 60_000) return `${Math.ceil(ms / 1000)}s`;
  if (ms < 3_600_000) return `${Math.ceil(ms / 60_000)}m`;
  return `${(ms / 3_600_000).toFixed(1)}h`;
}
</script>

<template>
  <div class="space-y-4">
    <div>
      <h1 class="text-lg font-semibold tracking-tight">HTTP</h1>
      <p class="text-sm text-muted-foreground">Outbound webhook defaults and delivery circuit-breaker controls.</p>
    </div>
    <Card>
      <CardHeader>
        <CardTitle>Outbound defaults</CardTitle>
        <CardDescription>Applied to publish & enqueue from the panel</CardDescription>
      </CardHeader>
      <CardContent class="grid gap-3 sm:grid-cols-2">
        <Field label="Method">
          <AppSelect
            v-model="outbound.method"
            :options="['POST', 'GET', 'PUT', 'PATCH', 'DELETE'].map((m) => ({ value: m, label: m }))"
          />
        </Field>
        <div class="flex items-center gap-2 self-end">
          <Checkbox id="hmac-sign" v-model="outbound.sign" />
          <Label for="hmac-sign" class="font-normal text-foreground">HMAC signature</Label>
        </div>
        <Field label="Headers (JSON)" class="sm:col-span-2">
          <Textarea v-model="outbound.headers" class="font-mono text-xs" rows="5" placeholder='{"Authorization":"Bearer …"}' />
        </Field>
      </CardContent>
    </Card>
    <Card>
      <CardHeader>
        <CardTitle>Blocked destinations</CardTitle>
        <CardDescription>Hosts paused after transport failures</CardDescription>
      </CardHeader>
      <CardContent class="space-y-4">
        <div class="flex flex-wrap items-end gap-3">
          <Field label="Host or URL" class="min-w-64 flex-1">
            <Input v-model="host" class="font-mono text-xs" placeholder="https://api.example.com" />
          </Field>
          <Field label="Minutes"><Input v-model="minutes" type="number" min="1" /></Field>
          <Button variant="warning" size="sm" @click="block">Block</Button>
          <Button variant="ghost" size="sm" @click="load">Refresh</Button>
          <ActionMessage :error="blockStatus.error || loadStatus.error" :ok="blockStatus.ok" />
        </div>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Host</TableHead>
              <TableHead>Remaining</TableHead>
              <TableHead />
            </TableRow>
          </TableHeader>
          <TableBody>
            <TableRow v-for="h in blocked" :key="h.host">
              <TableCell class="font-mono text-xs">{{ h.host }}</TableCell>
              <TableCell>{{ remaining(h) }}</TableCell>
              <TableCell><Button size="sm" variant="success" @click="unblock(h.host)">Unblock</Button></TableCell>
            </TableRow>
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  </div>
</template>
