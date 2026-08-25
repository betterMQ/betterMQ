<script setup lang="ts">
import { computed, onMounted, reactive } from "vue";
import { RouterLink } from "vue-router";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import Field from "@/components/form/Field.vue";
import ActionMessage from "@/components/form/ActionMessage.vue";
import AppSelect from "@/components/form/AppSelect.vue";
import RetryFields from "@/components/form/RetryFields.vue";
import { useActionStatus } from "@/composables/useActionStatus";
import { apiPost, publicUrl } from "@/lib/api";
import { catalog, flowId, groupId, refreshFlows, refreshGroups } from "@/lib/catalog";
import { defaultRetry, mergeOutbound, mergeRetry, parseBody } from "@/lib/payload";

const form = reactive({
  mode: "url",
  secret: "whsec_demo",
  url: "https://webhook.site",
  groupId: "",
  key: "user-1",
  priority: 5,
  delay: "",
  idempotency: "",
  body: '{"hello":"direct"}',
  pacing: "none",
  flowId: "",
  flowPar: "1",
  flowRate: "0",
  flowPeriod: "60",
  retry: defaultRetry(),
});

const status = useActionStatus();

const flowOptions = computed(() =>
  catalog.flows.map((f) => ({
    value: flowId(f),
    label: `${f.key || flowId(f)} · ${f.parallelism ?? 1} in-flight · ${f.rate ?? 0}/${f.period_secs ?? 60}s`,
  })),
);

const selectedFlow = computed(() => catalog.flows.find((f) => flowId(f) === form.flowId) || null);
const hideRoutingKey = computed(() => form.mode !== "group" && form.pacing === "profile");

onMounted(() => {
  void refreshGroups();
  void refreshFlows();
});

async function publish() {
  status.reset();
  let key = form.key.trim();
  if (form.mode !== "group" && form.pacing === "profile") {
    if (!form.flowId) {
      status.fail("Select a flow profile, or switch pacing to Unlimited");
      return;
    }
    const lane = selectedFlow.value?.key?.trim();
    if (!lane) {
      status.fail("That profile has no lane key");
      return;
    }
    key = lane;
  }
  const payload: Record<string, unknown> = {
    key,
    body: parseBody(form.body),
    priority: Number(form.priority) || 5,
  };
  if (form.mode === "group") {
    if (!form.groupId) {
      status.fail("Select a group first");
      return;
    }
    payload.group_id = form.groupId;
  } else {
    payload.url = form.url;
    payload.secret = form.secret;
    if (form.pacing === "profile") {
      payload.flow_id = form.flowId;
    } else if (form.pacing === "custom") {
      const parallelism = parseInt(String(form.flowPar), 10);
      const rate = parseInt(String(form.flowRate), 10);
      const period = parseInt(String(form.flowPeriod), 10);
      payload.flowControl = {
        parallelism: Number.isFinite(parallelism) && parallelism > 0 ? parallelism : 1,
        rate: Number.isFinite(rate) && rate >= 0 ? rate : 0,
        period: Number.isFinite(period) && period > 0 ? period : 60,
      };
    }
  }
  if (form.idempotency.trim()) payload.idempotency_key = form.idempotency.trim();
  mergeRetry(payload, form.retry);
  const delay = parseInt(String(form.delay), 10);
  if (delay > 0) payload.delay = delay;
  const headersError = mergeOutbound(payload);
  if (headersError) {
    status.fail(headersError);
    return;
  }
  try {
    const r = await apiPost<{
      group_id?: string;
      accepted?: number;
      deliveries?: unknown[];
      message_id?: string;
      scheduled?: { schedule_id?: string };
    }>(publicUrl("/v1/publish"), payload);
    if (r.group_id) status.succeed(`Group publish accepted ${r.accepted || 0} / ${(r.deliveries || []).length}`);
    else status.succeed(`Published ${r.message_id || r.scheduled?.schedule_id || "ok"}`);
    if (payload.flowControl) await refreshFlows();
  } catch (e) {
    status.fail(e);
  }
}
</script>

<template>
  <div class="space-y-4">
    <div>
      <h1 class="text-lg font-semibold tracking-tight">Publish</h1>
      <p class="text-sm text-muted-foreground">One webhook, or fan-out to every group member.</p>
      <code class="mt-1 block font-mono text-xs text-muted-foreground">POST /v1/publish</code>
    </div>
    <Alert>
      <AlertDescription>
        Leave pacing off for a plain webhook. A saved flow already has a lane key — routing key is hidden and taken from the profile.
      </AlertDescription>
    </Alert>
    <Card>
      <CardHeader>
        <CardTitle>Destination</CardTitle>
        <CardDescription>Where this payload is delivered</CardDescription>
      </CardHeader>
      <CardContent class="grid gap-3 sm:grid-cols-2">
        <Field label="Mode">
          <AppSelect
            v-model="form.mode"
            :options="[
              { value: 'url', label: 'URL — one webhook' },
              { value: 'group', label: 'Group — fan-out' },
            ]"
          />
        </Field>
        <Field v-if="form.mode !== 'group'" label="Secret">
          <Input v-model="form.secret" class="font-mono" />
        </Field>
        <Field v-if="form.mode !== 'group'" label="URL" class="sm:col-span-2">
          <Input v-model="form.url" class="font-mono text-xs" />
        </Field>
        <Field v-else label="Group" class="sm:col-span-2">
          <AppSelect
            v-model="form.groupId"
            :options="catalog.groups.map((g) => ({ value: groupId(g), label: g.name || groupId(g) }))"
            placeholder="— select group —"
          />
        </Field>
      </CardContent>
    </Card>
    <Card v-if="form.mode !== 'group'">
      <CardHeader>
        <CardTitle>Pacing</CardTitle>
        <CardDescription>
          Optional. Limits how fast this routing key is delivered. Group members already have their own limits.
        </CardDescription>
      </CardHeader>
      <CardContent class="grid gap-3 sm:grid-cols-2">
        <Field label="Mode" class="sm:col-span-2">
          <AppSelect
            v-model="form.pacing"
            :options="[
              { value: 'none', label: 'Unlimited — deliver as workers allow' },
              { value: 'profile', label: 'Saved flow profile' },
              { value: 'custom', label: 'Custom limits for this routing key' },
            ]"
          />
        </Field>
        <template v-if="form.pacing === 'profile'">
          <Field
            label="Profile"
            class="sm:col-span-2"
            :hint="selectedFlow?.key ? `Lane ${selectedFlow.key} is used as the routing key.` : undefined"
          >
            <AppSelect
              v-model="form.flowId"
              :options="flowOptions"
              placeholder="— select a profile —"
            />
          </Field>
          <p v-if="!flowOptions.length" class="sm:col-span-2 text-xs text-muted-foreground">
            No profiles yet.
            <RouterLink to="/flows" class="text-primary underline-offset-4 hover:underline">Create one on Flows</RouterLink>
            , or choose custom limits.
          </p>
        </template>
        <template v-else-if="form.pacing === 'custom'">
          <Field label="Parallelism" hint="Max in-flight. 1 = FIFO for this key.">
            <Input v-model="form.flowPar" type="number" min="1" />
          </Field>
          <Field label="Rate" hint="0 = no rate cap">
            <Input v-model="form.flowRate" type="number" min="0" />
          </Field>
          <Field label="Period (sec)" hint="Window for the rate cap">
            <Input v-model="form.flowPeriod" type="number" min="1" />
          </Field>
        </template>
      </CardContent>
    </Card>
    <Card>
      <CardHeader>
        <CardTitle>Message</CardTitle>
        <CardDescription>Payload and delivery options</CardDescription>
      </CardHeader>
      <CardContent class="grid gap-3 sm:grid-cols-2">
        <Field
          v-if="!hideRoutingKey"
          label="Routing key"
          hint="Groups related messages. With custom pacing, this is also the flow lane."
        >
          <Input v-model="form.key" />
        </Field>
        <Field label="Priority"><Input v-model="form.priority" type="number" min="0" max="9" /></Field>
        <Field label="Delay (ms)" hint="0 = send now"><Input v-model="form.delay" type="number" placeholder="0" /></Field>
        <Field label="Idempotency key" hint="Optional — same key is a no-op">
          <Input v-model="form.idempotency" />
        </Field>
        <div class="sm:col-span-2"><RetryFields v-model="form.retry" /></div>
        <Field label="Body (JSON)" class="sm:col-span-2">
          <Textarea v-model="form.body" class="font-mono text-xs" rows="5" />
        </Field>
      </CardContent>
    </Card>
    <div class="flex items-center gap-2">
      <Button size="sm" @click="publish">Publish</Button>
      <ActionMessage :error="status.error" :ok="status.ok" />
    </div>
  </div>
</template>
