<script setup lang="ts">
import type { RetryState } from "@/lib/payload";
import Field from "@/components/form/Field.vue";
import AppSelect from "@/components/form/AppSelect.vue";
import { Input } from "@/components/ui/input";

const retry = defineModel<RetryState>({ required: true });

const kinds = [
  { value: "exponential", label: "Exponential" },
  { value: "fixed", label: "Fixed interval" },
];
</script>

<template>
  <div class="grid gap-3 sm:grid-cols-2">
    <Field label="Max retries" hint="0 = no retries">
      <Input v-model="retry.maxRetries" type="number" min="0" />
    </Field>
    <template v-if="retry.maxRetries > 0">
      <Field label="Backoff">
        <AppSelect v-model="retry.kind" :options="kinds" />
      </Field>
      <Field label="Delay (ms)">
        <Input v-model="retry.initialMs" type="number" min="1" />
      </Field>
      <template v-if="retry.kind === 'exponential'">
        <Field label="Max delay (ms)">
          <Input v-model="retry.maxMs" type="number" min="1" />
        </Field>
        <Field label="Multiplier">
          <Input v-model="retry.multiplier" type="number" min="1" step="0.1" />
        </Field>
      </template>
    </template>
  </div>
</template>
