<script setup lang="ts">
import { computed } from "vue";
import { Card } from "@/components/ui/card";
import { session } from "@/lib/session";

const docsSrc = computed(() => `${session.apiBase}/docs?embed=1`);
const openapi = computed(() => `${session.apiBase}/openapi.json`);
</script>

<template>
  <div class="flex h-[calc(100svh-7rem)] flex-col gap-3">
    <div class="flex items-end justify-between gap-3">
      <div>
        <h1 class="text-lg font-semibold tracking-tight">API reference</h1>
        <p class="text-sm text-muted-foreground">
          OpenAPI 3.1 ·
          <a :href="docsSrc.replace('?embed=1', '')" target="_blank" rel="noopener" class="underline">/docs</a>
          ·
          <a :href="openapi" target="_blank" rel="noopener" class="underline">openapi.json</a>
        </p>
      </div>
    </div>
    <Card class="min-h-0 flex-1 overflow-hidden p-0">
      <iframe
        :src="docsSrc"
        title="BetterMQ API reference"
        class="h-full w-full border-0"
        sandbox="allow-scripts allow-same-origin allow-forms allow-popups"
      />
    </Card>
  </div>
</template>
