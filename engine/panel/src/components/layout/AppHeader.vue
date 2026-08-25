<script setup lang="ts">
import { RefreshCw } from "@lucide/vue";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import ActionMessage from "@/components/form/ActionMessage.vue";
import { SidebarTrigger } from "@/components/ui/sidebar";
import { useActionStatus } from "@/composables/useActionStatus";
import { refreshFlows, refreshGroups, refreshQueues } from "@/lib/catalog";
import { pollCluster, pollHealth, refreshCells, session } from "@/lib/session";

const refreshStatus = useActionStatus();

async function refreshAll() {
  refreshStatus.reset();
  try {
    await Promise.all([
      pollHealth(),
      pollCluster(),
      refreshCells(),
      refreshQueues(),
      refreshGroups(),
      refreshFlows(),
    ]);
    refreshStatus.succeed("Refreshed");
  } catch (e) {
    refreshStatus.fail(e);
  }
}
</script>

<template>
  <header class="flex h-12 shrink-0 items-center gap-2 border-b px-4">
    <SidebarTrigger />
    <Badge variant="outline" class="font-mono text-xs">0.4.0</Badge>
    <Badge
      :variant="session.healthOk ? 'outline' : 'destructive'"
      :class="session.healthOk ? 'border-transparent bg-success/15 text-success' : ''"
      class="text-xs"
    >
      {{ session.healthOk ? "Healthy" : "down" }}
    </Badge>
    <Badge variant="secondary" class="font-mono text-xs">{{ session.clusterLabel }}</Badge>
    <div class="ml-auto flex items-center gap-2">
      <ActionMessage :error="refreshStatus.error" :ok="refreshStatus.ok" />
      <Button variant="ghost" size="icon-sm" title="Refresh" @click="refreshAll">
        <RefreshCw />
      </Button>
    </div>
  </header>
</template>
