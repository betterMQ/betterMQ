<script setup lang="ts">
import { onMounted } from "vue";
import { TooltipProvider } from "@/components/ui/tooltip";
import AppShell from "@/components/layout/AppShell.vue";
import GateView from "@/views/GateView.vue";
import RevealView from "@/views/RevealView.vue";
import SetupView from "@/views/SetupView.vue";
import { initAuth, session } from "@/lib/session";
import { applyTheme } from "@/lib/storage";

onMounted(() => {
  window.matchMedia("(prefers-color-scheme: dark)").addEventListener("change", () => {
    if (session.themeMode === "system") applyTheme("system");
  });
  void initAuth();
});
</script>

<template>
  <TooltipProvider>
    <div v-if="session.phase === 'loading'" class="flex min-h-svh items-center justify-center text-sm text-muted-foreground">
      Connecting…
    </div>
    <SetupView v-else-if="session.phase === 'setup'" />
    <RevealView v-else-if="session.phase === 'reveal'" />
    <GateView v-else-if="session.phase === 'gate'" />
    <AppShell v-else />
  </TooltipProvider>
</template>
