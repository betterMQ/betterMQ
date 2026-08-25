<script setup lang="ts">
import { SidebarInset, SidebarProvider } from "@/components/ui/sidebar";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import AppHeader from "@/components/layout/AppHeader.vue";
import AppSidebar from "@/components/layout/AppSidebar.vue";
import { session } from "@/lib/session";
</script>

<template>
  <SidebarProvider>
    <AppSidebar />
    <SidebarInset>
      <Alert v-if="session.splitAuth" variant="destructive" class="m-2">
        <AlertTitle>Split auth across brokers</AlertTitle>
        <AlertDescription>
          This API URL hits nodes with different local auth. Use one broker for dev, or complete
          panel setup on each cluster node.
        </AlertDescription>
      </Alert>
      <AppHeader />
      <div class="flex-1 overflow-auto p-4">
        <router-view />
      </div>
    </SidebarInset>
  </SidebarProvider>
</template>
