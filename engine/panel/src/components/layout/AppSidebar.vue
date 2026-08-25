<script setup lang="ts">
import { useRoute, RouterLink } from "vue-router";
import {
  BookOpen,
  Clock,
  Globe,
  Layers,
  LayoutDashboard,
  Monitor,
  Moon,
  Plus,
  Send,
  Server,
  Settings,
  Share2,
  Sun,
  TriangleAlert,
  Users,
} from "@lucide/vue";
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarRail,
} from "@/components/ui/sidebar";
import CellSwitcher from "@/components/layout/CellSwitcher.vue";
import { isExternalAuth } from "@/lib/config";
import { session, setTheme } from "@/lib/session";
import type { ThemeMode } from "@/lib/storage";
import iconBrand from "@/assets/logos/betterMQ-icon-brand.svg";

const route = useRoute();

const nav = [
  { to: "/", label: "Overview", icon: LayoutDashboard },
  { to: "/infra", label: "Infrastructure", icon: Server },
  { to: "/queues", label: "Queues", icon: Layers },
  { to: "/groups", label: "Groups", icon: Users },
  { to: "/publish", label: "Publish", icon: Send },
  { to: "/enqueue", label: "Enqueue", icon: Plus },
  { to: "/flows", label: "Flows", icon: Share2 },
  { to: "/schedules", label: "Schedules", icon: Clock },
  { to: "/dlq", label: "DLQ", icon: TriangleAlert },
  { to: "/http", label: "HTTP", icon: Globe },
  { to: "/docs", label: "Docs", icon: BookOpen },
];

const settingsItem = { to: "/settings", label: "Settings", icon: Settings };

const themes: { id: ThemeMode; label: string; icon: typeof Sun }[] = [
  { id: "system", label: "System", icon: Monitor },
  { id: "light", label: "Light", icon: Sun },
  { id: "dark", label: "Dark", icon: Moon },
];
</script>

<template>
  <Sidebar variant="inset" collapsible="icon">
    <SidebarHeader class="gap-2 px-2 py-2">
      <RouterLink to="/" class="flex items-center gap-2 overflow-hidden rounded-2xl px-1 py-0.5">
        <span class="flex size-8 shrink-0 items-center justify-center rounded-2xl bg-primary/10 ring-1 ring-primary/20">
          <img :src="iconBrand" alt="" class="size-5" />
        </span>
        <span class="truncate text-sm font-semibold tracking-tight group-data-[collapsible=icon]:hidden">
          betterMQ
        </span>
      </RouterLink>
      <CellSwitcher />
    </SidebarHeader>
    <SidebarContent>
      <SidebarGroup>
        <SidebarGroupLabel class="text-xs tracking-widest uppercase">Navigate</SidebarGroupLabel>
        <SidebarGroupContent>
          <SidebarMenu>
            <SidebarMenuItem v-for="item in nav" :key="item.to">
              <SidebarMenuButton
                as-child
                :is-active="route.path === item.to"
                :tooltip="item.label"
              >
                <RouterLink :to="item.to">
                  <component :is="item.icon" />
                  <span>{{ item.label }}</span>
                </RouterLink>
              </SidebarMenuButton>
            </SidebarMenuItem>
            <SidebarMenuItem v-if="!isExternalAuth()">
              <SidebarMenuButton
                as-child
                :is-active="route.path === settingsItem.to"
                :tooltip="settingsItem.label"
              >
                <RouterLink :to="settingsItem.to">
                  <component :is="settingsItem.icon" />
                  <span>{{ settingsItem.label }}</span>
                </RouterLink>
              </SidebarMenuButton>
            </SidebarMenuItem>
          </SidebarMenu>
        </SidebarGroupContent>
      </SidebarGroup>
    </SidebarContent>
    <SidebarFooter>
      <div class="grid grid-cols-3 gap-0.5 rounded-2xl bg-muted p-1 group-data-[collapsible=icon]:hidden">
        <button
          v-for="t in themes"
          :key="t.id"
          type="button"
          class="flex h-7 items-center justify-center gap-1 rounded-xl text-xs font-medium transition-colors"
          :class="session.themeMode === t.id ? 'bg-background text-primary shadow-sm' : 'text-muted-foreground hover:text-foreground'"
          @click="setTheme(t.id)"
        >
          <component :is="t.icon" class="size-3.5" />
          {{ t.label }}
        </button>
      </div>
    </SidebarFooter>
    <SidebarRail />
  </Sidebar>
</template>
