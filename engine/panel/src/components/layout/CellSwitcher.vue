<script setup lang="ts">
import { computed } from "vue";
import { ChevronsUpDown } from "@lucide/vue";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { glassAvatar } from "@/lib/avatar";
import { session, setCellId, type CellRecord } from "@/lib/session";

const cells = computed<CellRecord[]>(() =>
  session.cells.length
    ? session.cells
    : [{ id: "local", region: "local", controllerUrl: "", label: "local" }],
);

const current = computed(
  () => cells.value.find((c) => c.id === session.selectedCellId) || cells.value[0],
);

function title(c: CellRecord) {
  return c.label || c.id;
}

function subtitle(c: CellRecord) {
  if (c.region && c.region !== c.id && c.region !== c.label) return `${c.id} · ${c.region}`;
  return c.id;
}
</script>

<template>
  <DropdownMenu>
    <DropdownMenuTrigger as-child>
      <button
        type="button"
        class="flex w-full items-center gap-2 overflow-hidden rounded-2xl p-1.5 text-left ring-1 ring-sidebar-border/80 transition-colors hover:bg-sidebar-accent group-data-[collapsible=icon]:size-8 group-data-[collapsible=icon]:justify-center group-data-[collapsible=icon]:p-0 group-data-[collapsible=icon]:ring-0"
      >
        <span class="glass-frost size-8 shrink-0 overflow-hidden rounded-2xl">
          <img :src="glassAvatar(current?.id || 'local')" :alt="current ? title(current) : 'cell'" class="size-full" />
        </span>
        <span class="min-w-0 flex-1 group-data-[collapsible=icon]:hidden">
          <span class="block truncate text-sm font-medium leading-tight">{{ current ? title(current) : 'local' }}</span>
          <span class="block truncate text-xs text-muted-foreground">{{ current ? subtitle(current) : 'local' }}</span>
        </span>
        <ChevronsUpDown class="size-4 shrink-0 text-muted-foreground group-data-[collapsible=icon]:hidden" />
      </button>
    </DropdownMenuTrigger>
    <DropdownMenuContent class="w-56" align="start" side="bottom">
      <DropdownMenuLabel class="text-xs text-muted-foreground">Cell</DropdownMenuLabel>
      <DropdownMenuItem
        v-for="c in cells"
        :key="c.id"
        class="gap-2 rounded-xl"
        @click="setCellId(c.id)"
      >
        <span class="glass-frost size-7 overflow-hidden rounded-xl">
          <img :src="glassAvatar(c.id)" :alt="title(c)" class="size-full" />
        </span>
        <span class="min-w-0 flex-1">
          <span class="block truncate text-sm font-medium">{{ title(c) }}</span>
          <span class="block truncate text-xs text-muted-foreground">{{ subtitle(c) }}</span>
        </span>
      </DropdownMenuItem>
    </DropdownMenuContent>
  </DropdownMenu>
</template>
