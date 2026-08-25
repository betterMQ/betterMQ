<script setup lang="ts">
import { cn } from "@/lib/utils";

defineProps<{ label: string; hint?: string; error?: string; class?: string }>();
</script>

<template>
  <div :class="cn('grid gap-1', $props.class)">
    <label class="text-sm font-medium text-muted-foreground">{{ label }}</label>
    <slot />
    <p v-if="error" class="text-xs text-destructive">{{ error }}</p>
    <p v-else-if="hint" class="text-xs text-muted-foreground">{{ hint }}</p>
  </div>
</template>
