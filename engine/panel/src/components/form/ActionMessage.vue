<script setup lang="ts">
import { cn } from "@/lib/utils";

defineProps<{ error?: string; ok?: string; class?: string }>();
</script>

<template>
  <p v-if="error" :class="cn('text-xs leading-snug text-destructive', $props.class)">{{ error }}</p>
  <p v-else-if="ok" :class="cn('text-xs leading-snug text-primary', $props.class)">{{ ok }}</p>
</template>
