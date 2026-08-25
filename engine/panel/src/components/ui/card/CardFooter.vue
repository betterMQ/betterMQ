<script setup lang="ts">
import type { HTMLAttributes } from 'vue'
import { cn } from '@/lib/utils'

const props = defineProps<{
  class?: HTMLAttributes['class']
}>()
</script>

<template>
  <div
    data-slot="card-footer"
    :class="cn('bg-muted/50 rounded-b-2xl border-t p-3 group-data-[size=sm]/card:p-2 flex items-center gap-3', props.class)"
  >
    <slot />
  </div>
</template>
