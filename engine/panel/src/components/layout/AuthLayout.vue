<script setup lang="ts">
import iconBrand from "@/assets/logos/betterMQ-icon-brand.svg";
</script>

<template>
  <div class="flex min-h-svh items-center justify-center bg-background p-6">
    <div class="w-full max-w-sm space-y-5">
      <div class="flex items-center gap-2">
        <span class="flex size-8 items-center justify-center rounded-2xl bg-primary/10 ring-1 ring-primary/20">
          <img :src="iconBrand" alt="" class="size-4" />
        </span>
        <div>
          <p class="text-sm font-semibold tracking-tight">betterMQ</p>
          <p class="text-xs text-muted-foreground">HTTP message broker</p>
        </div>
      </div>
      <slot />
    </div>
  </div>
</template>
