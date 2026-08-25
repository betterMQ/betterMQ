<script setup lang="ts">
import { ref } from "vue";
import AuthLayout from "@/components/layout/AuthLayout.vue";
import { Button } from "@/components/ui/button";
import ActionMessage from "@/components/form/ActionMessage.vue";
import { copyText } from "@/lib/payload";
import { finishReveal, session } from "@/lib/session";

const copyError = ref("");
const copyOk = ref("");

async function copy() {
  copyError.value = "";
  copyOk.value = "";
  try {
    await copyText(session.revealedToken);
    copyOk.value = "Token copied";
  } catch {
    copyError.value = "Copy failed";
  }
}
</script>

<template>
  <AuthLayout>
    <div class="space-y-4">
      <div>
        <h1 class="text-lg font-semibold">Copy your API token</h1>
        <p class="mt-1 text-sm text-muted-foreground">
          Shown once. It will not be displayed again. Regenerate from Settings with your password.
        </p>
      </div>
      <pre class="overflow-x-auto rounded-2xl border bg-muted p-3 font-mono text-xs">{{ session.revealedToken }}</pre>
      <ActionMessage :error="copyError" :ok="copyOk" />
      <Button class="w-full" variant="outline" @click="copy">Copy token</Button>
      <Button class="w-full" @click="finishReveal">I saved it — continue</Button>
    </div>
  </AuthLayout>
</template>
