<script setup lang="ts">
import { ref } from "vue";
import AuthLayout from "@/components/layout/AuthLayout.vue";
import Field from "@/components/form/Field.vue";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { session, setupHint, setupPassword } from "@/lib/session";
import { errText } from "@/lib/payload";

const p1 = ref("");
const p2 = ref("");
const error = ref("");
const busy = ref(false);

async function submit() {
  error.value = "";
  if (p1.value.length < 12) {
    error.value = "Password must be at least 12 characters.";
    return;
  }
  if (p1.value !== p2.value) {
    error.value = "Passwords do not match.";
    return;
  }
  busy.value = true;
  try {
    await setupPassword(p1.value);
  } catch (e) {
    error.value = errText(e);
  } finally {
    busy.value = false;
  }
}
</script>

<template>
  <AuthLayout>
    <div class="space-y-4">
      <div>
        <h1 class="text-lg font-semibold">Secure this broker</h1>
        <p class="mt-1 text-sm text-muted-foreground">
          Choose a password (at least 12 characters). You will get an API token once — copy it.
        </p>
      </div>
      <Field label="Password">
        <Input v-model="p1" type="password" autocomplete="new-password" />
      </Field>
      <Field label="Confirm">
        <Input v-model="p2" type="password" autocomplete="new-password" @keydown.enter="submit" />
      </Field>
      <p v-if="error" class="text-sm text-destructive">{{ error }}</p>
      <Button class="w-full" :disabled="busy" @click="submit">Create API token</Button>
      <p class="text-xs text-muted-foreground">{{ setupHint(session.auth) }}</p>
    </div>
  </AuthLayout>
</template>
