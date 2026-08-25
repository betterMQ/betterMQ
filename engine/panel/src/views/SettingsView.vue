<script setup lang="ts">
import { ref } from "vue";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import Field from "@/components/form/Field.vue";
import ActionMessage from "@/components/form/ActionMessage.vue";
import { useActionStatus } from "@/composables/useActionStatus";
import { regenerateToken } from "@/lib/session";

const password = ref("");
const busy = ref(false);
const status = useActionStatus();

async function regen() {
  status.reset();
  busy.value = true;
  try {
    await regenerateToken(password.value);
    password.value = "";
    status.succeed("API token regenerated");
  } catch (e) {
    status.fail(e);
  } finally {
    busy.value = false;
  }
}
</script>

<template>
  <div class="space-y-4">
    <div>
      <h1 class="text-lg font-semibold tracking-tight">Settings</h1>
      <p class="text-sm text-muted-foreground">Local API token (standalone mode only)</p>
    </div>
    <Card class="max-w-lg">
      <CardHeader>
        <CardTitle>Regenerate API token</CardTitle>
        <CardDescription>
          Enter your panel password. The new token is shown once — copy it immediately.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <Field label="Panel password">
          <Input v-model="password" type="password" autocomplete="current-password" />
        </Field>
      </CardContent>
      <CardFooter>
        <Button size="sm" :disabled="busy" @click="regen">Regenerate token</Button>
        <ActionMessage :error="status.error" :ok="status.ok" />
      </CardFooter>
    </Card>
  </div>
</template>
