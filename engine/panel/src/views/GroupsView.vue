<script setup lang="ts">
import { onMounted, reactive, ref } from "vue";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import Field from "@/components/form/Field.vue";
import ActionMessage from "@/components/form/ActionMessage.vue";
import AppSelect from "@/components/form/AppSelect.vue";
import { useActionStatus } from "@/composables/useActionStatus";
import { apiDelete, apiGet, apiPost, publicUrl } from "@/lib/api";
import { catalog, groupId, refreshGroups, type GroupRow } from "@/lib/catalog";
import { errText } from "@/lib/payload";

const name = ref("alerts");
const tab = ref("list");
const selected = ref("");
const members = ref<{ member_id?: string; id?: string; name?: string; url?: string; parallelism?: number; rate?: number }[]>([]);
const member = reactive({
  name: "primary",
  secret: "whsec_demo",
  url: "https://webhook.site",
  parallelism: 1,
  rate: 0,
  period: 60,
});

const createStatus = useActionStatus();
const listStatus = useActionStatus();
const memberStatus = useActionStatus();
const memberListStatus = useActionStatus();

onMounted(() => {
  void refreshGroups();
});

const groupOptions = () =>
  catalog.groups.map((g) => ({ value: groupId(g), label: g.name || groupId(g) }));

async function create() {
  createStatus.reset();
  try {
    const r = await apiPost<{ name?: string; group_id?: string }>(publicUrl("/v1/groups"), { name: name.value });
    createStatus.succeed(`Group ${r.name} → ${r.group_id}`);
    await refreshGroups();
    if (r.group_id) {
      selected.value = r.group_id;
      tab.value = "members";
      await loadMembers(r.group_id);
    }
  } catch (e) {
    createStatus.fail(errText(e));
  }
}

async function loadMembers(gid: string) {
  if (!gid) {
    members.value = [];
    return;
  }
  try {
    const d = await apiGet<{ members?: typeof members.value }>(publicUrl(`/v1/groups/${encodeURIComponent(gid)}`));
    members.value = d.members || [];
  } catch {
    members.value = [];
  }
}

async function addMember() {
  memberStatus.reset();
  if (!selected.value) {
    memberStatus.fail("Select a group first");
    return;
  }
  try {
    const r = await apiPost<{ name?: string; member_id?: string }>(
      publicUrl(`/v1/groups/${encodeURIComponent(selected.value)}/members`),
      {
        name: member.name,
        url: member.url,
        secret: member.secret,
        parallelism: Number(member.parallelism) || 1,
        rate: Number(member.rate) || 0,
        period_secs: Number(member.period) || 60,
      },
    );
    memberStatus.succeed(`Member ${r.name} → ${r.member_id}`);
    await loadMembers(selected.value);
  } catch (e) {
    memberStatus.fail(errText(e));
  }
}

async function removeGroup(g: GroupRow) {
  listStatus.reset();
  try {
    await apiDelete(publicUrl(`/v1/groups/${encodeURIComponent(groupId(g))}`));
    listStatus.succeed("Group removed");
    await refreshGroups();
  } catch (e) {
    listStatus.fail(errText(e));
  }
}

async function removeMember(id: string) {
  memberListStatus.reset();
  try {
    await apiDelete(
      publicUrl(`/v1/groups/${encodeURIComponent(selected.value)}/members/${encodeURIComponent(id)}`),
    );
    memberListStatus.succeed("Member removed");
    await loadMembers(selected.value);
  } catch (e) {
    memberListStatus.fail(errText(e));
  }
}

function openMembers(g: GroupRow) {
  selected.value = groupId(g);
  tab.value = "members";
  void loadMembers(selected.value);
}
</script>

<template>
  <div class="space-y-4">
    <div>
      <h1 class="text-lg font-semibold tracking-tight">Groups</h1>
      <p class="text-sm text-muted-foreground">Fan-out one payload to every active member.</p>
      <code class="mt-1 block font-mono text-xs text-muted-foreground">POST /v1/publish</code>
    </div>
    <Tabs v-model="tab" default-value="list" class="gap-4">
      <TabsList class="max-w-sm">
        <TabsTrigger value="list">Groups</TabsTrigger>
        <TabsTrigger value="members">Members</TabsTrigger>
      </TabsList>
      <TabsContent value="list" class="grid gap-3 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Create group</CardTitle>
            <CardDescription>Logical fan-out target</CardDescription>
          </CardHeader>
          <CardContent>
            <Field label="Name"><Input v-model="name" placeholder="alerts" /></Field>
          </CardContent>
          <CardFooter>
            <Button size="sm" @click="create">Create group</Button>
            <ActionMessage :error="createStatus.error" :ok="createStatus.ok" />
          </CardFooter>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Registered</CardTitle>
            <CardDescription>Select a group to manage members</CardDescription>
          </CardHeader>
          <CardContent>
            <ActionMessage class="mb-2" :error="listStatus.error" :ok="listStatus.ok" />
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>ID</TableHead>
                  <TableHead>Name</TableHead>
                  <TableHead />
                </TableRow>
              </TableHeader>
              <TableBody>
                <TableRow v-for="g in catalog.groups" :key="groupId(g)">
                  <TableCell class="font-mono text-xs">{{ groupId(g) }}</TableCell>
                  <TableCell>{{ g.name }}</TableCell>
                  <TableCell class="space-x-1">
                    <Button size="sm" variant="outline" @click="openMembers(g)">Members</Button>
                    <Button size="sm" variant="destructive" @click="removeGroup(g)">Delete</Button>
                  </TableCell>
                </TableRow>
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      </TabsContent>
      <TabsContent value="members" class="space-y-3">
        <Card>
          <CardHeader>
            <CardTitle>Members</CardTitle>
            <CardDescription>Webhook destinations that receive each group publish</CardDescription>
          </CardHeader>
          <CardContent class="grid gap-2 sm:grid-cols-2">
            <Field label="Group" class="sm:col-span-2">
              <AppSelect
                v-model="selected"
                :options="groupOptions()"
                placeholder="— select group —"
                @update:model-value="loadMembers"
              />
            </Field>
            <Field label="Name"><Input v-model="member.name" /></Field>
            <Field label="Secret"><Input v-model="member.secret" class="font-mono" /></Field>
            <Field label="Webhook URL" class="sm:col-span-2"><Input v-model="member.url" class="font-mono text-xs" /></Field>
            <Field label="Parallelism"><Input v-model="member.parallelism" type="number" min="1" /></Field>
            <Field label="Rate" hint="0 = unlimited"><Input v-model="member.rate" type="number" min="0" /></Field>
            <Field label="Period (s)"><Input v-model="member.period" type="number" min="1" /></Field>
          </CardContent>
          <CardFooter>
            <Button size="sm" @click="addMember">Add member</Button>
            <ActionMessage :error="memberStatus.error" :ok="memberStatus.ok" />
          </CardFooter>
        </Card>
        <Card>
          <CardHeader><CardTitle>Member list</CardTitle></CardHeader>
          <CardContent>
            <ActionMessage class="mb-2" :error="memberListStatus.error" :ok="memberListStatus.ok" />
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>ID</TableHead>
                  <TableHead>Name</TableHead>
                  <TableHead>URL</TableHead>
                  <TableHead />
                </TableRow>
              </TableHeader>
              <TableBody>
                <TableRow v-for="m in members" :key="m.member_id || m.id">
                  <TableCell class="font-mono text-xs">{{ m.member_id || m.id }}</TableCell>
                  <TableCell>{{ m.name }}</TableCell>
                  <TableCell class="max-w-xs truncate font-mono text-xs">{{ m.url }}</TableCell>
                  <TableCell>
                    <Button size="sm" variant="destructive" @click="removeMember(m.member_id || m.id || '')">Delete</Button>
                  </TableCell>
                </TableRow>
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      </TabsContent>
    </Tabs>
  </div>
</template>
