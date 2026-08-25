import { adminUrl, apiGet } from "@/lib/api";

export type FedOk<T> = { cell: string; data: T };
export type FedResult<T> = { ok?: FedOk<T>[]; failed?: { cell: string; error: string }[] };

export type AdminNode = {
  id?: string;
  addr?: string;
  alive?: boolean;
  self?: boolean;
  name?: string;
  healthy?: boolean;
  is_self?: boolean;
  led_shards?: number[];
  preferred_shards?: number[];
};

export type AdminShard = {
  shard?: number;
  leaderId?: string;
  replicas?: string[];
  learners?: string[];
  isr?: string[];
};

export type AdminCluster = {
  nodeCount?: number;
  replicationFactor?: number;
  minIsr?: number;
  ready?: boolean;
};

export async function queryCells<T>(kind: "nodes" | "shards" | "cluster" | "catalog"): Promise<FedResult<T>> {
  return apiGet<FedResult<T>>(adminUrl(`/cells/query/${kind}`));
}

export function dataForCell<T>(res: FedResult<T> | null, cellId: string): T | null {
  if (!res?.ok) return null;
  const hit = res.ok.find((row) => row.cell === cellId) || res.ok[0];
  return hit?.data ?? null;
}
