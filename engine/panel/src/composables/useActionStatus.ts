import { reactive } from "vue";
import { errText } from "@/lib/payload";

export function useActionStatus() {
  return reactive({
    error: "",
    ok: "",
    fail(e: unknown) {
      this.ok = "";
      this.error = typeof e === "string" ? e : errText(e);
    },
    succeed(msg: string) {
      this.error = "";
      this.ok = msg;
    },
    reset() {
      this.error = "";
      this.ok = "";
    },
  });
}
