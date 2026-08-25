import "@fontsource/inter/400.css";
import "@fontsource/inter/500.css";
import "@fontsource/inter/600.css";
import "@fontsource/inter/700.css";
import "@fontsource/jetbrains-mono/400.css";
import "@fontsource/jetbrains-mono/500.css";
import { createApp } from "vue";
import App from "./App.vue";
import { router } from "./router";
import "./style.css";

createApp(App).use(router).mount("#app");
