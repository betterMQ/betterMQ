(function () {
  const $log = $("#log");
  let queuesCache = [];
  let groupsCache = [];
  let groupMembersCache = [];
  let infraIsSeed = false;

  const TOKEN_PREFIX = "bettermq_api_token:";
  const API_BASE_KEY = "bettermq_api_base";
  const LEGACY_TOKEN_KEY = "bettermq_api_token";

  function defaultApiBase() {
    if (location.pathname.indexOf("/panel") !== -1) {
      return location.origin;
    }
    return "http://127.0.0.1:8080";
  }

  function apiBase() {
    const v = ($("#apiBase").val() || "").trim();
    return v.replace(/\/$/, "");
  }

  function tokenStorageKey() {
    return TOKEN_PREFIX + apiBase();
  }

  function loadSavedApiBase() {
    return localStorage.getItem(API_BASE_KEY) || defaultApiBase();
  }

  function loadSavedToken() {
    const key = tokenStorageKey();
    let t = localStorage.getItem(key);
    if (!t) {
      t = localStorage.getItem(LEGACY_TOKEN_KEY);
      if (t) {
        localStorage.setItem(key, t);
        localStorage.removeItem(LEGACY_TOKEN_KEY);
      }
    }
    return t || "";
  }

  function normalizeToken(raw) {
    let t = (raw || "").trim();
    if (/^bearer\s+/i.test(t)) {
      t = t.replace(/^bearer\s+/i, "").trim();
    }
    return t;
  }

  $("#apiBase").val(loadSavedApiBase());
  const savedToken = loadSavedToken();
  if (savedToken) $("#apiToken").val(savedToken);

  $("#apiToken").on("change blur", function () {
    const v = ($(this).val() || "").trim();
    if (v) localStorage.setItem(tokenStorageKey(), v);
    else localStorage.removeItem(tokenStorageKey());
    pollHealth();
  });
  $("#apiBase").on("change", function () {
    const v = ($(this).val() || "").trim();
    if (v) localStorage.setItem(API_BASE_KEY, v);
    appStarted = false;
    const t = loadSavedToken();
    $("#apiToken").val(t);
    syncDocsUrls();
    initAuth();
  });

  let sessionReady = false;
  let authCheckGen = 0;
  const apiErrorKinds = new Set();

  function authHeaders() {
    const t = normalizeToken($("#apiToken").val());
    return t ? { Authorization: "Bearer " + t } : {};
  }

  function apiGetJSON(url) {
    if (!sessionReady) {
      return $.Deferred().reject({ status: 0, statusText: "session not ready" }).promise();
    }
    return $.ajax({
      url: url,
      dataType: "json",
      headers: authHeaders(),
      timeout: 15000,
    });
  }

  function noteApiError(kind, xhr) {
    const err =
      (xhr && xhr.responseJSON && xhr.responseJSON.error) ||
      (xhr && xhr.statusText) ||
      "request failed";
    apiErrorKinds.add(err);
    if (apiErrorKinds.size > 1) {
      $("#clusterAuthBanner").removeClass("hidden");
    }
    return err;
  }

  $.ajaxSetup({
    beforeSend: function (xhr, settings) {
      if (!sessionReady) return;
      if (settings.url && String(settings.url).indexOf("/v1/") !== -1) {
        const h = authHeaders();
        if (h.Authorization) xhr.setRequestHeader("Authorization", h.Authorization);
      }
    },
  });

  $("#btnDismissAuthBanner").on("click", function () {
    $("#clusterAuthBanner").addClass("hidden");
  });

  function syncDocsUrls() {
    const base = apiBase();
    if (!base) return;
    $("#docsFrame").attr("src", base + "/docs");
    $("#docsOpenLink").attr("href", base + "/docs");
    $("#openapiLink").attr("href", base + "/openapi.json");
  }

  function setHealthStatus(state, meta) {
    const $pill = $("#healthPill");
    $pill.removeClass("health-healthy health-unhealthy health-checking");
    $pill.addClass("health-" + state);
    const labels = {
      healthy: "Healthy",
      unhealthy: "Unhealthy",
      checking: "Checking",
    };
    $("#healthLabel").text(labels[state] || "Checking");
    if (meta && meta.version) {
      $pill.attr("title", "v" + meta.version + (meta.protocol ? " · " + meta.protocol : ""));
    } else if (state === "unhealthy") {
      $pill.attr("title", "Cannot reach " + (apiBase() || "API"));
    } else {
      $pill.attr("title", "Broker reachability");
    }
    if (state === "healthy") {
      $("#dashHealth").text("OK").removeClass("kpi-bad").addClass("kpi-good");
      $("#dashHealthSub").text(meta && meta.version ? "v" + meta.version : "Online");
      if (meta && meta.version) {
        $("#dashVersion").text("protocol " + (meta.protocol || "?"));
      }
    } else if (state === "unhealthy") {
      $("#dashHealth").text("Down").removeClass("kpi-good").addClass("kpi-bad");
      $("#dashHealthSub").text("Unreachable");
      $("#dashVersion").text("");
    } else {
      $("#dashHealth").text("…").removeClass("kpi-good kpi-bad");
      $("#dashHealthSub").text("Checking");
    }
  }

  function pollHealth() {
    const base = apiBase();
    if (!base) {
      setHealthStatus("unhealthy");
      return;
    }
    setHealthStatus("checking");
    $.ajax({ url: base + "/healthz", dataType: "json", timeout: 8000 })
      .done(function (h) {
        setHealthStatus("healthy", { version: h.version, protocol: h.protocol });
      })
      .fail(function () {
        setHealthStatus("unhealthy");
      });
  }

  function shortNodeAddr(addr) {
    if (!addr) return "—";
    try {
      const u = new URL(addr);
      return u.hostname + (u.port ? ":" + u.port : "");
    } catch (e) {
      return addr;
    }
  }

  /** Same-host cluster (e.g. :8080 / :8081 on one machine) → show port with full URL in tooltip. */
  function formatClusterAddr(addr, peerAddrs) {
    if (!addr) return { text: "—", title: "" };
    const full = shortNodeAddr(addr);
    const peers = (peerAddrs || [])
      .map(shortNodeAddr)
      .filter(function (a) {
        return a && a !== "—";
      });
    if (peers.length > 1) {
      const hosts = {};
      peers.forEach(function (p) {
        const host = p.replace(/:\d+$/, "");
        hosts[host] = (hosts[host] || 0) + 1;
      });
      const hostKeys = Object.keys(hosts);
      if (hostKeys.length === 1 && hostKeys[0]) {
        try {
          const u = new URL(addr);
          const port = u.port || (u.protocol === "https:" ? "443" : "80");
          return { text: ":" + port, title: full };
        } catch (e) {
          /* fall through */
        }
      }
    }
    return { text: full, title: addr };
  }

  function clusterPeerAddrs(nodes) {
    return (nodes || []).map(function (n) {
      return n.addr;
    });
  }

  function nodeAddrHtml(addr, peerAddrs) {
    const f = formatClusterAddr(addr, peerAddrs);
    if (f.text === "—") return "—";
    return (
      '<span class="cluster-node-addr mono text-xs"' +
      (f.title ? ' title="' + escapeHtml(f.title) + '"' : "") +
      ">" +
      escapeHtml(f.text) +
      "</span>"
    );
  }

  function shardStatusHtml(s) {
    if (s.failover) {
      return (
        '<span class="pill pill-warn" title="Preferred node is down — another node claimed this shard">Claimed</span>'
      );
    }
    return (
      '<span class="pill pill-ok" title="Leader is the preferred (home) node">Home</span>'
    );
  }

  function shortId(id) {
    if (!id) return "—";
    return String(id).slice(0, 8);
  }

  function nodeLabel(node) {
    const host = shortNodeAddr(node.addr);
    return host + (node.is_self ? " (this)" : "");
  }

  function setClusterPill(c) {
    const $pill = $("#clusterPill");
    if (!c || !c.enabled) {
      $pill.addClass("hidden");
      return;
    }
    $pill.removeClass("hidden cluster-pill-ok cluster-pill-warn cluster-pill-bad");
    const label = c.healthy_count + "/" + c.node_count + " nodes";
    $("#clusterPillLabel").text(label);
    if (c.healthy_count === c.node_count) {
      $pill.addClass("cluster-pill-ok");
      $pill.attr("title", "All cluster nodes healthy");
    } else if (c.healthy_count > 0) {
      $pill.addClass("cluster-pill-warn");
      $pill.attr(
        "title",
        c.healthy_count + " of " + c.node_count + " nodes healthy (failover may be active)"
      );
    } else {
      $pill.addClass("cluster-pill-bad");
      $pill.attr("title", "No healthy cluster peers detected");
    }
  }

  function renderClusterStatus(c) {
    setClusterPill(c);
    if (!c || !c.enabled) {
      $("#dashCluster").text("1");
      $("#dashClusterSub").text("Single node");
      $("#clusterDetailCard").addClass("hidden");
      return;
    }

    const failoverShards = (c.shards || []).filter(function (s) {
      return s.failover;
    }).length;
    $("#dashCluster").text(c.healthy_count + "/" + c.node_count);
    $("#dashClusterSub").text(
      failoverShards
        ? failoverShards + " shard(s) claimed (failover)"
        : c.this_node_scheduler_leader
          ? "scheduler leader · all shards at home"
          : "all shards at home"
    );

    $("#clusterDetailCard").removeClass("hidden");
    const schedNode = (c.nodes || []).find(function (n) {
      return c.scheduler_leader_id && n.id === c.scheduler_leader_id;
    });
    const schedLabel = schedNode
      ? shortNodeAddr(schedNode.addr)
      : c.scheduler_leader_id
        ? shortId(c.scheduler_leader_id)
        : "—";

    $("#clusterSummaryGrid").html(
      [
        statBlock("Nodes", c.node_count),
        statBlock("Healthy", c.healthy_count),
        statBlock("Scheduler leader", schedLabel),
        statBlock(
          "This node",
          c.this_node_scheduler_leader ? "Scheduler + shards" : "Shards only"
        ),
        statBlock(
          "Claimed shards",
          failoverShards ? String(failoverShards) : "None"
        ),
      ].join("")
    );

    const peerAddrs = clusterPeerAddrs(c.nodes);
    const $nb = $("#clusterNodesBody").empty();
    (c.nodes || []).forEach(function (n) {
      const health = n.healthy
        ? '<span class="status-dot ok"></span>Healthy'
        : '<span class="status-dot down"></span>Down';
      const roles = [];
      if (n.is_self) roles.push("this node");
      if (c.scheduler_leader_id === n.id) roles.push("scheduler");
      if ((n.led_shards || []).length) roles.push("shard leader");
      const $tr = $("<tr>");
      $tr.append(
        tdHtml(
          nodeAddrHtml(n.addr, peerAddrs) +
            (n.is_self ? '<span class="node-badge-self">you</span>' : "")
        )
      );
      $tr.append(tdHtml(health));
      $tr.append(td(roles.length ? roles.join(", ") : "follower"));
      $tr.append(td((n.led_shards || []).join(", ") || "—", "mono"));
      $tr.append(td((n.preferred_shards || []).join(", ") || "—", "mono"));
      $nb.append($tr);
    });

    const $sb = $("#clusterShardsBody").empty();
    (c.shards || []).forEach(function (s) {
      const leaderNode = (c.nodes || []).find(function (n) {
        return n.id === s.leader_id;
      });
      const prefNode = (c.nodes || []).find(function (n) {
        return n.id === s.preferred_leader_id;
      });
      const leaderAddr = leaderNode
        ? leaderNode.addr
        : s.leader_addr || null;
      const prefAddr = prefNode ? prefNode.addr : null;
      const $tr = $("<tr>");
      $tr.append(td(String(s.shard), "mono"));
      $tr.append(
        tdHtml(
          leaderAddr
            ? nodeAddrHtml(leaderAddr, peerAddrs)
            : escapeHtml(shortId(s.leader_id))
        )
      );
      $tr.append(
        tdHtml(
          s.failover && prefAddr
            ? nodeAddrHtml(prefAddr, peerAddrs)
            : '<span class="text-muted">—</span>'
        )
      );
      $tr.append(tdHtml(shardStatusHtml(s)));
      $sb.append($tr);
    });

    $("#clusterDetailSub").text(
      "Cluster " +
        shortId(c.cluster_id) +
        " · gen " +
        (c.generation != null ? c.generation : "?") +
        " · viewed from " +
        shortNodeAddr(
          ((c.nodes || []).find(function (n) {
            return n.is_self;
          }) || {}).addr
        )
    );
  }

  function statBlock(label, value) {
    return (
      '<div class="cluster-stat"><div class="cluster-stat-label">' +
      escapeHtml(label) +
      '</div><div class="cluster-stat-value">' +
      escapeHtml(String(value)) +
      "</div></div>"
    );
  }

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function pollClusterStatus() {
    if (!sessionReady) return;
    const base = apiBase();
    if (!base) return;
    apiGetJSON(base + "/v1/cluster")
      .done(function (c) {
        renderClusterStatus(c);
      })
      .fail(function () {
        renderClusterStatus(null);
      });
  }

  function apiPostJSON(url, data) {
    return $.ajax({
      url: url,
      method: "POST",
      contentType: "application/json",
      data: JSON.stringify(data || {}),
      dataType: "json",
      headers: authHeaders(),
      timeout: 30000,
    });
  }

  function apiPutJSON(url, data) {
    return $.ajax({
      url: url,
      method: "PUT",
      contentType: "application/json",
      data: JSON.stringify(data || {}),
      dataType: "json",
      headers: authHeaders(),
      timeout: 30000,
    });
  }

  function toggleInfraS3Fields() {
    const slate = $("#infraStorageMode").val() === "slate";
    $("#infraS3Fields").toggleClass("hidden", !slate);
  }

  function showInfraRestart(msg) {
    $("#infraRestartBanner").removeClass("hidden");
    if (msg) $("#infraRestartMsg").text(msg);
  }

  function fillInfraFromConfig(cfg) {
    if (!cfg) return;
    $("#infraNodeName").val(cfg.node.name || "");
    $("#infraPublicUrl").val(cfg.node.publicUrl || "");
    if (cfg.storage && cfg.storage.mode === "slate" && cfg.storage.s3) {
      $("#infraStorageMode").val("slate");
      const s3 = cfg.storage.s3;
      $("#infraS3Endpoint").val(s3.endpoint || "");
      $("#infraS3Bucket").val(s3.bucket || "");
      $("#infraS3PayloadBucket").val(s3.payloadBucket || "");
      $("#infraS3AccessKey").val(s3.accessKey || "");
      $("#infraS3SecretKey").val("");
      $("#infraS3Region").val(s3.region || "auto");
    } else {
      $("#infraStorageMode").val("local");
    }
    toggleInfraS3Fields();
  }

  function renderInfraDashboard(st) {
    if (!st) return;
    const c = st.cluster;
    const clusterOn = c && c.enabled;
    infraIsSeed = !!st.is_cluster_seed;

    $("#infraKpiNode").text(st.node_name || "—");
    $("#infraKpiNodeUrl").text(st.public_url || "—");
    $("#infraKpiStorage").text(st.active_storage || "—");
    $("#infraStorageActive").text(
      st.pending_storage
        ? "Pending → " + st.pending_storage + " (restart)"
        : "Running"
    );

    if (clusterOn) {
      $("#infraKpiCluster").text(c.healthy_count + "/" + c.node_count);
      $("#infraClusterHint").text(
        c.healthy_count === c.node_count ? "All nodes healthy" : "Some nodes down"
      );
      const schedNode = (c.nodes || []).find(function (n) {
        return c.scheduler_leader_id && n.id === c.scheduler_leader_id;
      });
      const schedLabel = schedNode
        ? shortNodeAddr(schedNode.addr)
        : c.scheduler_leader_id
          ? shortId(c.scheduler_leader_id)
          : "—";
      $("#infraKpiScheduler").text(schedLabel);
      $("#infraKpiSchedulerSub").text(
        c.this_node_scheduler_leader ? "You are scheduler leader" : "Follower on this node"
      );
      $("#infraClusterSetupHint").text(
        "Cluster " +
          shortId(c.cluster_id) +
          " · gen " +
          (c.generation != null ? c.generation : "?")
      );
      $("#infraNodesSub").text(
        c.healthy_count +
          " of " +
          c.node_count +
          " nodes healthy · viewed from this broker"
      );
    } else {
      $("#infraKpiCluster").text("Standalone");
      $("#infraClusterHint").text("Not in a cluster yet");
      $("#infraKpiScheduler").text("—");
      $("#infraKpiSchedulerSub").text("Single node");
      $("#infraClusterSetupHint").text("Turn this broker into a multi-node HA cluster");
      $("#infraNodesSub").text("This broker only — create or join a cluster to add nodes");
    }

    if (st.join_token_active) {
      $("#infraJoinTokenActiveHint").removeClass("hidden");
    } else {
      $("#infraJoinTokenActiveHint").addClass("hidden");
    }

    const peerAddrs = clusterPeerAddrs(c.nodes);
    const $nb = $("#infraNodesBody").empty();
    if (clusterOn && (c.nodes || []).length) {
      (c.nodes || []).forEach(function (n) {
        const health = n.healthy
          ? '<span class="status-dot ok"></span>Healthy'
          : '<span class="status-dot down"></span>Down';
        const roles = [];
        if (n.is_self) roles.push("this node");
        if (c.scheduler_leader_id === n.id) roles.push("scheduler");
        if ((n.led_shards || []).length) roles.push("shard leader");
        const $tr = $("<tr>");
        const displayName =
          n.name && n.name.indexOf("://") === -1 && n.name !== n.addr ? n.name : "";
        $tr.append(
          tdHtml(
            (displayName
              ? '<span class="text-sm">' + escapeHtml(displayName) + "</span><br>"
              : "") +
              nodeAddrHtml(n.addr, peerAddrs) +
              (n.is_self ? '<span class="node-badge-self">you</span>' : "")
          )
        );
        $tr.append(tdHtml(health));
        $tr.append(td(roles.length ? roles.join(", ") : "follower"));
        $tr.append(td((n.led_shards || []).join(", ") || "—", "mono"));
        $tr.append(td((n.preferred_shards || []).join(", ") || "—", "mono"));
        if (infraIsSeed && !n.is_self) {
          const removeKey =
            displayName || (n.name && n.name.indexOf("://") === -1 ? n.name : n.addr);
          $tr.append(
            tdHtml(
              '<button type="button" class="btn btn-sm btn-outline infra-remove-node" data-remove-key="' +
                escapeHtml(removeKey) +
                '">Remove</button>'
            )
          );
        } else {
          $tr.append(td(""));
        }
        $nb.append($tr);
      });
    } else {
      const $tr = $("<tr>");
      $tr.append(
        tdHtml(
          '<span class="mono text-xs">' +
            escapeHtml(st.public_url || "localhost") +
            '</span><span class="node-badge-self">you</span>'
        )
      );
      $tr.append(tdHtml('<span class="status-dot ok"></span>Healthy'));
      $tr.append(td("standalone"));
      $tr.append(td("—", "mono"));
      $tr.append(td("—", "mono"));
      $tr.append(td(""));
      $nb.append($tr);
    }

    const $sb = $("#infraShardsBody").empty();
    if (clusterOn && (c.shards || []).length) {
      $("#infraShardsCard").removeClass("hidden");
      (c.shards || []).forEach(function (s) {
        const leaderNode = (c.nodes || []).find(function (n) {
          return n.id === s.leader_id;
        });
        const prefNode = (c.nodes || []).find(function (n) {
          return n.id === s.preferred_leader_id;
        });
        const leaderAddr = leaderNode
          ? leaderNode.addr
          : s.leader_addr || null;
        const prefAddr = prefNode ? prefNode.addr : null;
        const $tr = $("<tr>");
        $tr.append(td(String(s.shard), "mono"));
        $tr.append(
          tdHtml(
            leaderAddr
              ? nodeAddrHtml(leaderAddr, peerAddrs)
              : escapeHtml(shortId(s.leader_id))
          )
        );
        $tr.append(
          tdHtml(
            s.failover && prefAddr
              ? nodeAddrHtml(prefAddr, peerAddrs)
              : '<span class="text-muted">—</span>'
          )
        );
        $tr.append(tdHtml(shardStatusHtml(s)));
        $sb.append($tr);
      });
    } else {
      $("#infraShardsCard").addClass("hidden");
    }

    if (clusterOn) {
      renderClusterStatus(c);
    }
  }

  function refreshInfra() {
    if (!sessionReady) return;
    const base = apiBase();
    apiGetJSON(base + "/v1/infra/status")
      .done(function (st) {
        if (st.needs_restart) {
          showInfraRestart(
            "Pending " +
              (st.pending_storage || "config") +
              " storage — restart broker to apply."
          );
        } else {
          $("#infraRestartBanner").addClass("hidden");
        }
        renderInfraDashboard(st);
        $("#infraJoinPublicUrl").val(st.public_url || $("#infraPublicUrl").val());
        if (!$("#infraJoinNodeName").val()) {
          $("#infraJoinNodeName").val((st.node_name || "broker") + "-2");
        }
      })
      .fail(function () {});
    apiGetJSON(base + "/v1/infra/config")
      .done(function (cfg) {
        fillInfraFromConfig(cfg);
      })
      .fail(function () {});
  }

  $("#infraStorageMode").on("change", toggleInfraS3Fields);

  $("#btnInfraSaveNode").on("click", function () {
    apiPutJSON(apiBase() + "/v1/infra/node", {
      name: $("#infraNodeName").val(),
      public_url: $("#infraPublicUrl").val(),
      listen: "0.0.0.0:8080",
    })
      .done(function (res) {
        log(res.message || "Node settings saved");
        if (res.needs_restart) showInfraRestart(res.message);
        refreshInfra();
        pollClusterStatus();
      })
      .fail(function (xhr) {
        log(noteApiError("infra", xhr), true);
      });
  });

  $("#btnInfraTestS3").on("click", function () {
    const $r = $("#infraS3TestResult").text("Testing…");
    apiPostJSON(apiBase() + "/v1/infra/storage/test", {
      endpoint: $("#infraS3Endpoint").val(),
      bucket: $("#infraS3Bucket").val(),
      access_key: $("#infraS3AccessKey").val(),
      secret_key: $("#infraS3SecretKey").val() || "••••••••",
      region: $("#infraS3Region").val(),
    })
      .done(function (res) {
        $r.text(res.message).css("color", res.ok ? "var(--success)" : "var(--destructive)");
      })
      .fail(function (xhr) {
        $r.text(noteApiError("s3", xhr));
      });
  });

  $("#btnInfraSaveStorage").on("click", function () {
    const mode = $("#infraStorageMode").val();
    const body = { mode: mode };
    if (mode === "slate") {
      body.s3 = {
        endpoint: $("#infraS3Endpoint").val(),
        bucket: $("#infraS3Bucket").val(),
        payloadBucket: $("#infraS3PayloadBucket").val() || null,
        accessKey: $("#infraS3AccessKey").val(),
        secretKey: $("#infraS3SecretKey").val() || "••••••••",
        region: $("#infraS3Region").val(),
      };
    }
    apiPutJSON(apiBase() + "/v1/infra/storage", body)
      .done(function (res) {
        log(res.message);
        if (res.needs_restart) showInfraRestart(res.message);
        refreshInfra();
      })
      .fail(function (xhr) {
        log(noteApiError("storage", xhr), true);
      });
  });

  $("#btnInfraCreateCluster").on("click", function () {
    apiPostJSON(apiBase() + "/v1/infra/cluster/create", {})
      .done(function (res) {
        log(res.message);
        $("#infraJoinTokenBox").removeClass("hidden");
        $("#infraJoinToken").text(res.join_token);
        $("#infraSeedUrlHint").text(
          "Seed URL for joiners: " + ($("#infraPublicUrl").val() || apiBase())
        );
        if (res.needs_restart) showInfraRestart(res.message);
        refreshInfra();
        pollClusterStatus();
      })
      .fail(function (xhr) {
        log(noteApiError("cluster", xhr), true);
      });
  });

  $("#btnInfraSyncCluster").on("click", function () {
    apiPostJSON(apiBase() + "/v1/infra/cluster/sync", {})
      .done(function (res) {
        log("Synced " + res.nodes.length + " nodes from seed");
        showInfraRestart("Restart recommended after sync.");
        refreshInfra();
        pollClusterStatus();
      })
      .fail(function (xhr) {
        log(noteApiError("sync", xhr), true);
      });
  });

  $("#btnInfraTestSeed").on("click", function () {
    apiPostJSON(apiBase() + "/v1/infra/cluster/test-peer", {
      url: $("#infraJoinSeed").val(),
    })
      .done(function (res) {
        log(res.message + (res.version ? " (v" + res.version + ")" : ""), !res.ok);
      })
      .fail(function (xhr) {
        log(noteApiError("peer", xhr), true);
      });
  });

  $("#btnInfraJoinCluster").on("click", function () {
    apiPostJSON(apiBase() + "/v1/infra/cluster/join", {
      seed_url: $("#infraJoinSeed").val(),
      join_token: $("#infraJoinTokenInput").val(),
      node_name: $("#infraJoinNodeName").val(),
      public_url: $("#infraJoinPublicUrl").val(),
    })
      .done(function (res) {
        log(res.message);
        if (res.needs_restart) showInfraRestart(res.message);
        refreshInfra();
        pollClusterStatus();
      })
      .fail(function (xhr) {
        log(noteApiError("join", xhr), true);
      });
  });

  $("#infraNodesBody").on("click", ".infra-remove-node", function () {
    const name = $(this).attr("data-remove-key");
    if (!name) return;
    if (
      !window.confirm(
        "Remove " +
          name +
          " from the cluster? That broker can join again with the join token after you restart nodes."
      )
    ) {
      return;
    }
    apiPostJSON(apiBase() + "/v1/infra/cluster/remove-node", { node_name: name })
      .done(function (res) {
        log(res.message);
        if (res.needs_restart) showInfraRestart(res.message);
        refreshInfra();
        pollClusterStatus();
      })
      .fail(function (xhr) {
        log(noteApiError("remove", xhr), true);
      });
  });

  function log(msg, isErr) {
    const $line = $("<div>").addClass(isErr ? "log-err" : "log-ok");
    $line.text("[" + new Date().toLocaleTimeString() + "] " + msg);
    $log.prepend($line);
  }

  function fmtTime(ms) {
    if (!ms) return "—";
    return new Date(ms).toLocaleString();
  }

  function trunc(s, n) {
    if (!s) return "";
    return s.length > n ? s.slice(0, n) + "…" : s;
  }

  function parseBody(raw) {
    try {
      return JSON.parse(raw);
    } catch (e) {
      return raw;
    }
  }

  function apiErrorText(xhr) {
    if (xhr.responseJSON && xhr.responseJSON.error) return xhr.responseJSON.error;
    if (xhr.status === 0) return "network error (check API URL and CORS)";
    return xhr.statusText || "request failed";
  }

  function apiPostJSON(url, payload) {
    return $.ajax({
      url: url,
      method: "POST",
      contentType: "application/json",
      data: JSON.stringify(payload),
      headers: authHeaders(),
      timeout: 30000,
    });
  }

  function optFlowId(val) {
    const s = (val || "").trim();
    return s ? s : undefined;
  }

  function mergeOutbound(payload) {
    payload.method = $("#outMethod").val();
    if ($("#outSign").is(":checked")) payload.sign = true;
    const raw = ($("#outHeaders").val() || "").trim();
    if (raw) {
      try {
        payload.headers = JSON.parse(raw);
      } catch (e) {
        log("headers must be valid JSON", true);
        return false;
      }
    }
    return true;
  }

  function parseMaxRetries($input) {
    const n = parseInt($input.val(), 10);
    return isNaN(n) ? 0 : Math.max(0, n);
  }

  function mergeRetryFields(payload, prefix) {
    const max = parseMaxRetries($("#" + prefix + "MaxRetries"));
    if (max <= 0) return payload;
    payload.max_retries = max;
    const kind = ($("#" + prefix + "RetryKind").val() || "exponential").toLowerCase();
    const initialMs = parseInt($("#" + prefix + "RetryInitialMs").val(), 10) || 500;
    const backoff = { kind: kind, initialMs: initialMs };
    if (kind === "exponential") {
      backoff.maxMs = parseInt($("#" + prefix + "RetryMaxMs").val(), 10) || 30000;
      const mult = parseFloat($("#" + prefix + "RetryMultiplier").val());
      if (!isNaN(mult) && mult > 0) backoff.multiplier = mult;
    }
    payload.retry_backoff = backoff;
    return payload;
  }

  function syncRetryBackoffUi(prefix) {
    const max = parseMaxRetries($("#" + prefix + "MaxRetries"));
    $("#" + prefix + "RetryBackoffFields").toggleClass("hidden", max <= 0);
    const exp = ($("#" + prefix + "RetryKind").val() || "exponential") === "exponential";
    $("." + prefix + "-retry-exp").toggleClass("hidden", !exp);
  }

  function bindRetryForm(prefix) {
    $("#" + prefix + "MaxRetries, #" + prefix + "RetryKind").on("input change", function () {
      syncRetryBackoffUi(prefix);
    });
    syncRetryBackoffUi(prefix);
  }

  ["pub", "enq", "cron", "ep"].forEach(bindRetryForm);

  function td(text, cls) {
    const $el = $("<td>");
    if (cls) $el.addClass(cls);
    if (text !== undefined && text !== null) $el.text(text);
    return $el;
  }

  function tdHtml(html, cls) {
    const $el = $("<td>");
    if (cls) $el.addClass(cls);
    $el.html(html);
    return $el;
  }

  function actionsCell() {
    return $("<td>").addClass("table-actions");
  }

  function emptyRow($tb, cols, text) {
    $tb.append(
      $("<tr>").append(
        $("<td>").attr("colspan", cols).addClass("table-empty").text(text)
      )
    );
  }

  function fillQueueSelects() {
    const opts = queuesCache.map(function (q) {
      return $("<option>").val(q.queue_id).text(q.queue + " · " + q.queue_id.slice(0, 8) + "…");
    });
    $("#enqQueueId").each(function () {
      const $sel = $(this).empty();
      if (!queuesCache.length) {
        $sel.append($("<option>").val("").text("— create a queue first —"));
      } else {
        opts.forEach(function ($o) {
          $sel.append($o.clone());
        });
      }
    });
  }

  function fillSidebarQueues() {
    const $ul = $("#sidebarQueues").empty();
    if (!queuesCache.length) {
      $ul.append($("<li class='app-sidebar-empty'>").text("No queues"));
      return;
    }
    queuesCache.forEach(function (q) {
      $ul.append(
        $("<li>")
          .append($("<button type='button'>").text(q.queue).attr("data-goto-queue", q.queue_id))
      );
    });
  }

  function showPanel(panelId) {
    $(".view-panel").attr("hidden", true);
    $("#" + panelId).removeAttr("hidden");
    $("#mainNav [data-panel]").removeClass("active");
    $('#mainNav [data-panel="' + panelId + '"]').addClass("active");
    onPanelShown(panelId);
  }

  $("#mainNav").on("click", "[data-panel]", function () {
    showPanel($(this).data("panel"));
  });

  $("#sidebarQueues").on("click", "button[data-goto-queue]", function () {
    const qid = $(this).data("goto-queue");
    showPanel("panel-queues");
    log("Queue selected in sidebar");
    $("#enqQueueId").val(qid);
  });

  function refreshDashboard() {
    pollHealth();
    pollClusterStatus();
    apiGetJSON(apiBase() + "/v1/queues")
      .done(function (d) {
        const n = (d.queues || []).length;
        $("#dashQueues").text(n);
        $("#dashQueuesSub").text(n ? "registered" : "none yet");
      })
      .fail(function () {
        $("#dashQueues").text("—");
        $("#dashQueuesSub").text("");
      });
    apiGetJSON(apiBase() + "/v1/flows")
      .done(function (d) {
        const n = (d.flows || []).length;
        $("#dashFlows").text(n);
        $("#dashFlowsSub").text("rate profiles");
      })
      .fail(function () {
        $("#dashFlows").text("—");
        $("#dashFlowsSub").text("");
      });
    $.when(apiGetJSON(apiBase() + "/v1/delayed"), apiGetJSON(apiBase() + "/v1/crons"))
      .done(function (a, b) {
        const n =
          ((a[0] && a[0].delayed) || []).length + ((b[0] && b[0].crons) || []).length;
        $("#dashSchedules").text(n);
        $("#dashSchedulesSub").text("cron + delayed");
      })
      .fail(function () {
        $("#dashSchedules").text("—");
        $("#dashSchedulesSub").text("");
      });
  }

  function refreshJobs() {
    const $tb = $("#jobsBody").empty();
    $.when(apiGetJSON(apiBase() + "/v1/delayed"), apiGetJSON(apiBase() + "/v1/crons"))
      .done(function (delayedRes, cronsRes) {
        const delayed = (delayedRes[0] && delayedRes[0].delayed) || [];
        const crons = (cronsRes[0] && cronsRes[0].crons) || [];

        delayed.forEach(function (d) {
          const $tr = $("<tr>");
          $tr.append(tdHtml('<span class="pill">delayed</span>'));
          $tr.append(td(d.schedule_id, "mono"));
          $tr.append(td(d.queue));
          $tr.append(td(d.key));
          $tr.append(tdHtml('<span class="pill pill-muted">pending</span>'));
          $tr.append(td(fmtTime(d.deliver_at_ms), "muted"));
          const $act = actionsCell();
          $act.append(
            $('<button type="button" class="btn-sm btn-destructive">')
              .text("Remove")
              .data("kind", "delayed")
              .data("id", d.schedule_id)
          );
          $tr.append($act);
          $tb.append($tr);
        });

        crons.forEach(function (c) {
          const spec =
            c.schedule_type === "interval"
              ? "every " + c.every_seconds + "s"
              : c.cron || "—";
          const $tr = $("<tr>");
          $tr.append(tdHtml('<span class="pill pill-outline">cron</span>'));
          $tr.append(td(c.cron_id, "mono"));
          $tr.append(
            td(
              c.destination_url
                ? trunc(c.destination_url, 48)
                : c.queue || "—",
              c.destination_url ? "mono" : ""
            )
          );
          $tr.append(td(spec, "mono"));
          const st = c.paused
            ? '<span class="pill pill-muted">paused</span>'
            : '<span class="pill">active</span>';
          $tr.append(tdHtml(st));
          $tr.append(td(fmtTime(c.next_run_at_ms), "muted"));
          const $act = actionsCell();
          if (c.paused) {
            $act.append(
              $('<button type="button" class="btn-sm btn-outline">')
                .text("Resume")
                .data("kind", "cron-resume")
                .data("id", c.cron_id)
            );
          } else {
            $act.append(
              $('<button type="button" class="btn-sm btn-outline">')
                .text("Pause")
                .data("kind", "cron-pause")
                .data("id", c.cron_id)
            );
          }
          $act.append(
            $('<button type="button" class="btn-sm btn-destructive">')
              .text("Remove")
              .data("kind", "cron-delete")
              .data("id", c.cron_id)
          );
          $tr.append($act);
          $tb.append($tr);
        });

        if (!delayed.length && !crons.length) emptyRow($tb, 7, "No scheduled jobs");
        log("Schedules refreshed");
      })
      .fail(function (xhr) {
        log("Schedules failed: " + noteApiError("schedules", xhr), true);
      });
  }

  function refreshFlows() {
    const $tb = $("#flowsBody").empty();
    apiGetJSON(apiBase() + "/v1/flows")
      .done(function (data) {
        (data.flows || []).forEach(function (f) {
          const $tr = $("<tr>");
          $tr.append(td(f.flow_id, "mono"));
          $tr.append(td(f.key));
          $tr.append(td(String(f.parallelism)));
          $tr.append(td(String(f.rate)));
          $tr.append(td(String(f.period_secs)));
          const $act = actionsCell();
          $act.append(
            $('<button type="button" class="btn-sm btn-destructive">')
              .text("Delete")
              .data("kind", "flow")
              .data("id", f.flow_id)
          );
          $tr.append($act);
          $tb.append($tr);
        });
        if (!(data.flows || []).length) emptyRow($tb, 6, "No flow profiles yet");
        log("Flows refreshed");
      })
      .fail(function (xhr) {
        log("Flows failed: " + noteApiError("flows", xhr), true);
      });
  }

  function fillGroupSelects() {
    const opts = groupsCache.map(function (g) {
      return $("<option>").val(g.group_id).text(g.name + " (" + g.group_id.slice(0, 8) + "…)");
    });
    $("#grpMemberGroup, #grpPubGroup").each(function () {
      const $sel = $(this).empty().append($("<option>").val("").text("— select group —"));
      opts.forEach(function ($o) {
        $sel.append($o.clone());
      });
    });
  }

  function refreshGroupMembers(groupId) {
    const $tb = $("#groupMembersBody").empty();
    if (!groupId) {
      emptyRow($tb, 5, "Select a group");
      return;
    }
    apiGetJSON(apiBase() + "/v1/groups/" + groupId)
      .done(function (data) {
        groupMembersCache = data.members || [];
        groupMembersCache.forEach(function (m) {
          const $tr = $("<tr>");
          $tr.append(td(m.member_id, "mono"));
          $tr.append(td(m.name, "strong"));
          $tr.append(td(trunc(m.url, 48), "mono muted"));
          $tr.append(td("p" + m.parallelism + " r" + m.rate + "/" + m.period_secs + "s", "muted"));
          const $act = actionsCell();
          $act.append(
            $('<button type="button" class="btn-sm btn-destructive">')
              .text("Delete")
              .data("kind", "group-member")
              .data("group", groupId)
              .data("id", m.member_id)
          );
          $tr.append($act);
          $tb.append($tr);
        });
        if (!groupMembersCache.length) emptyRow($tb, 5, "No members yet");
      })
      .fail(function (xhr) {
        log("Group members failed: " + noteApiError("group", xhr), true);
      });
  }

  function refreshGroups() {
    const $tb = $("#groupsBody").empty();
    apiGetJSON(apiBase() + "/v1/groups")
      .done(function (data) {
        groupsCache = data.groups || [];
        fillGroupSelects();
        groupsCache.forEach(function (g) {
          const $tr = $("<tr>");
          $tr.append(td(g.group_id, "mono"));
          $tr.append(td(g.name, "strong"));
          const $act = actionsCell();
          $act.append(
            $('<button type="button" class="btn-sm">')
              .text("Members")
              .data("goto-group", g.group_id)
          );
          $act.append(
            $('<button type="button" class="btn-sm btn-destructive">')
              .text("Delete")
              .data("kind", "group")
              .data("id", g.group_id)
          );
          $tr.append($act);
          $tb.append($tr);
        });
        if (!groupsCache.length) emptyRow($tb, 3, "No groups yet");
        log("Groups refreshed (" + groupsCache.length + ")");
        const sel = $("#grpMemberGroup").val() || $("#grpPubGroup").val();
        if (sel) refreshGroupMembers(sel);
      })
      .fail(function (xhr) {
        log("Groups failed: " + noteApiError("groups", xhr), true);
      });
  }

  function refreshEndpoints() {
    const $tb = $("#endpointsBody").empty();
    apiGetJSON(apiBase() + "/v1/queues")
      .done(function (data) {
        queuesCache = data.queues || [];
        fillQueueSelects();
        fillSidebarQueues();
        queuesCache.forEach(function (e) {
          const $tr = $("<tr>");
          $tr.append(td(e.queue_id, "mono"));
          $tr.append(td(e.queue, "strong"));
          $tr.append(td(trunc(e.url, 52), "mono muted"));
          const $act = actionsCell();
          $act.append(
            $('<button type="button" class="btn-sm btn-destructive">')
              .text("Delete")
              .data("kind", "queue")
              .data("id", e.queue_id)
          );
          $tr.append($act);
          $tb.append($tr);
        });
        if (!queuesCache.length) emptyRow($tb, 4, "No queues yet");
        log("Queues refreshed (" + queuesCache.length + ")");
        refreshDashboard();
      })
      .fail(function (xhr) {
        log("Queues failed: " + noteApiError("queues", xhr), true);
      });
  }

  function refreshDlq() {
    const $tb = $("#dlqBody").empty();
    const q = $("#dlqQueue").val() || "jobs";
    const limit = parseInt($("#dlqLimit").val(), 10) || 20;
    apiGetJSON(apiBase() + "/v1/dlq?queue=" + encodeURIComponent(q) + "&limit=" + limit)
      .done(function (data) {
        (data.messages || []).forEach(function (m) {
          const $tr = $("<tr>");
          $tr.append(td(m.message_id, "mono"));
          $tr.append(td(m.key));
          $tr.append(td(fmtTime(m.published_at_ms), "muted"));
          $tr.append(td(trunc(m.body, 60), "mono"));
          $tb.append($tr);
        });
        if (!(data.messages || []).length) emptyRow($tb, 4, 'DLQ empty for "' + q + '"');
        log("DLQ: " + (data.messages || []).length + " on " + data.dlq_topic);
      })
      .fail(function (xhr) {
        log("DLQ failed: " + ((xhr.responseJSON && xhr.responseJSON.error) || xhr.statusText), true);
      });
  }

  function onPanelShown(panelId) {
    pollHealth();
    if (panelId === "panel-dlq") refreshDlq();
    if (panelId === "panel-dashboard") refreshDashboard();
    if (panelId === "panel-infra") refreshInfra();
    if (panelId === "panel-schedules") refreshJobs();
    if (panelId === "panel-queues") refreshEndpoints();
    if (panelId === "panel-groups") refreshGroups();
    if (panelId === "panel-flows") refreshFlows();
    if (panelId === "panel-docs") syncDocsUrls();
  }

  $("#btnDashPublish").on("click", function () {
    const payload = {
      url: $("#dashPubUrl").val(),
      secret: $("#dashPubSecret").val(),
      key: "dashboard-" + Date.now(),
      body: parseBody($("#dashPubBody").val()),
      priority: 5,
    };
    if (!mergeOutbound(payload)) return;
    $.ajax({
      url: apiBase() + "/v1/publish",
      method: "POST",
      contentType: "application/json",
      data: JSON.stringify(payload),
    })
      .done(function (r) {
        log("Published " + (r.message_id || "ok"));
      })
      .fail(function (xhr) {
        log("Publish failed: " + JSON.stringify(xhr.responseJSON || xhr.statusText), true);
      });
  });

  $("#btnRefreshAll").on("click", function () {
    pollHealth();
    refreshDashboard();
    refreshInfra();
    refreshJobs();
    refreshEndpoints();
    refreshGroups();
    refreshFlows();
  });

  $("#btnRefreshDlq").on("click", refreshDlq);
  $("#btnClearLog").on("click", function () {
    $log.empty();
  });

  $("#cronScheduleType").on("change", function () {
    const interval = $(this).val() === "interval";
    $("#cronExprWrap").toggleClass("hidden", interval);
    $("#cronIntervalWrap").toggleClass("hidden", !interval);
  });

  $("#btnPublish").on("click", function () {
    const payload = {
      url: $("#pubUrl").val(),
      secret: $("#pubSecret").val(),
      key: $("#pubKey").val(),
      body: parseBody($("#pubBody").val()),
      priority: parseInt($("#pubPriority").val(), 10) || 5,
    };
    const fid = optFlowId($("#pubFlowId").val());
    if (fid) payload.flow_id = fid;
    const idem = ($("#pubIdempotency").val() || "").trim();
    if (idem) payload.idempotency_key = idem;
    mergeRetryFields(payload, "pub");
    const pubDelay = parseInt($("#pubDelay").val(), 10);
    if (pubDelay > 0) payload.delay = pubDelay;
    if (!mergeOutbound(payload)) return;
    apiPostJSON(apiBase() + "/v1/publish", payload)
      .done(function (r) {
        log("Published " + (r.message_id || (r.scheduled && r.scheduled.schedule_id)));
        refreshJobs();
      })
      .fail(function (xhr) {
        log("Publish failed: " + apiErrorText(xhr), true);
      });
  });

  function enqueueToQueue($queueSel, keyVal, bodyRaw, priority, idempotency, delayMs, doneMsg) {
    const qid = $queueSel.val();
    if (!qid) {
      log("Select a queue first", true);
      return;
    }
    const payload = {
      key: keyVal,
      body: parseBody(bodyRaw),
      priority: priority,
    };
    mergeRetryFields(payload, "enq");
    const idem = (idempotency || "").trim();
    if (idem) payload.idempotency_key = idem;
    if (delayMs != null) payload.delay = delayMs;
    if (!mergeOutbound(payload)) return;
    apiPostJSON(apiBase() + "/v1/queues/" + qid + "/enqueue", payload)
      .done(function (r) {
        log(doneMsg + (r.message_id || (r.scheduled && r.scheduled.schedule_id) || "?"));
        refreshJobs();
      })
      .fail(function (xhr) {
        log("Enqueue failed: " + apiErrorText(xhr), true);
      });
  }

  $("#btnEnqueueNow").on("click", function () {
    const delay = parseInt($("#enqDelay").val(), 10);
    enqueueToQueue(
      $("#enqQueueId"),
      $("#enqKey").val(),
      $("#enqBody").val(),
      parseInt($("#enqPriority").val(), 10) || 5,
      $("#enqIdempotency").val(),
      delay > 0 ? delay : null,
      delay > 0 ? "Delayed " : "Enqueued "
    );
  });

  $("#btnCreateFlow").on("click", function () {
    $.ajax({
      url: apiBase() + "/v1/flows",
      method: "POST",
      contentType: "application/json",
      data: JSON.stringify({
        key: $("#flowKey").val(),
        parallelism: parseInt($("#flowParallelism").val(), 10) || 1,
        rate: parseInt($("#flowRate").val(), 10) || 0,
        period_secs: parseInt($("#flowPeriod").val(), 10) || 60,
      }),
    })
      .done(function (r) {
        log("Flow created " + r.flow_id);
        $("#pubFlowId, #cronFlowId").val(r.flow_id);
        if (!$("#pubKey").val()) $("#pubKey").val($("#flowKey").val());
        refreshFlows();
      })
      .fail(function (xhr) {
        log("Flow failed: " + JSON.stringify(xhr.responseJSON || xhr.statusText), true);
      });
  });

  $("#btnCreateCron").on("click", function () {
    const url = ($("#cronUrl").val() || "").trim();
    const secret = ($("#cronSecret").val() || "").trim();
    if (!url || !secret) {
      log("Schedule needs destination URL and secret", true);
      return;
    }
    const payload = {
      url: url,
      secret: secret,
      key: $("#cronKey").val(),
      body: parseBody($("#cronBody").val()),
      priority: parseInt($("#cronPriority").val(), 10) || 5,
    };
    if ($("#cronScheduleType").val() === "interval") {
      payload.every_seconds = parseInt($("#cronEverySeconds").val(), 10) || 10;
    } else {
      payload.cron = $("#cronExpr").val();
    }
    const fid = optFlowId($("#cronFlowId").val());
    if (fid) payload.flow_id = fid;
    mergeRetryFields(payload, "cron");
    if (!mergeOutbound(payload)) return;
    apiPostJSON(apiBase() + "/v1/crons", payload)
      .done(function (r) {
        log("Schedule created " + r.cron_id);
        refreshJobs();
      })
      .fail(function (xhr) {
        log("Schedule failed: " + JSON.stringify(xhr.responseText || xhr.statusText), true);
      });
  });

  $("#grpMemberGroup").on("change", function () {
    refreshGroupMembers($(this).val());
  });

  $("#btnCreateGroup").on("click", function () {
    apiPostJSON(apiBase() + "/v1/groups", { name: $("#grpName").val() })
      .done(function (r) {
        log("Group " + r.name + " → " + r.group_id);
        refreshGroups();
      })
      .fail(function (xhr) {
        log("Group failed: " + JSON.stringify(xhr.responseJSON || xhr.statusText), true);
      });
  });

  $("#btnAddGroupMember").on("click", function () {
    const gid = $("#grpMemberGroup").val();
    if (!gid) {
      log("Select a group first", true);
      return;
    }
    apiPostJSON(apiBase() + "/v1/groups/" + gid + "/members", {
      name: $("#grpMemberName").val(),
      url: $("#grpMemberUrl").val(),
      secret: $("#grpMemberSecret").val(),
      parallelism: parseInt($("#grpMemberParallelism").val(), 10) || 1,
      rate: parseInt($("#grpMemberRate").val(), 10) || 0,
      period_secs: parseInt($("#grpMemberPeriod").val(), 10) || 60,
    })
      .done(function (r) {
        log("Member " + r.name + " → " + r.member_id);
        refreshGroupMembers(gid);
      })
      .fail(function (xhr) {
        log("Member failed: " + JSON.stringify(xhr.responseJSON || xhr.statusText), true);
      });
  });

  $("#btnGroupPublish").on("click", function () {
    const gid = $("#grpPubGroup").val();
    if (!gid) {
      log("Select a group first", true);
      return;
    }
    apiPostJSON(apiBase() + "/v1/groups/" + gid + "/publish", {
      key: $("#grpPubKey").val(),
      body: parseBody($("#grpPubBody").val()),
    })
      .done(function (r) {
        log("Group publish accepted " + r.accepted + " / " + (r.deliveries || []).length);
      })
      .fail(function (xhr) {
        log("Group publish failed: " + JSON.stringify(xhr.responseJSON || xhr.statusText), true);
      });
  });

  $("#groupsBody").on("click", "button[data-goto-group]", function () {
    const gid = $(this).data("goto-group");
    $("#grpMemberGroup").val(gid);
    refreshGroupMembers(gid);
    showPanel("panel-groups");
  });

  $("#btnCreateEndpoint").on("click", function () {
    $.ajax({
      url: apiBase() + "/v1/queues",
      method: "POST",
      contentType: "application/json",
      data: JSON.stringify(
        mergeRetryFields(
          {
            queue: $("#epQueue").val(),
            url: $("#epUrl").val(),
            secret: $("#epSecret").val(),
          },
          "ep"
        )
      ),
    })
      .done(function (r) {
        log("Queue " + r.queue + " → " + r.queue_id);
        refreshEndpoints();
      })
      .fail(function (xhr) {
        log("Queue failed: " + JSON.stringify(xhr.responseJSON || xhr.statusText), true);
      });
  });

  $("#jobsBody").on("click", "button", function () {
    const kind = $(this).data("kind");
    const id = $(this).data("id");
    let req;
    if (kind === "delayed") req = $.ajax({ url: apiBase() + "/v1/delayed/" + id, method: "DELETE" });
    else if (kind === "cron-pause") req = $.post(apiBase() + "/v1/crons/" + id + "/pause");
    else if (kind === "cron-resume") req = $.post(apiBase() + "/v1/crons/" + id + "/resume");
    else if (kind === "cron-delete") req = $.ajax({ url: apiBase() + "/v1/crons/" + id, method: "DELETE" });
    if (!req) return;
    req
      .done(function () {
        log(kind + " ok");
        refreshJobs();
      })
      .fail(function () {
        log(kind + " failed", true);
      });
  });

  $("#endpointsBody, #flowsBody, #groupsBody, #groupMembersBody").on("click", "button", function () {
    const kind = $(this).data("kind");
    const id = $(this).data("id");
    let url;
    if (kind === "flow") url = apiBase() + "/v1/flows/" + id;
    else if (kind === "group") url = apiBase() + "/v1/groups/" + id;
    else if (kind === "group-member") url = apiBase() + "/v1/groups/" + $(this).data("group") + "/members/" + id;
    else url = apiBase() + "/v1/queues/" + id;
    $.ajax({ url: url, method: "DELETE" })
      .done(function () {
        log(kind + " removed");
        if (kind === "flow") refreshFlows();
        else if (kind === "group" || kind === "group-member") refreshGroups();
        else refreshEndpoints();
      })
      .fail(function () {
        log("Delete failed", true);
      });
  });


  function showAuthOverlay(id) {
    $("#authSetup, #tokenReveal, #authGate").addClass("hidden");
    if (id) {
      $(id).removeClass("hidden");
      $("body").addClass("auth-locked");
    } else {
      $("body").removeClass("auth-locked");
    }
  }

  function saveToken(token) {
    const t = normalizeToken(token);
    if (!t) return;
    localStorage.setItem(tokenStorageKey(), t);
    $("#apiToken").val(t);
    $("#gateToken").val(t);
  }

  function clearToken() {
    localStorage.removeItem(tokenStorageKey());
    localStorage.removeItem(LEGACY_TOKEN_KEY);
    $("#apiToken").val("");
    $("#gateToken").val("");
  }

  function verifySession(token, cb) {
    const t = normalizeToken(token || $("#apiToken").val());
    if (!t) {
      cb(false, "missing API token");
      return;
    }
    $.ajax({
      url: apiBase() + "/v1/queues",
      dataType: "json",
      timeout: 8000,
      headers: { Authorization: "Bearer " + t },
    })
      .done(function () {
        cb(true, null);
      })
      .fail(function (xhr) {
        const msg =
          (xhr.responseJSON && xhr.responseJSON.error) ||
          xhr.statusText ||
          "request failed";
        cb(false, msg);
      });
  }

  function revealTokenOnce(token) {
    $("#tokenRevealValue").text(normalizeToken(token));
    showAuthOverlay("#tokenReveal");
  }

  function openGate(message) {
    const $err = $("#gateError");
    if (message) {
      $err.removeClass("hidden").text(message);
    } else {
      $err.addClass("hidden").text("");
    }
    const saved = ($("#apiToken").val() || "").trim();
    if (saved) $("#gateToken").val(saved);
    showAuthOverlay("#authGate");
  }

  let appStarted = false;

  function startApp() {
    if (appStarted) return;
    appStarted = true;
    sessionReady = true;
    apiErrorKinds.clear();
    pollHealth();
    pollClusterStatus();
    setInterval(pollHealth, 15000);
    setInterval(pollClusterStatus, 5000);
    refreshDashboard();
    refreshInfra();
    refreshJobs();
    refreshEndpoints();
    refreshFlows();
  }

  function fetchAuthConfig(cb) {
    const base = apiBase();
    $.getJSON(base + "/v1/auth/config")
      .done(function (cfg1) {
        $.getJSON(base + "/v1/auth/config")
          .done(function (cfg2) {
            if (cfg1.configured !== cfg2.configured || cfg1.mode !== cfg2.mode) {
              $("#clusterAuthBanner").removeClass("hidden");
              cb(null, "split");
              return;
            }
            cb(cfg1, null);
          })
          .fail(function () {
            cb(cfg1, null);
          });
      })
      .fail(function () {
        cb(null, "unreachable");
      });
  }

  function initAuth() {
    sessionReady = false;
    appStarted = false;
    const gen = ++authCheckGen;
    const base = apiBase();
    if (!base) {
      log("Set API URL in the top bar", true);
      openGate("Set the API URL in the top bar, then paste your token.");
      return;
    }
    fetchAuthConfig(function (cfg, err) {
      if (gen !== authCheckGen) return;
      if (err === "unreachable" || !cfg) {
        log("Cannot reach API at " + base, true);
        openGate("Cannot reach broker at " + base + ". Check API URL and that bettermq is running.");
        return;
      }
      if (err === "split") {
        openGate(
          "Load balancer is returning different auth state from different brokers. Use one bettermq process for local dev, or shared BETTERMQ_LOCAL_AUTH_FILE on cluster nodes."
        );
        return;
      }
      if (window.__BETTERMQ_EXTERNAL_AUTH__ && cfg.mode === "control_plane") {
        $("#navSettings").addClass("hidden");
        $("#navInfra").removeClass("hidden");
        showAuthOverlay(null);
        startApp();
        return;
      }
      $("#navSettings").removeClass("hidden");
      $("#navInfra").removeClass("hidden");
      if (!cfg.configured) {
        clearToken();
        $("#setupPassword, #setupPassword2").val("");
        showAuthOverlay("#authSetup");
        return;
      }
      const saved = normalizeToken($("#apiToken").val());
      if (!saved) {
        openGate();
        return;
      }
      verifySession(saved, function (ok, verr) {
        if (gen !== authCheckGen) return;
        if (ok) {
          saveToken(saved);
          showAuthOverlay(null);
          startApp();
          return;
        }
        clearToken();
        if (verr && verr.indexOf("not configured") !== -1) {
          showAuthOverlay("#authSetup");
        } else {
          openGate(verr || "Invalid API token for this broker.");
        }
      });
    });
  }

  $("#btnSetup").on("click", function () {
    const p1 = $("#setupPassword").val();
    const p2 = $("#setupPassword2").val();
    const $err = $("#setupError");
    $err.addClass("hidden").text("");
    if (p1.length < 8) {
      $err.removeClass("hidden").text("Password must be at least 8 characters.");
      return;
    }
    if (p1 !== p2) {
      $err.removeClass("hidden").text("Passwords do not match.");
      return;
    }
    $.ajax({
      url: apiBase() + "/v1/local-auth/setup",
      method: "POST",
      contentType: "application/json",
      data: JSON.stringify({ password: p1 }),
    })
      .done(function (r) {
        saveToken(r.token);
        revealTokenOnce(r.token);
      })
      .fail(function (xhr) {
        const msg =
          (xhr.responseJSON && xhr.responseJSON.error) || xhr.statusText || "Setup failed";
        if (msg.indexOf("already configured") !== -1) {
          openGate("Broker is already set up. Paste your sk_local_… token below.");
          return;
        }
        $err.removeClass("hidden").text(msg);
      });
  });

  $("#btnTokenDone").on("click", function () {
    const t = normalizeToken($("#tokenRevealValue").text());
    saveToken(t);
    verifySession(t, function (ok, err) {
      if (!ok) {
        openGate(err || "Token did not work. Regenerate in Settings.");
        return;
      }
      $("#tokenRevealValue").text("");
      showAuthOverlay(null);
      startApp();
    });
  });

  $("#btnCopyToken").on("click", function () {
    const t = $("#tokenRevealValue").text();
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(t);
      log("Token copied");
    }
  });

  function submitGateToken() {
    const t = normalizeToken($("#gateToken").val() || $("#apiToken").val());
    const $err = $("#gateError");
    $err.addClass("hidden").text("");
    if (!t) {
      $err.removeClass("hidden").text("Paste your sk_local_… token.");
      return;
    }
    if (!t.startsWith("sk_local_")) {
      $err.removeClass("hidden").text("Expected a sk_local_… token from this broker's setup.");
      return;
    }
    saveToken(t);
    verifySession(t, function (ok, err) {
      if (ok) {
        showAuthOverlay(null);
        startApp();
        return;
      }
      clearToken();
      $err.removeClass("hidden").text(err || "Invalid token for this broker.");
      log("Invalid token: " + err, true);
    });
  }

  $("#btnGateSave").on("click", submitGateToken);

  $("#gateToken").on("keydown", function (e) {
    if (e.key === "Enter") {
      e.preventDefault();
      submitGateToken();
    }
  });

  $("#btnGateSettings").on("click", function () {
    showAuthOverlay(null);
    showPanel("panel-settings");
  });

  $("#btnRegenToken").on("click", function () {
    const password = $("#regenPassword").val();
    $.ajax({
      url: apiBase() + "/v1/local-auth/regenerate",
      method: "POST",
      contentType: "application/json",
      data: JSON.stringify({ password: password }),
    })
      .done(function (r) {
        saveToken(r.token);
        $("#regenPassword").val("");
        revealTokenOnce(r.token);
        log("API token regenerated");
      })
      .fail(function (xhr) {
        log(
          "Regenerate failed: " + ((xhr.responseJSON && xhr.responseJSON.error) || xhr.statusText),
          true
        );
      });
  });

  syncDocsUrls();
  initAuth();
})();

