// Swap error HTML fragments into the runs list too (4xx validation / start failures).
htmx.config.responseHandling = [
  { code: "[1235]..", swap: true },
  { code: "4..", swap: true, error: true },
  { code: "...", swap: false }
];

const workflowEditors = {
  "fan-out-policy": {
    apply: "applyFanOutInput",
    sync: "syncFanOutPayload",
    parse: "parseAndApplyFanOutPayload",
  },
  "reusable-artifacts": {
    apply: "applyReusableArtifactInput",
    sync: "syncReusableArtifactPayload",
    parse: "parseAndApplyReusableArtifactPayload",
  },
  "durable-report": {
    apply: "applyDurableReportInput",
    sync: "syncDurableReportPayload",
    parse: "parseAndApplyDurableReportPayload",
  },
};

function pageJSON(id, fallback) {
  try {
    return JSON.parse(document.getElementById(id)?.textContent || "");
  } catch (_) {
    return fallback;
  }
}

function optionValues(id) {
  return Array.from(document.getElementById(id)?.options ?? [], (option) => option.value);
}

function requireObject(value, label) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`${label} must be a JSON object.`);
  }
}

function requireOnlyKeys(value, allowed, label) {
  const unexpected = Object.keys(value).filter((key) => !allowed.includes(key));
  if (unexpected.length > 0) {
    throw new Error(`${label} contains unsupported field ${JSON.stringify(unexpected[0])}.`);
  }
}

function requireIdentifier(value, label) {
  if (typeof value !== "string" || !/^[A-Za-z0-9._-]{1,128}$/.test(value)) {
    throw new Error(`${label} must be 1-128 characters using letters, numbers, dot, underscore, or hyphen.`);
  }
}

function requireActivityVersion(value) {
  if (typeof value !== "string" || value.length === 0 || value.trim() !== value) {
    throw new Error("Activity version is required and must not have surrounding whitespace.");
  }
}

function requirePositiveSeconds(value) {
  if (typeof value !== "string" || !/^[1-9][0-9]*s$/.test(value)) {
    throw new Error("Heavy-work duration must be a positive whole number of seconds, such as 20s.");
  }
}

function workflowLab() {
  const activeRunsStorageKey = "temporal-workflow-lab.active-runs.v1";
  const fanOutProbabilityFields = [
    { key: "success", label: "Success" },
    { key: "retryableFailure", label: "Retryable failure" },
    { key: "nonRetryableFailure", label: "Non-retryable failure" },
    { key: "panic", label: "Panic" },
    { key: "startToCloseTimeout", label: "Start-to-close timeout" },
    { key: "heartbeatTimeout", label: "Heartbeat timeout" },
  ];
  const workflows = pageJSON("workflow-catalog", []);
  const pageConfig = pageJSON("page-config", {});
  const maxEventRuns = Number.isInteger(pageConfig.maxEventRuns) && pageConfig.maxEventRuns > 0
    ? pageConfig.maxEventRuns
    : 1;
  const fanOutExample = workflows.find((workflow) => workflow.id === "fan-out-policy")?.exampleInput;

  return {
    workflows,
    selectedId: "",
    selectedName: "",
    selectedDescription: "",
    payload: "{\n}",
    payloadError: "",
    fanOut: {
      policy: "",
      campaignType: "",
      activityCount: "",
      seed: "",
      probabilities: Object.fromEntries(fanOutProbabilityFields.map(({ key }) => [key, ""])),
    },
    fanOutProbabilityFields,
    reusableArtifacts: {
      experimentId: "",
      activityVersion: "",
      heavyWorkSeconds: "",
      failureCase: "",
      failureTargetActivity: "",
    },
    durableReport: {
      experimentId: "",
      reportId: "",
      activityVersion: "",
      heavyWorkSeconds: "",
      failureCase: "",
    },
    starting: 0,
    startingMatrix: false,
    activeRuns: [],
    hasCompletedRuns: false,
    refreshing: false,
    emptyCatalogMessage: "No workflows are currently available.",
    now: Date.now(),
    _timer: null,
    _eventSource: null,
    _runStartedHandler: null,

    get inflightLabel() {
      const count = this.activeRuns.length + this.starting;
      return count === 1 ? "1 run in flight" : `${count} runs in flight`;
    },

    init() {
      if (this.workflows.length > 0) {
        this.selectById(this.workflows[0].id);
      }
      this.activeRuns = this.loadActiveRuns();
      this._runStartedHandler = (event) => this.registerRun(event.detail?.value ?? event.detail);
      document.body.addEventListener("runStarted", this._runStartedHandler);
      this._timer = window.setInterval(() => {
        this.now = Date.now();
        this.updateElapsedTimes();
      }, 250);
      this.connectEvents();
    },

    destroy() {
      if (this._timer) window.clearInterval(this._timer);
      if (this._eventSource) this._eventSource.close();
      if (this._runStartedHandler) document.body.removeEventListener("runStarted", this._runStartedHandler);
    },

    selectById(id) {
      const workflow = this.workflows.find((item) => item.id === id);
      if (!workflow) return;
      this.selectedId = workflow.id;
      this.selectedName = workflow.name;
      this.selectedDescription = workflow.description || "No description provided.";
      const editor = workflowEditors[workflow.id];
      if (!editor) {
        this.payload = JSON.stringify(workflow.exampleInput ?? {}, null, 2);
        this.payloadError = "";
        return;
      }
      try {
        this[editor.apply](workflow.exampleInput);
        this[editor.sync]();
      } catch (error) {
        const detail = error instanceof Error ? error.message : "The example input is invalid.";
        this.payload = JSON.stringify(workflow.exampleInput ?? {}, null, 2);
        this.payloadError = `The registered example input is invalid. ${detail}`;
      }
    },

    fanOutInputFromControls() {
      const campaign = {
        type: this.fanOut.campaignType,
        activityCount: this.fanOut.activityCount === "" ? 0 : Number(this.fanOut.activityCount),
        seed: String(this.fanOut.seed),
      };
      if (campaign.type === "FAULT_CAMPAIGN_TYPE_MIXED_V1") {
        campaign.backgroundProbabilities = Object.fromEntries(
          fanOutProbabilityFields.map(({ key }) => [
            key,
            this.fanOut.probabilities[key] === "" ? 0 : Number(this.fanOut.probabilities[key]),
          ]),
        );
      }
      return { policy: this.fanOut.policy, campaign };
    },

    normalizeFanOutInput(input) {
      requireObject(input, "The fan-out payload");
      requireOnlyKeys(input, ["policy", "campaign"], "The fan-out payload");
      if (!optionValues("fan-out-policy-select").includes(input.policy)) {
        throw new Error("Choose a supported aggregation policy.");
      }

      const campaign = input.campaign;
      requireObject(campaign, "Campaign");
      requireOnlyKeys(campaign, ["type", "activityCount", "seed", "backgroundProbabilities"], "Campaign");
      if (!optionValues("fan-out-campaign-select").includes(campaign.type)) {
        throw new Error("Choose the all-success V1 or mixed V1 campaign.");
      }

      if (!Number.isInteger(campaign.activityCount)) {
        throw new Error("Activity count must be a whole number.");
      }
      const minimumCount = campaign.type === "FAULT_CAMPAIGN_TYPE_MIXED_V1" ? 6 : 1;
      if (campaign.activityCount < minimumCount || campaign.activityCount > 1000) {
        throw new Error(`Activity count must be between ${minimumCount} and 1,000 for this campaign.`);
      }

      if (typeof campaign.seed !== "string" || !/^-?\d+$/.test(campaign.seed)) {
        throw new Error("Seed must be a decimal integer encoded as a JSON string.");
      }
      const seed = BigInt(campaign.seed);
      if (seed < -9223372036854775808n || seed > 9223372036854775807n) {
        throw new Error("Seed must fit in a signed 64-bit integer.");
      }

      const normalizedCampaign = {
        type: campaign.type,
        activityCount: campaign.activityCount,
        seed: campaign.seed,
      };
      if (campaign.type === "FAULT_CAMPAIGN_TYPE_ALL_SUCCESS_V1") {
        if (Object.prototype.hasOwnProperty.call(campaign, "backgroundProbabilities")) {
          throw new Error("All-success campaigns do not accept background probabilities.");
        }
      } else {
        const probabilities = campaign.backgroundProbabilities;
        requireObject(probabilities, "Background probabilities");
        const probabilityKeys = fanOutProbabilityFields.map(({ key }) => key);
        requireOnlyKeys(probabilities, probabilityKeys, "Background probabilities");
        let total = 0;
        for (const { key, label } of fanOutProbabilityFields) {
          const value = probabilities[key];
          if (!Number.isInteger(value) || value < 0) {
            throw new Error(`${label} probability must be a non-negative whole number.`);
          }
          total += value;
        }
        if (total !== 100) {
          throw new Error(`Mixed background probabilities must total 100; current total is ${total}.`);
        }
        normalizedCampaign.backgroundProbabilities = Object.fromEntries(
          probabilityKeys.map((key) => [key, probabilities[key]]),
        );
      }

      return { policy: input.policy, campaign: normalizedCampaign };
    },

    applyFanOutInput(input) {
      const normalized = this.normalizeFanOutInput(input);
      this.fanOut.policy = normalized.policy;
      this.fanOut.campaignType = normalized.campaign.type;
      this.fanOut.activityCount = String(normalized.campaign.activityCount);
      this.fanOut.seed = normalized.campaign.seed;
      const probabilities = normalized.campaign.backgroundProbabilities
        ?? fanOutExample?.campaign?.backgroundProbabilities
        ?? this.fanOut.probabilities;
      for (const { key } of fanOutProbabilityFields) {
        this.fanOut.probabilities[key] = String(probabilities[key]);
      }
      return normalized;
    },

    syncFanOutPayload() {
      const input = this.fanOutInputFromControls();
      this.payload = JSON.stringify(input, null, 2);
      try {
        this.normalizeFanOutInput(input);
        this.payloadError = "";
      } catch (error) {
        const detail = error instanceof Error ? error.message : "The configuration is invalid.";
        this.payloadError = `Fan-out configuration: ${detail}`;
      }
    },

    parseAndApplyFanOutPayload() {
      const raw = this.payload.trim();
      if (!raw) {
        this.payloadError = "Enter a fan-out JSON payload before starting a run.";
        return null;
      }
      try {
        const normalized = this.applyFanOutInput(JSON.parse(raw));
        this.payload = JSON.stringify(normalized, null, 2);
        this.payloadError = "";
        return normalized;
      } catch (error) {
        const detail = error instanceof Error ? error.message : "The configuration is invalid.";
        this.payloadError = `Fan-out configuration: ${detail}`;
        return null;
      }
    },

    reusableArtifactInputFromControls() {
      return {
        experimentId: this.reusableArtifacts.experimentId,
        activityVersion: this.reusableArtifacts.activityVersion,
        heavyWorkDuration: `${this.reusableArtifacts.heavyWorkSeconds}s`,
        failureCase: this.reusableArtifacts.failureCase,
        failureTargetActivity: this.reusableArtifacts.failureTargetActivity,
      };
    },

    normalizeReusableArtifactInput(input) {
      requireObject(input, "The reusable artifact payload");
      requireOnlyKeys(
        input,
        ["experimentId", "activityVersion", "heavyWorkDuration", "failureCase", "failureTargetActivity"],
        "The reusable artifact payload",
      );
      requireIdentifier(input.experimentId, "Experiment ID");
      requireActivityVersion(input.activityVersion);
      requirePositiveSeconds(input.heavyWorkDuration);
      if (!optionValues("reusable-artifact-failure-case").includes(input.failureCase)) {
        throw new Error("Choose no fault, failure before publication, or failure after publication.");
      }
      if (!optionValues("reusable-artifact-failure-target").includes(input.failureTargetActivity)) {
        throw new Error("Choose a supported artifact failure target.");
      }
      return {
        experimentId: input.experimentId,
        activityVersion: input.activityVersion,
        heavyWorkDuration: input.heavyWorkDuration,
        failureCase: input.failureCase,
        failureTargetActivity: input.failureTargetActivity,
      };
    },

    applyReusableArtifactInput(input) {
      const normalized = this.normalizeReusableArtifactInput(input);
      this.reusableArtifacts.experimentId = normalized.experimentId;
      this.reusableArtifacts.activityVersion = normalized.activityVersion;
      this.reusableArtifacts.heavyWorkSeconds = normalized.heavyWorkDuration.slice(0, -1);
      this.reusableArtifacts.failureCase = normalized.failureCase;
      this.reusableArtifacts.failureTargetActivity = normalized.failureTargetActivity;
      return normalized;
    },

    syncReusableArtifactPayload() {
      const input = this.reusableArtifactInputFromControls();
      this.payload = JSON.stringify(input, null, 2);
      try {
        this.normalizeReusableArtifactInput(input);
        this.payloadError = "";
      } catch (error) {
        const detail = error instanceof Error ? error.message : "The configuration is invalid.";
        this.payloadError = `Reusable artifact configuration: ${detail}`;
      }
    },

    parseAndApplyReusableArtifactPayload() {
      const raw = this.payload.trim();
      if (!raw) {
        this.payloadError = "Enter a reusable artifact JSON payload before starting a run.";
        return null;
      }
      try {
        const normalized = this.applyReusableArtifactInput(JSON.parse(raw));
        this.payload = JSON.stringify(normalized, null, 2);
        this.payloadError = "";
        return normalized;
      } catch (error) {
        const detail = error instanceof Error ? error.message : "The configuration is invalid.";
        this.payloadError = `Reusable artifact configuration: ${detail}`;
        return null;
      }
    },

    durableReportInputFromControls() {
      return {
        experimentId: this.durableReport.experimentId,
        reportId: this.durableReport.reportId,
        activityVersion: this.durableReport.activityVersion,
        heavyWorkDuration: `${this.durableReport.heavyWorkSeconds}s`,
        failureCase: this.durableReport.failureCase,
      };
    },

    normalizeDurableReportInput(input) {
      requireObject(input, "The durable report payload");
      requireOnlyKeys(
        input,
        ["experimentId", "reportId", "activityVersion", "heavyWorkDuration", "failureCase"],
        "The durable report payload",
      );
      requireIdentifier(input.experimentId, "Experiment ID");
      requireIdentifier(input.reportId, "Report ID");
      requireActivityVersion(input.activityVersion);
      requirePositiveSeconds(input.heavyWorkDuration);
      if (!optionValues("durable-report-failure-case").includes(input.failureCase)) {
        throw new Error("Choose no fault, aggregation retry, persistence failure before commit, or persistence failure after commit.");
      }
      return {
        experimentId: input.experimentId,
        reportId: input.reportId,
        activityVersion: input.activityVersion,
        heavyWorkDuration: input.heavyWorkDuration,
        failureCase: input.failureCase,
      };
    },

    applyDurableReportInput(input) {
      const normalized = this.normalizeDurableReportInput(input);
      this.durableReport.experimentId = normalized.experimentId;
      this.durableReport.reportId = normalized.reportId;
      this.durableReport.activityVersion = normalized.activityVersion;
      this.durableReport.heavyWorkSeconds = normalized.heavyWorkDuration.slice(0, -1);
      this.durableReport.failureCase = normalized.failureCase;
      return normalized;
    },

    syncDurableReportPayload() {
      const input = this.durableReportInputFromControls();
      this.payload = JSON.stringify(input, null, 2);
      try {
        this.normalizeDurableReportInput(input);
        this.payloadError = "";
      } catch (error) {
        const detail = error instanceof Error ? error.message : "The configuration is invalid.";
        this.payloadError = `Durable report configuration: ${detail}`;
      }
    },

    parseAndApplyDurableReportPayload() {
      const raw = this.payload.trim();
      if (!raw) {
        this.payloadError = "Enter a durable report JSON payload before starting a run.";
        return null;
      }
      try {
        const normalized = this.applyDurableReportInput(JSON.parse(raw));
        this.payload = JSON.stringify(normalized, null, 2);
        this.payloadError = "";
        return normalized;
      } catch (error) {
        const detail = error instanceof Error ? error.message : "The configuration is invalid.";
        this.payloadError = `Durable report configuration: ${detail}`;
        return null;
      }
    },

    async refreshCatalog() {
      this.refreshing = true;
      try {
        const response = await fetch("/api/workflows", { headers: { Accept: "application/json" } });
        if (!response.ok) throw new Error("refresh failed");
        const data = await response.json();
        if (!data || !Array.isArray(data.workflows)) throw new Error("bad catalog");
        this.workflows = data.workflows;
        if (this.workflows.length === 0) {
          this.selectedId = "";
          this.emptyCatalogMessage = "No workflows are currently available.";
          return;
        }
        if (!this.workflows.find((item) => item.id === this.selectedId)) {
          this.selectById(this.workflows[0].id);
        }
      } catch (_) {
        this.emptyCatalogMessage = "Could not refresh workflows.";
      } finally {
        this.refreshing = false;
      }
    },

    onSubmit(event) {
      this.payloadError = "";
      const editor = workflowEditors[this.selectedId];
      if (editor) {
        if (!this[editor.parse]()) event.preventDefault();
        return;
      }

      const raw = this.payload.trim();
      if (!raw) {
        event.preventDefault();
        this.payloadError = "Enter a JSON payload before running the workflow.";
        return;
      }
      try {
        JSON.parse(raw);
      } catch (error) {
        event.preventDefault();
        const detail = error instanceof Error ? error.message : "Invalid JSON.";
        this.payloadError = `The payload is not valid JSON. ${detail}`;
      }
    },

    onConfigRequest(event) {
      event.detail.parameters.workflow = this.selectedId;
      event.detail.parameters.input = this.payload;
    },

    onBeforeRequest() {
      this.starting += 1;
    },

    onAfterRequest() {
      this.starting = Math.max(0, this.starting - 1);
      this.refreshCompletedState();
    },

    onSendError() {
      this.payloadError = "The server could not be reached. Check your connection and try again.";
    },

    async runAllPolicies() {
      if (this.selectedId !== "fan-out-policy" || this.startingMatrix) return;
      const baseInput = this.parseAndApplyFanOutPayload();
      if (!baseInput) return;

      this.startingMatrix = true;
      const policies = optionValues("fan-out-policy-select");
      const failures = [];
      let started = 0;
      try {
        for (const policy of policies) {
          this.starting += 1;
          try {
            const input = {
              policy,
              campaign: JSON.parse(JSON.stringify(baseInput.campaign)),
            };
            const response = await fetch("/api/workflows/run", {
              method: "POST",
              headers: {
                Accept: "application/json",
                "Content-Type": "application/json",
              },
              body: JSON.stringify({ workflow: "fan-out-policy", input }),
            });
            const body = await response.json().catch(() => null);
            if (!response.ok) {
              throw new Error(body?.error || `The server returned HTTP ${response.status}.`);
            }
            if (!body?.workflowId || !body?.runId) {
              throw new Error("The server returned an incomplete run descriptor.");
            }
            this.registerRun(body);
            started += 1;
          } catch (error) {
            const label = this.humanizeEnum(policy, "AGGREGATION_POLICY_") || policy;
            const detail = error instanceof Error ? error.message : "The run could not be started.";
            failures.push(`${label}: ${detail}`);
          } finally {
            this.starting = Math.max(0, this.starting - 1);
          }
        }
      } finally {
        this.startingMatrix = false;
      }

      if (failures.length > 0) {
        const summary = started === 0
          ? "No policy runs were started."
          : `Started ${started} of ${policies.length} policy runs; already-started runs remain active.`;
        this.payloadError = `${summary} ${failures.join(" ")}`;
      }
    },

    registerRun(detail) {
      if (!detail || !detail.workflowId || !detail.runId) return;
      const descriptor = {
        workflow: detail.workflow,
        workflowName: detail.workflowName || detail.workflow,
        status: "running",
        workflowId: detail.workflowId,
        runId: detail.runId,
        startedAt: detail.startedAt,
        temporalUiUrl: detail.temporalUiUrl,
      };
      const key = this.runKey(descriptor);
      this.activeRuns = [descriptor, ...this.activeRuns.filter((run) => this.runKey(run) !== key)].slice(0, maxEventRuns);
      this.persistActiveRuns();
      // attached describes this start, not the run being monitored, so it stays out of the
      // stored descriptor and is shown once against the card it produced.
      const card = this.ensureRunCard(descriptor);
      if (card && detail.attached) {
        card.querySelector("[data-run-kicker]").textContent = "Joined in flight";
        card.querySelector("[data-run-message]").textContent =
          "This request joined a run already in flight for the same business key.";
      }
      this.connectEvents();
    },

    connectEvents() {
      if (this._eventSource) {
        this._eventSource.close();
        this._eventSource = null;
      }
      if (this.activeRuns.length === 0) return;

      const url = new URL("/api/runs/events", window.location.origin);
      for (const run of this.activeRuns.slice(0, maxEventRuns)) {
        url.searchParams.append("run", JSON.stringify(run));
      }
      const source = new EventSource(url);
      source.addEventListener("run", (event) => this.applyRunEvent(JSON.parse(event.data)));
      source.addEventListener("monitorError", (event) => this.applyMonitorError(JSON.parse(event.data)));
      source.onerror = () => this.markStreamsReconnecting();
      this._eventSource = source;
    },

    applyRunEvent(event) {
      const card = this.ensureRunCard(event);
      if (!card) return;
      if (event.operationStatus) {
        this.applyOperationStatus(card, event.operationStatus);
      }
      if (event.runResponse) {
        this.applyTerminalResponse(card, event.runResponse);
      }
    },

    applyOperationStatus(card, status) {
      const revision = Number(status.revision || 0);
      if (revision > 0 && Number(card.dataset.revision || 0) >= revision) return;
      card.dataset.revision = String(revision);
      card.querySelector("[data-run-status]").textContent = this.humanizeEnum(status.state, "OPERATION_STATE_") || "Running";
      card.querySelector("[data-run-message]").textContent = status.message || "The workflow is running.";
      card.querySelector("[data-run-step]").textContent = status.currentStep || status.phase || "Running";

      const percent = Math.max(0, Math.min(100, Number(status.progress?.percent || 0)));
      card.querySelector("[data-run-percent]").textContent = `${Math.round(percent)}%`;
      const bar = card.querySelector("[data-run-progress-bar]");
      bar.value = percent;
      bar.textContent = `${Math.round(percent)}%`;

      // Controls come from the running Workflow, never from the catalog: a job that is
      // already shutting down withdraws the action rather than offering it again.
      const actions = card.querySelector("[data-run-actions]");
      if (actions) {
        const offered = Array.isArray(status.availableActions) ? status.availableActions : [];
        actions.hidden = card.dataset.terminal === "true" || !offered.includes("OPERATION_ACTION_CANCEL");
      }
    },

    bindRunActions(card) {
      if (!card || card.dataset.actionsBound === "true") return;
      card.dataset.actionsBound = "true";
      const button = card.querySelector("[data-run-cancel]");
      if (button) button.addEventListener("click", () => this.cancelRun(card));
    },

    async cancelRun(card) {
      const button = card.querySelector("[data-run-cancel]");
      if (button) button.disabled = true;
      try {
        const response = await fetch("/api/runs/cancel", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            workflow: card.dataset.workflow,
            workflowId: card.dataset.workflowId,
            runId: card.dataset.runId,
          }),
        });
        if (!response.ok) {
          const body = await response.json().catch(() => ({}));
          card.querySelector("[data-run-message]").textContent = body.error || "The run could not be cancelled.";
          if (button) button.disabled = false;
        }
      } catch {
        if (button) button.disabled = false;
      }
    },

    applyTerminalResponse(card, response) {
      const succeeded = response.status === "completed";
      card.dataset.terminal = "true";
      const actionsHost = card.querySelector("[data-run-actions]");
      if (actionsHost) actionsHost.hidden = true;
      card.classList.remove("result-pending", "result-warning", "result-success", "result-failure");
      card.classList.add(succeeded ? "result-success" : "result-failure");

      const badge = card.querySelector("[data-run-status]");
      badge.className = `status-badge ${succeeded ? "status-success" : "status-failure"}`;
      badge.textContent = succeeded ? "Completed" : "Failed";
      card.querySelector("[data-run-message]").textContent = succeeded
        ? "The workflow completed successfully."
        : "The workflow did not complete successfully. Review the failure details below.";
      card.querySelector("[data-run-step]").textContent = succeeded ? "Completed" : "Failed";
      card.querySelector("[data-run-percent]").textContent = "100%";
      card.querySelector("[data-run-progress-bar]").value = 100;
      card.querySelector("[data-run-elapsed]").textContent = response.elapsed || "—";
      card.querySelector("[data-run-finished]").textContent = this.formatDateTime(response.finishedAt);

      const output = response.failure ?? response.result;
      if (output != null) {
        card.querySelector("[data-run-output-heading]").textContent = response.failure ? "Failure" : "Result";
        card.querySelector("[data-run-output]").textContent = JSON.stringify(output, null, 2);
        card.querySelector("[data-run-output-block]").hidden = false;
      }

      const key = `${response.workflowId}:${response.runId}`;
      this.activeRuns = this.activeRuns.filter((run) => this.runKey(run) !== key);
      this.persistActiveRuns();
      this.refreshCompletedState();
      if (this.activeRuns.length === 0 && this._eventSource) {
        this._eventSource.close();
        this._eventSource = null;
      }
    },

    applyMonitorError(event) {
      const card = this.ensureRunCard(event);
      if (!card) return;
      card.querySelector("[data-run-message]").textContent = event.error || "Live status is temporarily unavailable. Reconnecting…";
    },

    markStreamsReconnecting() {
      for (const run of this.activeRuns) {
        const card = document.getElementById(`run-${run.runId}`);
        if (card && card.dataset.terminal !== "true") {
          card.querySelector("[data-run-message]").textContent = "Live status is temporarily unavailable. Reconnecting…";
        }
      }
    },

    ensureRunCard(run) {
      if (!run?.runId) return null;
      let card = document.getElementById(`run-${run.runId}`);
      if (card) {
        this.bindRunActions(card);
        return card;
      }

      const template = document.getElementById("live-run-card-template");
      const fragment = template.content.cloneNode(true);
      card = fragment.querySelector("article");
      card.id = `run-${run.runId}`;
      card.dataset.workflow = run.workflow;
      card.dataset.workflowId = run.workflowId;
      card.dataset.runId = run.runId;
      card.querySelector("[data-run-name]").textContent = run.workflowName || run.workflow;
      card.querySelector("[data-run-workflow]").textContent = run.workflow;
      card.querySelector("[data-run-workflow-id]").textContent = run.workflowId;
      card.querySelector("[data-run-id]").textContent = run.runId;
      card.querySelector("[data-run-started]").textContent = this.formatDateTime(run.startedAt);
      const link = card.querySelector("[data-run-temporal-link]");
      link.href = run.temporalUiUrl || "#";
      document.getElementById("runs-list").prepend(fragment);
      const attached = document.getElementById(`run-${run.runId}`);
      this.bindRunActions(attached);
      return attached;
    },

    updateElapsedTimes() {
      for (const run of this.activeRuns) {
        const card = document.getElementById(`run-${run.runId}`);
        if (!card || card.dataset.terminal === "true") continue;
        const started = Date.parse(run.startedAt);
        if (Number.isFinite(started)) {
          card.querySelector("[data-run-elapsed]").textContent = this.formatElapsed(this.now - started);
        }
      }
    },

    clearCompleted() {
      const list = document.getElementById("runs-list");
      if (list) {
        list.querySelectorAll('[data-terminal="true"], .error-card').forEach((card) => card.remove());
      }
      this.refreshCompletedState();
    },

    refreshCompletedState() {
      const list = document.getElementById("runs-list");
      this.hasCompletedRuns = !!list?.querySelector('[data-terminal="true"], .error-card');
    },

    runKey(run) {
      return `${run.workflowId}:${run.runId}`;
    },

    loadActiveRuns() {
      try {
        const runs = JSON.parse(sessionStorage.getItem(activeRunsStorageKey) || "[]");
        return Array.isArray(runs) ? runs.filter((run) => run.workflowId && run.runId).slice(0, maxEventRuns) : [];
      } catch (_) {
        return [];
      }
    },

    persistActiveRuns() {
      sessionStorage.setItem(activeRunsStorageKey, JSON.stringify(this.activeRuns));
    },

    formatDateTime(value) {
      const date = new Date(value);
      return Number.isFinite(date.getTime()) ? date.toLocaleString() : "—";
    },

    humanizeEnum(value, prefix) {
      return String(value || "")
        .replace(prefix, "")
        .toLowerCase()
        .split("_")
        .filter(Boolean)
        .map((part) => part[0].toUpperCase() + part.slice(1))
        .join(" ");
    },

    formatElapsed(ms) {
      if (!Number.isFinite(ms) || ms < 0) return "0ms";
      if (ms < 1000) return `${Math.round(ms)}ms`;
      if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
      return `${Math.floor(ms / 60000)}m ${Math.round((ms % 60000) / 1000)}s`;
    },
  };
}
