(() => {
  "use strict";

  const elements = {
    refreshButton: document.querySelector("#refresh-button"),
    workflowLoadMessage: document.querySelector("#workflow-load-message"),
    workflowList: document.querySelector("#workflow-list"),
    emptySelection: document.querySelector("#empty-selection"),
    runForm: document.querySelector("#run-form"),
    selectedWorkflowName: document.querySelector("#selected-workflow-name"),
    selectedWorkflowId: document.querySelector("#selected-workflow-id"),
    selectedWorkflowDescription: document.querySelector("#selected-workflow-description"),
    payloadInput: document.querySelector("#payload-input"),
    payloadError: document.querySelector("#payload-error"),
    runButton: document.querySelector("#run-button"),
    runningIndicator: document.querySelector("#running-indicator"),
    runningElapsed: document.querySelector("#running-elapsed"),
    runResult: document.querySelector("#run-result"),
    resultStatus: document.querySelector("#result-status"),
    resultMessage: document.querySelector("#result-message"),
    resultElapsed: document.querySelector("#result-elapsed"),
    resultWorkflow: document.querySelector("#result-workflow"),
    resultWorkflowId: document.querySelector("#result-workflow-id"),
    resultRunId: document.querySelector("#result-run-id"),
    resultStarted: document.querySelector("#result-started"),
    resultFinished: document.querySelector("#result-finished"),
    temporalUiLink: document.querySelector("#temporal-ui-link"),
    resultOutput: document.querySelector("#result-output"),
    outputHeading: document.querySelector("#output-heading"),
    outputJson: document.querySelector("#output-json"),
    requestError: document.querySelector("#request-error"),
    requestErrorMessage: document.querySelector("#request-error-message")
  };

  const state = {
    workflows: [],
    selectedWorkflow: null,
    running: false,
    timerId: null,
    runStartedAt: 0
  };

  class ApiError extends Error {
    constructor(message, status = 0) {
      super(message);
      this.name = "ApiError";
      this.status = status;
    }
  }

  async function fetchJson(url, options = {}) {
    let response;

    try {
      response = await fetch(url, {
        ...options,
        headers: {
          Accept: "application/json",
          ...(options.body ? { "Content-Type": "application/json" } : {}),
          ...options.headers
        }
      });
    } catch (error) {
      throw new ApiError("The server could not be reached. Check your connection and try again.");
    }

    let data;
    const responseText = await response.text();

    if (responseText.trim()) {
      try {
        data = JSON.parse(responseText);
      } catch (error) {
        throw new ApiError(
          response.ok
            ? "The server returned an invalid JSON response."
            : `The server returned an unreadable error response (HTTP ${response.status}).`,
          response.status
        );
      }
    }

    if (!response.ok) {
      const serverMessage = data && typeof data.error === "string" ? data.error.trim() : "";
      throw new ApiError(
        serverMessage || `The request failed with HTTP status ${response.status}.`,
        response.status
      );
    }

    if (data === undefined) {
      throw new ApiError("The server returned an empty response.", response.status);
    }

    return data;
  }

  function normalizeWorkflows(data) {
    if (!data || !Array.isArray(data.workflows)) {
      throw new ApiError("The workflow list response has an unexpected format.");
    }

    return data.workflows.map((workflow, index) => {
      if (
        !workflow ||
        typeof workflow.id !== "string" ||
        !workflow.id.trim() ||
        typeof workflow.name !== "string" ||
        !workflow.name.trim() ||
        typeof workflow.description !== "string" ||
        typeof workflow.exampleInput !== "object" ||
        workflow.exampleInput === null ||
        Array.isArray(workflow.exampleInput)
      ) {
        throw new ApiError(`Workflow entry ${index + 1} has an unexpected format.`);
      }

      return {
        id: workflow.id,
        name: workflow.name,
        description: workflow.description,
        exampleInput: workflow.exampleInput
      };
    });
  }

  async function loadWorkflows() {
    if (state.running) return;

    setLoadingState(true);
    clearRequestError();

    try {
      const data = await fetchJson("/api/workflows");
      state.workflows = normalizeWorkflows(data);
      renderWorkflowList();

      if (state.workflows.length === 0) {
        elements.workflowLoadMessage.textContent = "No workflows are currently available.";
        elements.workflowLoadMessage.hidden = false;
        clearSelection();
        return;
      }

      elements.workflowLoadMessage.hidden = true;

      const currentSelection = state.selectedWorkflow
        ? state.workflows.find((workflow) => workflow.id === state.selectedWorkflow.id)
        : null;

      if (currentSelection) {
        selectWorkflow(currentSelection, false);
      } else {
        selectWorkflow(state.workflows[0], false);
      }
    } catch (error) {
      state.workflows = [];
      clearSelection();
      renderWorkflowList();
      elements.workflowLoadMessage.textContent = getErrorMessage(error);
      elements.workflowLoadMessage.hidden = false;
    } finally {
      setLoadingState(false);
    }
  }

  function setLoadingState(isLoading) {
    elements.refreshButton.disabled = isLoading;
    elements.refreshButton.textContent = isLoading ? "Loading…" : "Refresh";

    if (isLoading) {
      elements.workflowLoadMessage.textContent = "Loading workflows…";
      elements.workflowLoadMessage.hidden = false;
    }
  }

  function renderWorkflowList() {
    const fragment = document.createDocumentFragment();

    state.workflows.forEach((workflow) => {
      const button = document.createElement("button");
      const name = document.createElement("span");
      const marker = document.createElement("span");
      const id = document.createElement("span");
      const selected = state.selectedWorkflow?.id === workflow.id;

      button.type = "button";
      button.className = "workflow-option";
      button.dataset.workflowId = workflow.id;
      button.setAttribute("aria-pressed", String(selected));
      button.disabled = state.running;

      name.className = "workflow-option-name";
      name.textContent = workflow.name;
      marker.className = "selection-mark";
      marker.textContent = selected ? "✓" : "";
      marker.setAttribute("aria-hidden", "true");
      id.className = "workflow-option-id";
      id.textContent = workflow.id;

      button.append(name, marker, id);
      button.addEventListener("click", () => selectWorkflow(workflow, true));
      fragment.append(button);
    });

    elements.workflowList.replaceChildren(fragment);
  }

  function selectWorkflow(workflow, shouldFocusPayload) {
    if (state.running) return;

    state.selectedWorkflow = workflow;
    elements.selectedWorkflowName.textContent = workflow.name;
    elements.selectedWorkflowId.textContent = workflow.id;
    elements.selectedWorkflowId.title = workflow.id;
    elements.selectedWorkflowDescription.textContent = workflow.description || "No description provided.";
    elements.payloadInput.value = JSON.stringify(workflow.exampleInput, null, 2);
    elements.emptySelection.hidden = true;
    elements.runForm.hidden = false;

    clearPayloadError();
    clearRunFeedback();
    updateWorkflowSelection();

    if (shouldFocusPayload) {
      elements.payloadInput.focus();
    }
  }

  function clearSelection() {
    state.selectedWorkflow = null;
    elements.runForm.hidden = true;
    elements.emptySelection.hidden = false;
    clearPayloadError();
    clearRunFeedback();
  }

  function updateWorkflowSelection() {
    elements.workflowList.querySelectorAll(".workflow-option").forEach((button) => {
      const selected = button.dataset.workflowId === state.selectedWorkflow?.id;
      button.setAttribute("aria-pressed", String(selected));
      const marker = button.querySelector(".selection-mark");
      if (marker) marker.textContent = selected ? "✓" : "";
    });
  }

  function validatePayload() {
    clearPayloadError();
    const rawPayload = elements.payloadInput.value.trim();

    if (!rawPayload) {
      showPayloadError("Enter a JSON payload before running the workflow.");
      return null;
    }

    try {
      return JSON.parse(rawPayload);
    } catch (error) {
      const detail = error instanceof Error ? error.message : "Invalid JSON.";
      showPayloadError(`The payload is not valid JSON. ${detail}`);
      return null;
    }
  }

  function showPayloadError(message) {
    elements.payloadError.textContent = message;
    elements.payloadError.hidden = false;
    elements.payloadInput.setAttribute("aria-invalid", "true");
    elements.payloadInput.focus();
  }

  function clearPayloadError() {
    elements.payloadError.hidden = true;
    elements.payloadError.textContent = "";
    elements.payloadInput.removeAttribute("aria-invalid");
  }

  async function runWorkflow(event) {
    event.preventDefault();

    if (state.running || !state.selectedWorkflow) return;

    const input = validatePayload();
    if (input === null) return;

    const selectedId = state.selectedWorkflow.id;
    clearRunFeedback();
    setRunningState(true);

    try {
      const data = await fetchJson("/api/workflows/run", {
        method: "POST",
        body: JSON.stringify({
          workflow: selectedId,
          input
        })
      });

      validateRunResponse(data);
      renderRunResult(data, performance.now() - state.runStartedAt);
      elements.runResult.focus({ preventScroll: true });
      elements.runResult.scrollIntoView({ behavior: prefersReducedMotion() ? "auto" : "smooth", block: "start" });
    } catch (error) {
      showRequestError(getErrorMessage(error));
    } finally {
      setRunningState(false);
    }
  }

  function validateRunResponse(data) {
    if (!data || typeof data !== "object" || Array.isArray(data)) {
      throw new ApiError("The run response has an unexpected format.");
    }

    if (typeof data.status !== "string" || !data.status.trim()) {
      throw new ApiError("The run response did not include a status.");
    }
  }

  function setRunningState(isRunning) {
    state.running = isRunning;
    elements.runButton.disabled = isRunning;
    elements.payloadInput.disabled = isRunning;
    elements.refreshButton.disabled = isRunning;
    elements.runningIndicator.hidden = !isRunning;
    elements.runButton.textContent = isRunning ? "Running…" : "Run workflow";

    elements.workflowList.querySelectorAll(".workflow-option").forEach((button) => {
      button.disabled = isRunning;
    });

    if (isRunning) {
      state.runStartedAt = performance.now();
      elements.runningElapsed.textContent = "0.0s";
      state.timerId = window.setInterval(updateRunningTimer, 100);
    } else {
      if (state.timerId !== null) {
        window.clearInterval(state.timerId);
        state.timerId = null;
      }
    }
  }

  function updateRunningTimer() {
    const elapsedMilliseconds = performance.now() - state.runStartedAt;
    elements.runningElapsed.textContent = formatClientElapsed(elapsedMilliseconds);
  }

  function renderRunResult(data, clientElapsedMilliseconds) {
    const status = data.status.trim();
    const statusKind = getStatusKind(status);

    elements.runResult.classList.remove("result-success", "result-failure", "result-warning");
    elements.resultStatus.classList.remove("status-success", "status-failure", "status-warning");
    elements.runResult.classList.add(`result-${statusKind}`);
    elements.resultStatus.classList.add(`status-${statusKind}`);
    elements.resultStatus.textContent = humanizeStatus(status);
    elements.resultMessage.textContent = getStatusMessage(statusKind);

    setText(elements.resultElapsed, formatServerElapsed(data.elapsed, clientElapsedMilliseconds));
    setText(elements.resultWorkflow, data.workflow);
    setText(elements.resultWorkflowId, data.workflowId);
    setText(elements.resultRunId, data.runId);
    setText(elements.resultStarted, formatTimestamp(data.startedAt));
    setText(elements.resultFinished, formatTimestamp(data.finishedAt));
    renderTemporalLink(data.temporalUiUrl);
    renderOutput(data);

    elements.runResult.hidden = false;
    elements.requestError.hidden = true;
  }

  function getStatusKind(status) {
    const normalized = status.toLowerCase();

    if (/fail|error|cancel|terminate|timeout/.test(normalized)) return "failure";
    if (/complete|success|succeed/.test(normalized)) return "success";
    return "warning";
  }

  function getStatusMessage(kind) {
    if (kind === "success") return "The workflow completed successfully.";
    if (kind === "failure") return "The workflow did not complete successfully. Review the failure details below.";
    return "The workflow finished with the status reported below.";
  }

  function humanizeStatus(status) {
    return status
      .replace(/[_-]+/g, " ")
      .replace(/([a-z])([A-Z])/g, "$1 $2")
      .replace(/\b\w/g, (character) => character.toUpperCase());
  }

  function formatServerElapsed(value, fallbackMilliseconds) {
    if (typeof value === "string" && value.trim()) return value;
    if (typeof value === "number" && Number.isFinite(value)) return String(value);
    return formatClientElapsed(fallbackMilliseconds);
  }

  function formatClientElapsed(milliseconds) {
    if (!Number.isFinite(milliseconds) || milliseconds < 0) return "—";
    if (milliseconds < 1000) return `${Math.round(milliseconds)}ms`;
    return `${(milliseconds / 1000).toFixed(1)}s`;
  }

  function formatTimestamp(value) {
    if (typeof value !== "string" || !value.trim()) return "—";
    const date = new Date(value);

    if (Number.isNaN(date.getTime())) return value;

    return new Intl.DateTimeFormat(undefined, {
      dateStyle: "medium",
      timeStyle: "medium"
    }).format(date);
  }

  function setText(element, value) {
    if (value === null || value === undefined || String(value).trim() === "") {
      element.textContent = "—";
      return;
    }

    element.textContent = String(value);
  }

  function renderTemporalLink(value) {
    elements.temporalUiLink.hidden = true;
    elements.temporalUiLink.removeAttribute("href");

    if (typeof value !== "string" || !value.trim()) return;

    try {
      const url = new URL(value, window.location.href);
      if (url.protocol !== "http:" && url.protocol !== "https:") return;
      elements.temporalUiLink.href = url.href;
      elements.temporalUiLink.hidden = false;
    } catch (error) {
      // An invalid or unsafe URL is intentionally not rendered as a link.
    }
  }

  function renderOutput(data) {
    const hasFailure = Object.prototype.hasOwnProperty.call(data, "failure") && data.failure !== undefined;
    const hasResult = Object.prototype.hasOwnProperty.call(data, "result") && data.result !== undefined;

    if (!hasFailure && !hasResult) {
      elements.resultOutput.hidden = true;
      elements.outputJson.textContent = "";
      return;
    }

    const output = hasFailure ? data.failure : data.result;
    elements.outputHeading.textContent = hasFailure ? "Failure" : "Result";
    elements.outputJson.textContent = prettyPrint(output);
    elements.resultOutput.hidden = false;
  }

  function prettyPrint(value) {
    if (typeof value === "string") {
      try {
        return JSON.stringify(JSON.parse(value), null, 2);
      } catch (error) {
        return JSON.stringify(value, null, 2);
      }
    }

    try {
      const serialized = JSON.stringify(value, null, 2);
      return serialized === undefined ? String(value) : serialized;
    } catch (error) {
      return String(value);
    }
  }

  function showRequestError(message) {
    elements.requestErrorMessage.textContent = message;
    elements.requestError.hidden = false;
    elements.runResult.hidden = true;
    elements.requestError.focus({ preventScroll: true });
    elements.requestError.scrollIntoView({ behavior: prefersReducedMotion() ? "auto" : "smooth", block: "start" });
  }

  function clearRequestError() {
    elements.requestError.hidden = true;
    elements.requestErrorMessage.textContent = "";
  }

  function clearRunFeedback() {
    elements.runResult.hidden = true;
    elements.requestError.hidden = true;
    elements.requestErrorMessage.textContent = "";
  }

  function getErrorMessage(error) {
    if (error instanceof Error && error.message) return error.message;
    return "An unexpected error occurred. Please try again.";
  }

  function prefersReducedMotion() {
    return window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  }

  elements.refreshButton.addEventListener("click", loadWorkflows);
  elements.runForm.addEventListener("submit", runWorkflow);
  elements.payloadInput.addEventListener("input", clearPayloadError);

  loadWorkflows();
})();
