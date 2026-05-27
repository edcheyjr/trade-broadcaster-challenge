const consumersEl = document.querySelector("#consumers");
const consumerTemplate = document.querySelector("#consumerTemplate");
const addConsumerButton = document.querySelector("#addConsumerButton");
const broadcasterUrlInput = document.querySelector("#broadcasterUrlInput");
const providerUrlInput = document.querySelector("#providerUrlInput");
const symbolsInput = document.querySelector("#symbolsInput");

let nextConsumerId = 1;

function parseSymbols() {
  return symbolsInput.value
    .split(",")
    .map((symbol) => symbol.trim())
    .filter(Boolean);
}

function getFirstSymbol() {
  const [firstSymbol] = parseSymbols();
  return firstSymbol;
}

function formatPayload(payload) {
  return JSON.stringify(payload, null, 2);
}

function addLog(container, direction, label, payload) {
  const event = document.createElement("div");
  event.className = `event ${direction}`;
  event.textContent = `[${new Date().toLocaleTimeString()}] ${label}\n${payload}`;
  container.appendChild(event);
  container.scrollTop = container.scrollHeight;
}

function setStatus(statusEl, value) {
  statusEl.className = `status ${value}`;
  statusEl.textContent = value;
}

function createConsumer() {
  const id = nextConsumerId;
  nextConsumerId += 1;

  const fragment = consumerTemplate.content.cloneNode(true);
  const consumerEl = fragment.querySelector(".consumer");
  const titleEl = fragment.querySelector("h2");
  const statusEl = fragment.querySelector(".status");
  const eventsEl = fragment.querySelector(".events");

  let socket = null;

  titleEl.textContent = `Consumer ${id}`;

  function logIncoming(rawMessage) {
    let parsed;
    try {
      parsed = JSON.parse(rawMessage);
    } catch {
      addLog(eventsEl, "in", "message event", rawMessage);
      return;
    }

    addLog(eventsEl, "in", "message event", formatPayload(parsed));
  }

  function sendCommand(payload) {
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      addLog(eventsEl, "error", "not sent", "Consumer is not connected.");
      return;
    }

    socket.send(JSON.stringify(payload));
    addLog(eventsEl, "out", "sent command", formatPayload(payload));
  }

  function connect() {
    if (socket && socket.readyState !== WebSocket.CLOSED) {
      addLog(eventsEl, "error", "connect skipped", "Consumer already has a socket.");
      return;
    }

    const url = broadcasterUrlInput.value.trim();
    setStatus(statusEl, "connecting");
    addLog(eventsEl, "out", "connecting", url);

    socket = new WebSocket(url);

    socket.addEventListener("open", () => {
      setStatus(statusEl, "open");
      addLog(eventsEl, "in", "open event", `Connected to ${url}`);
    });

    socket.addEventListener("message", (event) => {
      logIncoming(event.data);
    });

    socket.addEventListener("error", () => {
      addLog(eventsEl, "error", "error event", "WebSocket error fired.");
    });

    socket.addEventListener("close", (event) => {
      setStatus(statusEl, "closed");
      addLog(
        eventsEl,
        "error",
        "close event",
        `Closed with code ${event.code}${event.reason ? `: ${event.reason}` : ""}`
      );
    });
  }

  function disconnect() {
    if (!socket) {
      return;
    }

    socket.close();
  }

  consumerEl.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-action]");
    if (!button) {
      return;
    }

    const action = button.dataset.action;

    if (action === "connect") {
      connect();
      return;
    }

    if (action === "disconnect") {
      disconnect();
      return;
    }

    if (action === "add-provider") {
      sendCommand({
        action: "add-provider",
        host: providerUrlInput.value.trim(),
        symbols: parseSymbols(),
      });
      return;
    }

    if (action === "reserve-symbol") {
      const symbol = getFirstSymbol();
      if (!symbol) {
        addLog(eventsEl, "error", "not sent", "No symbol is configured.");
        return;
      }

      sendCommand({
        action: "reserve-symbol",
        symbol,
      });
      return;
    }

    if (action === "clear-prices") {
      sendCommand({ action: "clear-prices" });
      return;
    }

    if (action === "clear-providers") {
      sendCommand({ action: "clear-providers" });
      return;
    }

    if (action === "clear-log") {
      eventsEl.replaceChildren();
    }
  });

  consumersEl.appendChild(fragment);
}

addConsumerButton.addEventListener("click", createConsumer);

createConsumer();
