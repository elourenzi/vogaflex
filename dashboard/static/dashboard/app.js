const rawData = JSON.parse(
  document.getElementById("seed-data").textContent || "[]"
);

const currencyFormatter = new Intl.NumberFormat("pt-BR", {
  style: "currency",
  currency: "BRL",
  maximumFractionDigits: 0,
});

const dateFormatter = new Intl.DateTimeFormat("pt-BR", {
  dateStyle: "medium",
  timeStyle: "short",
});

const inactiveStatuses = ["perdido", "fechado", "finalizado", "cancelado"];

const state = {
  rangeDays: 7,
  status: "Todos",
  etapa: "Todos",
  departamento: "Todos",
  instancia: "Todos",
  search: "",
  selectedId: null,
};

const elements = {
  searchInput: document.getElementById("searchInput"),
  statusFilter: document.getElementById("statusFilter"),
  etapaFilter: document.getElementById("etapaFilter"),
  departamentoFilter: document.getElementById("departamentoFilter"),
  instanciaFilter: document.getElementById("instanciaFilter"),
  resultsCount: document.getElementById("resultsCount"),
  conversationList: document.getElementById("conversationList"),
  chatMessages: document.getElementById("chatMessages"),
  chatTitle: document.getElementById("chatTitle"),
  chatMeta: document.getElementById("chatMeta"),
  metricTotal: document.getElementById("metricTotal"),
  metricValor: document.getElementById("metricValor"),
  metricTicket: document.getElementById("metricTicket"),
  metricAtivos: document.getElementById("metricAtivos"),
  detailSubtitle: document.getElementById("detailSubtitle"),
  detailMain: document.getElementById("detailMain"),
  detailContact: document.getElementById("detailContact"),
  detailFunnel: document.getElementById("detailFunnel"),
  clearFilters: document.getElementById("clearFilters"),
  exportCsv: document.getElementById("exportCsv"),
  chips: document.querySelectorAll(".chip"),
};

const normalizeMoney = (value) => {
  if (typeof value === "number") return value;
  if (!value) return 0;
  let text = String(value).trim();
  const hasComma = text.includes(",");
  if (hasComma) {
    text = text.replace(/\./g, "").replace(",", ".");
  }
  const cleaned = text.replace(/[^\d.-]/g, "");
  const parsed = Number.parseFloat(cleaned);
  return Number.isFinite(parsed) ? parsed : 0;
};

const normalizeText = (value) => String(value || "").toLowerCase().trim();

const pickTimestamp = (item) =>
  new Date(item.evento_timestamp || item.data_criacao_chat || Date.now());

const events = rawData.map((item) => ({
  ...item,
  valor_orcamento: normalizeMoney(item.valor_orcamento),
}));

const groupByChat = (items) => {
  const map = new Map();
  items.forEach((item) => {
    const key = item.chat_id || item.protocolo || String(item.id);
    if (!map.has(key)) {
      map.set(key, []);
    }
    map.get(key).push(item);
  });
  return map;
};

const buildConversations = (items) => {
  const grouped = groupByChat(items);
  return [...grouped.entries()].map(([chatId, list]) => {
    const sorted = [...list].sort(
      (a, b) => pickTimestamp(a) - pickTimestamp(b)
    );
    const latest = sorted[sorted.length - 1];
    const latestBudget = [...sorted]
      .reverse()
      .find((item) => Number(item.valor_orcamento) > 0);
    return {
      ...latest,
      chat_id: chatId,
      valor_orcamento: latestBudget ? latestBudget.valor_orcamento : 0,
      timeline: sorted,
    };
  });
};

const conversations = buildConversations(events);

const uniqueSorted = (items) =>
  [...new Set(items.filter(Boolean))].sort((a, b) =>
    String(a).localeCompare(String(b))
  );

const populateSelect = (select, values) => {
  select.innerHTML = "";
  const all = document.createElement("option");
  all.value = "Todos";
  all.textContent = "Todos";
  select.appendChild(all);
  values.forEach((value) => {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = value;
    select.appendChild(option);
  });
};

const formatCurrency = (value) => currencyFormatter.format(value || 0);

const formatDate = (value) => {
  if (!value) return "--";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "--";
  return dateFormatter.format(date);
};

const parseSender = (content, fallbackClient, fallbackVendor) => {
  const text = String(content || "");
  const match = text.match(/\*([^*]+)\*/);
  if (match) {
    const name = match[1].trim();
    const cleaned = text.replace(match[0], "").trim();
    return {
      name: name || fallbackVendor || "Vendedor",
      role: "vendor",
      content: cleaned || "--",
    };
  }
  return {
    name: fallbackClient || "Cliente",
    role: "client",
    content: text.trim() || "--",
  };
};

const renderDetailList = (container, entries) => {
  container.innerHTML = "";
  entries.forEach(([label, value]) => {
    const wrapper = document.createElement("div");
    const dt = document.createElement("dt");
    dt.textContent = label;
    const dd = document.createElement("dd");
    dd.textContent = value || "--";
    wrapper.appendChild(dt);
    wrapper.appendChild(dd);
    container.appendChild(wrapper);
  });
};

const renderDetails = (item) => {
  if (!item) {
    elements.detailSubtitle.textContent = "Selecione uma conversa";
    elements.detailMain.innerHTML = "";
    elements.detailContact.innerHTML = "";
    elements.detailFunnel.innerHTML = "";
    elements.chatTitle.textContent = "Selecione uma conversa";
    elements.chatMeta.textContent = "--";
    elements.chatMessages.innerHTML = "";
    return;
  }

  elements.detailSubtitle.textContent = `${item.protocolo || item.chat_id}`;
  elements.chatTitle.textContent = item.cliente_nome || item.chat_id;
  elements.chatMeta.textContent = `${item.protocolo || item.chat_id} · ${
    item.vendedor_nome || "--"
  } · ${item.status_conversa || "--"}`;

  renderDetailList(elements.detailMain, [
    ["Status", item.status_conversa],
    ["Tipo de fluxo", item.tipo_fluxo],
    ["Valor do orcamento", formatCurrency(item.valor_orcamento)],
    ["Data de criacao", formatDate(item.data_criacao_chat)],
    ["Data de fechamento", formatDate(item.data_fechamento)],
  ]);

  renderDetailList(elements.detailContact, [
    ["Cliente", item.cliente_nome],
    ["Telefone", item.cliente_telefone],
    ["Vendedor", item.vendedor_nome],
    ["Email", item.vendedor_email],
  ]);

  renderDetailList(elements.detailFunnel, [
    ["Etapa", item.etapa_funil],
    ["Coluna Kanban", item.coluna_kanban],
    ["Departamento", item.departamento],
    ["Produto", item.produto_interesse],
    ["Motivo perda", item.motivo_perda],
  ]);
};

const applyFilters = () => {
  const search = normalizeText(state.search);
  const now = new Date();
  const rangeLimit =
    state.rangeDays > 0
      ? new Date(now.getTime() - state.rangeDays * 24 * 60 * 60 * 1000)
      : null;

  const filtered = conversations.filter((item) => {
    if (rangeLimit && pickTimestamp(item) < rangeLimit) return false;
    if (state.status !== "Todos" && item.status_conversa !== state.status)
      return false;
    if (state.etapa !== "Todos" && item.etapa_funil !== state.etapa) return false;
    if (
      state.departamento !== "Todos" &&
      item.departamento !== state.departamento
    )
      return false;
    if (state.instancia !== "Todos" && item.instancia_nome !== state.instancia)
      return false;
    if (!search) return true;

    const haystack = [
      item.protocolo,
      item.chat_id,
      item.cliente_nome,
      item.cliente_telefone,
      item.vendedor_nome,
      item.vendedor_email,
    ]
      .map(normalizeText)
      .join(" ");

    return haystack.includes(search);
  });

  renderConversationList(filtered);
  renderMetrics(filtered);

  if (!filtered.find((item) => item.chat_id === state.selectedId)) {
    state.selectedId = filtered[0]?.chat_id || null;
  }
  renderDetails(filtered.find((item) => item.chat_id === state.selectedId));
  renderChatMessages(filtered.find((item) => item.chat_id === state.selectedId));
};

const renderMetrics = (items) => {
  const total = items.length;
  const totalValue = items.reduce(
    (acc, item) => acc + (item.valor_orcamento || 0),
    0
  );
  const ticket = total ? totalValue / total : 0;
  const ativos = items.filter((item) => {
    const status = normalizeText(item.status_conversa);
    return !inactiveStatuses.includes(status);
  }).length;

  elements.metricTotal.textContent = String(total);
  elements.metricValor.textContent = formatCurrency(totalValue);
  elements.metricTicket.textContent = formatCurrency(ticket);
  elements.metricAtivos.textContent = String(ativos);

  elements.resultsCount.textContent = `${total} conversas encontradas`;
};

const renderConversationList = (items) => {
  elements.conversationList.innerHTML = "";
  if (items.length === 0) {
    const empty = document.createElement("p");
    empty.textContent = "Nenhuma conversa encontrada.";
    elements.conversationList.appendChild(empty);
    return;
  }

  items
    .slice()
    .sort((a, b) => pickTimestamp(b) - pickTimestamp(a))
    .forEach((item) => {
      const card = document.createElement("button");
      card.type = "button";
      card.className = "conversation-item";
      card.dataset.chatId = item.chat_id;
      card.setAttribute("role", "option");
      card.setAttribute(
        "aria-selected",
        item.chat_id === state.selectedId ? "true" : "false"
      );
      if (item.chat_id === state.selectedId) card.classList.add("is-active");

      const title = document.createElement("div");
      title.className = "conversation-title";
      title.innerHTML = `
        <strong>${item.cliente_nome || item.chat_id}</strong>
        <span>${formatDate(item.evento_timestamp || item.data_criacao_chat)}</span>
      `;

      const meta = document.createElement("div");
      meta.className = "conversation-meta";
      meta.innerHTML = `
        <span>${item.status_conversa || "--"}</span>
        <span>${formatCurrency(item.valor_orcamento)}</span>
      `;

      const preview = document.createElement("div");
      preview.className = "conversation-preview";
      preview.textContent = item.msg_conteudo || "Sem mensagem.";

      card.appendChild(title);
      card.appendChild(meta);
      card.appendChild(preview);

      card.addEventListener("click", () => {
        state.selectedId = item.chat_id;
        applyFilters();
      });

      elements.conversationList.appendChild(card);
    });
};

const renderChatMessages = (item) => {
  elements.chatMessages.innerHTML = "";
  if (!item) {
    const empty = document.createElement("p");
    empty.textContent = "Selecione uma conversa para ver o historico.";
    elements.chatMessages.appendChild(empty);
    return;
  }

  const timeline = (item.timeline || []).slice().sort((a, b) => {
    return pickTimestamp(a) - pickTimestamp(b);
  });

  timeline.forEach((entry) => {
    const sender = parseSender(
      entry.msg_conteudo,
      item.cliente_nome,
      item.vendedor_nome
    );
    const bubble = document.createElement("div");
    bubble.className = `message-bubble${
      sender.role === "vendor" ? " is-outgoing" : ""
    }`;

    const meta = document.createElement("div");
    meta.className = "message-meta";
    meta.textContent = `${entry.msg_tipo || "Mensagem"} · ${formatDate(
      entry.evento_timestamp || entry.data_criacao_chat
    )}`;

    const senderLabel = document.createElement("div");
    senderLabel.className = "message-sender";
    senderLabel.textContent =
      sender.role === "vendor" ? `Vendedor: ${sender.name}` : sender.name;

    const text = document.createElement("p");
    text.className = "message-text";
    text.textContent = sender.content;

    const status = document.createElement("div");
    status.className = "message-meta";
    const statusText = String(entry.msg_status_envio || "").trim();
    status.textContent =
      statusText && statusText.toLowerCase() !== "true" ? statusText : "";

    bubble.appendChild(meta);
    bubble.appendChild(senderLabel);
    bubble.appendChild(text);
    if (status.textContent) bubble.appendChild(status);

    elements.chatMessages.appendChild(bubble);
  });

  elements.chatMessages.scrollTop = elements.chatMessages.scrollHeight;
};

const exportCsv = (items) => {
  const headers = [
    "protocolo",
    "cliente",
    "vendedor",
    "etapa",
    "status",
    "valor",
    "data",
  ];
  const rows = items.map((item) => [
    item.protocolo || item.chat_id || "",
    item.cliente_nome || "",
    item.vendedor_nome || "",
    item.etapa_funil || "",
    item.status_conversa || "",
    item.valor_orcamento || 0,
    item.evento_timestamp || item.data_criacao_chat || "",
  ]);

  const lines = [
    headers.join(","),
    ...rows.map((row) =>
      row
        .map((value) => `"${String(value).replace(/"/g, '""')}"`)
        .join(",")
    ),
  ];

  const blob = new Blob([lines.join("\n")], {
    type: "text/csv;charset=utf-8;",
  });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = "orcamentos.csv";
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
};

const bindEvents = () => {
  elements.searchInput.addEventListener("input", (event) => {
    state.search = event.target.value;
    applyFilters();
  });

  elements.statusFilter.addEventListener("change", (event) => {
    state.status = event.target.value;
    applyFilters();
  });

  elements.etapaFilter.addEventListener("change", (event) => {
    state.etapa = event.target.value;
    applyFilters();
  });

  elements.departamentoFilter.addEventListener("change", (event) => {
    state.departamento = event.target.value;
    applyFilters();
  });

  elements.instanciaFilter.addEventListener("change", (event) => {
    state.instancia = event.target.value;
    applyFilters();
  });

  elements.clearFilters.addEventListener("click", () => {
    state.search = "";
    state.status = "Todos";
    state.etapa = "Todos";
    state.departamento = "Todos";
    state.instancia = "Todos";
    state.rangeDays = 7;
    elements.searchInput.value = "";
    elements.statusFilter.value = "Todos";
    elements.etapaFilter.value = "Todos";
    elements.departamentoFilter.value = "Todos";
    elements.instanciaFilter.value = "Todos";
    elements.chips.forEach((chip, index) => {
      chip.classList.toggle("is-active", index === 0);
    });
    applyFilters();
  });

  elements.chips.forEach((chip) => {
    chip.addEventListener("click", () => {
      const range = Number.parseInt(chip.dataset.range || "0", 10);
      state.rangeDays = Number.isNaN(range) ? 0 : range;
      elements.chips.forEach((button) =>
        button.classList.toggle("is-active", button === chip)
      );
      applyFilters();
    });
  });

  elements.exportCsv.addEventListener("click", () => {
    const filtered = conversations.filter((item) => {
      if (state.status !== "Todos" && item.status_conversa !== state.status)
        return false;
      if (state.etapa !== "Todos" && item.etapa_funil !== state.etapa)
        return false;
      if (
        state.departamento !== "Todos" &&
        item.departamento !== state.departamento
      )
        return false;
      if (state.instancia !== "Todos" && item.instancia_nome !== state.instancia)
        return false;
      if (state.rangeDays > 0) {
        const limit = new Date(
          Date.now() - state.rangeDays * 24 * 60 * 60 * 1000
        );
        if (pickTimestamp(item) < limit) return false;
      }
      if (state.search) {
        const haystack = [
          item.protocolo,
          item.chat_id,
          item.cliente_nome,
          item.cliente_telefone,
          item.vendedor_nome,
          item.vendedor_email,
        ]
          .map(normalizeText)
          .join(" ");
        if (!haystack.includes(normalizeText(state.search))) return false;
      }
      return true;
    });
    exportCsv(filtered);
  });
};

const init = () => {
  populateSelect(
    elements.statusFilter,
    uniqueSorted(conversations.map((item) => item.status_conversa))
  );
  populateSelect(
    elements.etapaFilter,
    uniqueSorted(conversations.map((item) => item.etapa_funil))
  );
  populateSelect(
    elements.departamentoFilter,
    uniqueSorted(conversations.map((item) => item.departamento))
  );
  populateSelect(
    elements.instanciaFilter,
    uniqueSorted(conversations.map((item) => item.instancia_nome))
  );

  bindEvents();
  applyFilters();
};

init();
