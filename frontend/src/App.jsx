import { useEffect, useMemo, useState } from "react";

const currencyFormatter = new Intl.NumberFormat("pt-BR", {
  style: "currency",
  currency: "BRL",
  maximumFractionDigits: 0,
});

const countFormatter = new Intl.NumberFormat("pt-BR");

const dateFormatter = new Intl.DateTimeFormat("pt-BR", {
  dateStyle: "medium",
  timeStyle: "short",
});

const timeFormatter = new Intl.DateTimeFormat("pt-BR", {
  timeStyle: "short",
});

const dateOnlyFormatter = new Intl.DateTimeFormat("pt-BR", {
  dateStyle: "short",
});

const normalizeMoney = (value) => {
  if (typeof value === "number") return value;
  if (!value) return 0;
  let text = String(value).trim();
  if (text.includes(",")) {
    text = text.replace(/\./g, "").replace(",", ".");
  }
  const cleaned = text.replace(/[^\d.-]/g, "");
  const parsed = Number.parseFloat(cleaned);
  return Number.isFinite(parsed) ? parsed : 0;
};

const normalizeText = (value) => String(value || "").toLowerCase().trim();

const normalizeBotText = (value) =>
  normalizeText(value)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");

const pickTimestamp = (item) =>
  new Date(item.evento_timestamp || item.data_criacao_chat || Date.now());

const splitVendorNameFromContent = (content) => {
  const text = String(content || "");
  const match = text.match(/\*([^*]+)\*/);
  if (!match) {
    return { name: null, content: text.trim() || "--" };
  }
  const name = match[1].trim();
  const normalizedName = normalizeBotText(name);
  const wordCount = name.split(/\s+/).filter(Boolean).length;
  const looksLikeName =
    name.length > 0 &&
    name.length <= 40 &&
    wordCount <= 5 &&
    !/[.!?:;]/.test(name) &&
    !/\d/.test(name) &&
    !normalizedName.includes("horario de atendimento") &&
    !normalizedName.includes("fora do horario") &&
    !normalizedName.includes("assistente") &&
    !normalizedName.includes("setor de vendas") &&
    !normalizedName.includes("time de vendas");
  if (!looksLikeName) {
    return { name: null, content: text.trim() || "--" };
  }
  const cleaned = text.replace(match[0], "").trim();
  return { name, content: cleaned || "--" };
};

const formatCurrency = (value) => currencyFormatter.format(value || 0);

const formatCount = (value) => countFormatter.format(value || 0);

const formatPercent = (value, total) => {
  if (!total || total <= 0) return "0%";
  return `${Math.round((value / total) * 100)}%`;
};

const formatDate = (value) => {
  if (!value) return "--";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "--";
  return dateFormatter.format(date);
};

const formatTime = (value) => {
  if (!value) return "--";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "--";
  return timeFormatter.format(date);
};

const formatDayLabel = (value) => {
  if (!value) return "--";
  const text = String(value);
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    const [year, month, day] = text.split("-");
    return `${day}/${month}/${year}`;
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return text;
  return dateOnlyFormatter.format(date);
};

const buildMiniBarTooltip = (entry, valueKey) => {
  const dayLabel = formatDayLabel(entry?.day);
  const metricLabels = [
    ["Novos contatos", "contacts"],
    ["Em espera", "waiting"],
    ["Vendas", "sales"],
    ["SAC", "sac"],
    ["Rastreio", "tracking"],
    ["Transferidos", "transferred"],
    ["Morreram", "dead"],
  ];
  const presentMetrics = metricLabels.filter(([, key]) => entry?.[key] !== undefined);
  if (presentMetrics.length === 0) {
    const value = Number(entry?.[valueKey]) || 0;
    return `Data: ${dayLabel}\n${valueKey}: ${formatCount(value)}`;
  }
  const lines = [`Data: ${dayLabel}`];
  presentMetrics.forEach(([label, key]) => {
    lines.push(`${label}: ${formatCount(Number(entry?.[key]) || 0)}`);
  });
  return lines.join("\n");
};

const formatDuration = (seconds) => {
  if (!Number.isFinite(seconds) || seconds <= 0) return "--";
  const totalMinutes = Math.round(seconds / 60);
  const hours = Math.floor(totalMinutes / 60);
  const minutes = totalMinutes % 60;
  if (hours > 0) {
    return `${hours}h ${minutes}m`;
  }
  return `${minutes}m`;
};

const formatDateInput = (date) => {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
};

const parseScore = (value) => {
  if (value === null || value === undefined) return null;
  const match = String(value).match(/\d+(?:[.,]\d+)?/);
  if (!match) return null;
  const numeric = Number(match[0].replace(",", "."));
  return Number.isFinite(numeric) ? numeric : null;
};

const formatScore = (value) => {
  if (!Number.isFinite(value) || value <= 0) return "--";
  return value.toFixed(1);
};

const messagePlaceholder = (messageType) => {
  const type = normalizeText(messageType);
  if (type === "image") return "[Imagem]";
  if (type === "audio") return "[Áudio]";
  if (type === "video") return "[Vídeo]";
  if (type === "file") return "[Arquivo]";
  if (type === "document") return "[Documento]";
  if (type === "template") return "[Template]";
  if (type === "call") return "[Chamada]";
  if (type === "vcard") return "[Contato]";
  if (type === "location") return "[Localização]";
  return "[Mensagem]";
};

const messageContent = (entry) => {
  if (entry.msg_conteudo) return entry.msg_conteudo;
  return messagePlaceholder(entry.msg_tipo);
};

const cleanMessageText = (value) => {
  const normalized = String(value || "").replace(/\r\n?/g, "\n");
  const lines = normalized.split("\n");
  while (lines.length && lines[0].trim() === "") lines.shift();
  if (lines.length && lines[0].trim() === ":") lines.shift();
  while (lines.length && lines[0].trim() === "") lines.shift();
  const cleaned = lines.join("\n").trim();
  return cleaned || "--";
};

const botPhrases = [
  "ola, seja bem-vindo(a)! sou a helena, assistente de vendas da vogaflex.",
  "deseja tirar alguma duvida ou gostaria de um orcamento?",
  "nosso horario de atendimento e das 08h as 17h, de segunda a sexta-feira.",
  "no momento, nossos atendentes estao fora do horario de atendimento. assim que retornarmos, daremos sequencia a nossa conversa.",
  "agradeco pelas informacoes! estou direcionando o seu atendimento ao nosso setor de vendas",
  "vou verificar a disponibilidade com nosso time de vendas. agradeco pelas informacoes! estou direcionando o seu atendimento ao nosso setor de vendas",
  "agradeco pelas informacoes! estou direcionando o seu atendimento ao nosso time de vendas",
  "vou direcionar seu atendimento ao nosso time de vendas",
  "vou encaminhar ao nosso time de vendas",
  "obrigado, vou encaminhar ao nosso time de vendas",
  "obrigada, vou encaminhar ao nosso time de vendas",
];

const STATUS_OPTIONS = [
  "Todos",
  "Triagem",
  "Aguardando",
  "Em atendimento",
  "Finalizado",
];

const EMPTY_LIST = Object.freeze([]);
const EMPTY_MAP = Object.freeze({});

const isBotContent = (entry, content) => {
  const messageType = normalizeBotText(entry.msg_tipo);
  if (["template", "system", "bot", "automation", "automated"].includes(messageType)) {
    return true;
  }
  const normalized = normalizeBotText(content);
  if (!normalized || normalized === "null") return false;
  if (botPhrases.some((phrase) => normalized.includes(phrase))) return true;
  if (normalized.includes("assistente de vendas")) return true;
  if (normalized.includes("horario de atendimento")) return true;
  if (normalized.includes("fora do horario")) return true;
  if (normalized.includes("daremos sequencia")) return true;
  if (normalized.includes("retornarmos")) return true;
  if (normalized.includes("seja bem-vind")) return true;
  if (normalized.includes("deseja tirar alguma duvida")) return true;
  if (normalized.includes("gostaria de um orcamento")) return true;
  return false;
};

const buildConversations = (items) => {
  const map = new Map();
  items.forEach((item) => {
    const key = item.chat_id || item.protocolo || String(item.id);
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(item);
  });

  return [...map.entries()].map(([chatId, list]) => {
    const sorted = [...list].sort(
      (a, b) => pickTimestamp(a) - pickTimestamp(b)
    );
    const latest = sorted[sorted.length - 1];
    const budgetEvents = sorted.filter(
      (item) => Number(item.valor_orcamento) > 0
    );
    const latestBudget = [...budgetEvents]
      .reverse()
      .find((item) => Number(item.valor_orcamento) > 0);
    return {
      ...latest,
      chat_id: chatId,
      valor_orcamento: latestBudget ? latestBudget.valor_orcamento : 0,
      budget_updated_at: latestBudget
        ? latestBudget.evento_timestamp || latestBudget.data_criacao_chat
        : null,
      timeline: sorted,
    };
  });
};

function LoginScreen({ onLogin }) {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");

  const handleSubmit = (event) => {
    event.preventDefault();
    onLogin?.();
  };

  return (
    <div className="login-page">
      <div className="login-grid">
        <section className="login-brand">
          <div className="login-logo" aria-label="Vogaflex">
            <span className="logo-mark" aria-hidden="true" />
            <span className="logo-text">
              <span className="logo-bold">voga</span>
              <span className="logo-light">flex</span>
            </span>
            <span className="logo-tagline">
              more
              <br />
              e decore
            </span>
          </div>
          <h1>Gestão inteligente do relacionamento com seus clientes.</h1>
          <p className="login-subhead">
            Centralize conversas, acompanhe o funil em tempo real e responda com agilidade
            mantendo o tom premium da Vogaflex.
          </p>
          <div className="login-highlights">
            <article className="highlight-card">
              <p className="highlight-label">Atendimento premium</p>
              <p className="highlight-value">SLA mais rápido</p>
              <p className="highlight-note">Monitoramento de fila e transferencias.</p>
            </article>
            <article className="highlight-card">
              <p className="highlight-label">Visão do funil</p>
              <p className="highlight-value">Etapas claras</p>
              <p className="highlight-note">Status e histórico em um único lugar.</p>
            </article>
            <article className="highlight-card">
              <p className="highlight-label">Equipe conectada</p>
              <p className="highlight-value">Dados em tempo real</p>
              <p className="highlight-note">Dashboard atualizado para gestores.</p>
            </article>
          </div>
        </section>

        <section className="login-card">
          <p className="eyebrow">Acesso seguro</p>
          <h2>Entrar na plataforma</h2>
          <p className="muted">
            Use seu e-mail corporativo para continuar e acesse os painéis do time.
          </p>
          <form className="login-form" onSubmit={handleSubmit}>
            <label className="field">
              <span>Email</span>
              <input
                type="email"
                placeholder="nome@vogaflex.com.br"
                value={email}
                onChange={(event) => setEmail(event.target.value)}
                required
              />
            </label>
            <label className="field">
              <span>Senha</span>
              <input
                type="password"
                placeholder="Digite sua senha"
                value={password}
                onChange={(event) => setPassword(event.target.value)}
                required
              />
            </label>
            <div className="login-row">
              <label className="login-check">
                <input type="checkbox" />
                <span>Lembrar acesso</span>
              </label>
              <button type="button" className="login-link">
                Esqueci minha senha
              </button>
            </div>
            <button type="submit" className="login-primary">
              Entrar
            </button>
            <button type="button" className="login-secondary">
              Solicitar acesso
            </button>
          </form>
          <p className="login-support">
            Dificuldade para acessar? Fale com o suporte interno da Vogaflex.
          </p>
        </section>
      </div>
    </div>
  );
}

function MiniBars({ data, valueKey, color }) {
  const max = Math.max(
    ...data.map((entry) => Number(entry?.[valueKey]) || 0),
    1
  );
  return (
    <div className="mini-chart" aria-hidden="true">
      {data.map((entry, index) => {
        const value = Number(entry?.[valueKey]) || 0;
        const height = Math.round((value / max) * 100);
        const day = entry.day || `item-${index}`;
        return (
          <span
            key={`${day}-${valueKey}-${index}`}
            className="mini-bar"
            style={{
              height: `${height}%`,
              background: color,
            }}
            title={buildMiniBarTooltip(entry, valueKey)}
          />
        );
      })}
    </div>
  );
}

function AppContent({ onLogout }) {
  const [events, setEvents] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [search, setSearch] = useState("");
  const [debouncedSearch, setDebouncedSearch] = useState("");
  const [selectedId, setSelectedId] = useState(null);
  const [statusFilter, setStatusFilter] = useState("Todos");
  const [etapaFilter, setEtapaFilter] = useState("Todos");
  const [vendedorFilter, setVendedorFilter] = useState("Todos");
  const [monthFilter, setMonthFilter] = useState(true);
  const [dateFrom, setDateFrom] = useState("");
  const [dateTo, setDateTo] = useState("");
  const [activeView, setActiveView] = useState("conversations");
  const [fetchVersion, setFetchVersion] = useState(0);
  const [dashboardData, setDashboardData] = useState(null);
  const [dashboardLoading, setDashboardLoading] = useState(false);
  const [dashboardDateFrom, setDashboardDateFrom] = useState("");
  const [dashboardDateTo, setDashboardDateTo] = useState("");
  const [dashboardTab, setDashboardTab] = useState("sdr");
  const [dashboardRange, setDashboardRange] = useState("month");
  const [dashboardVendor, setDashboardVendor] = useState("");
  const [dashboardFetchVersion, setDashboardFetchVersion] = useState(0);
  const [vendorBreakdownOpen, setVendorBreakdownOpen] = useState(false);
  const [vendorBreakdown, setVendorBreakdown] = useState(null);
  const [vendorBreakdownLoading, setVendorBreakdownLoading] = useState(false);

  const loadConversations = async () => {
    const params = new URLSearchParams();
    params.set("limit", "50000");
    if (statusFilter && statusFilter !== "Todos") {
      params.set("status", statusFilter);
    }
    if (etapaFilter && etapaFilter !== "Todos") {
      params.set("etapa", etapaFilter);
    }
    if (dateFrom) params.set("date_from", dateFrom);
    if (dateTo) params.set("date_to", dateTo);
    if (!dateFrom && !dateTo && monthFilter) {
      const now = new Date();
      const monthStart = new Date(now.getFullYear(), now.getMonth(), 1);
      const monthEnd = new Date(now.getFullYear(), now.getMonth() + 1, 0);
      params.set("date_from", formatDateInput(monthStart));
      params.set("date_to", formatDateInput(monthEnd));
    }
    if (vendedorFilter && vendedorFilter !== "Todos") {
      params.set("vendedor", vendedorFilter);
    }
    const response = await fetch(`/api/conversations/?${params}`);
    const data = await response.json();
    if (!response.ok) {
      throw new Error(data.error || "Erro ao carregar dados");
    }
    const normalized = (data.conversations || []).map((item) => ({
      ...item,
      valor_orcamento: normalizeMoney(item.valor_orcamento),
    }));
    setEvents(normalized);
  };

  useEffect(() => {
    let active = true;
    setLoading(true);
    loadConversations()
      .catch((err) => {
        if (!active) return;
        setError(err.message || "Erro desconhecido");
      })
      .finally(() => {
        if (!active) return;
        setLoading(false);
      });
    return () => {
      active = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [fetchVersion]);

  useEffect(() => {
    const handle = setTimeout(() => {
      setDebouncedSearch(search);
    }, 300);
    return () => clearTimeout(handle);
  }, [search]);

  useEffect(() => {
    if (dateFrom || dateTo) {
      setMonthFilter(false);
    }
  }, [dateFrom, dateTo]);

  const conversations = useMemo(() => buildConversations(events), [events]);

  const filteredConversations = useMemo(() => {
    const term = normalizeText(debouncedSearch);
    const now = new Date();
    const monthStart = new Date(now.getFullYear(), now.getMonth(), 1);
    const monthEnd = new Date(now.getFullYear(), now.getMonth() + 1, 1);
    return conversations.filter((item) => {
      // filtros ja aplicados no backend quando usa o botao "Aplicar"
      if (monthFilter) {
        const basis =
          item.evento_timestamp ||
          item.data_criacao_chat ||
          item.updated_at ||
          item.created_at;
        if (!basis) return false;
        const basisDate = new Date(basis);
        if (Number.isNaN(basisDate.getTime())) return false;
        if (basisDate < monthStart || basisDate >= monthEnd) return false;
      }
      if (!term) return true;
      const haystack = [
        item.protocolo,
        item.chat_id,
        item.cliente_nome,
        item.cliente_telefone,
        item.vendedor_nome,
        item.vendedor_email,
        item.status_normalizado,
      ]
        .map(normalizeText)
        .join(" ");
      return haystack.includes(term);
    });
  }, [conversations, debouncedSearch, monthFilter]);

  const vendedorOptions = useMemo(() => {
    const values = Array.from(
      new Set(conversations.map((item) => item.vendedor_nome).filter(Boolean))
    ).sort((a, b) => a.localeCompare(b));
    return ["Todos", ...values];
  }, [conversations]);

  const etapaOptions = useMemo(() => {
    const statusSet = new Set(STATUS_OPTIONS.map((s) => normalizeText(s)));
    const values = Array.from(
      new Set(
        conversations
          .map((item) => item.etapa_funil)
          .filter(
            (value) => value && !statusSet.has(normalizeText(value))
          )
      )
    ).sort((a, b) => a.localeCompare(b));
    return ["Todos", ...values];
  }, [conversations]);

  const periodLabel = useMemo(() => {
    if (dateFrom && dateTo) return `${dateFrom} até ${dateTo}`;
    if (dateFrom) return `A partir de ${dateFrom}`;
    if (dateTo) return `Até ${dateTo}`;
    return "Mês corrente";
  }, [dateFrom, dateTo]);

  const activeFilterChips = useMemo(() => {
    const chips = [`Período: ${periodLabel}`];
    if (statusFilter !== "Todos") chips.push(`Status: ${statusFilter}`);
    if (etapaFilter !== "Todos") chips.push(`Etapa: ${etapaFilter}`);
    if (vendedorFilter !== "Todos") chips.push(`Vendedor: ${vendedorFilter}`);
    return chips;
  }, [periodLabel, statusFilter, etapaFilter, vendedorFilter]);

  const clearConversationFilters = () => {
    setSearch("");
    setStatusFilter("Todos");
    setEtapaFilter("Todos");
    setVendedorFilter("Todos");
    setDateFrom("");
    setDateTo("");
    setMonthFilter(true);
    setFetchVersion((v) => v + 1);
  };

  useEffect(() => {
    if (filteredConversations.length === 0) {
      if (selectedId !== null) setSelectedId(null);
      return;
    }
    const selectedExists =
      selectedId !== null &&
      filteredConversations.some((item) => item.chat_id === selectedId);
    if (!selectedExists) {
      setSelectedId(filteredConversations[0].chat_id);
    }
  }, [filteredConversations, selectedId]);

  const selected = filteredConversations.find(
    (item) => item.chat_id === selectedId
  );

  const selectedVendorAvgScore = useMemo(() => {
    if (!selected?.vendedor_nome) return null;
    const relevant = filteredConversations.filter(
      (item) => item.vendedor_nome === selected.vendedor_nome
    );
    if (relevant.length === 0) return null;
    const scores = relevant
      .map((item) => parseScore(item.ai_agent_rating))
      .filter((value) => value !== null);
    if (scores.length === 0) return null;
    const total = scores.reduce((acc, value) => acc + value, 0);
    return total / scores.length;
  }, [filteredConversations, selected]);

  const [messages, setMessages] = useState([]);
  const [messagesLoading, setMessagesLoading] = useState(false);

  const dedupedMessages = useMemo(() => {
    const seen = new Set();
    return messages.filter((entry) => {
      const signature = [
        entry.chat_id || selectedId || "",
        entry.evento_timestamp || "",
        entry.msg_from_client === true ? "1" : "0",
        normalizeText(entry.msg_tipo),
        cleanMessageText(messageContent(entry)),
      ].join("|");
      if (seen.has(signature)) return false;
      seen.add(signature);
      return true;
    });
  }, [messages, selectedId]);

  useEffect(() => {
    let active = true;
    if (!selectedId) {
      setMessages([]);
      return () => {};
    }
    const loadMessages = async () => {
      setMessagesLoading(true);
      try {
        const response = await fetch(
          `/api/messages/?chat_id=${encodeURIComponent(selectedId)}&limit=2000`
        );
        const data = await response.json();
        if (!response.ok) {
          throw new Error(data.error || "Erro ao carregar mensagens");
        }
        if (!active) return;
        setMessages(data.messages || []);
      } catch (err) {
        if (!active) return;
        setError(err.message || "Erro desconhecido");
      } finally {
        if (active) setMessagesLoading(false);
      }
    };
    loadMessages();
    return () => {
      active = false;
    };
  }, [selectedId]);

  const buildDashboardParams = (extra = {}) => {
    const params = new URLSearchParams();
    if (dashboardDateFrom) params.set("date_from", dashboardDateFrom);
    if (dashboardDateTo) params.set("date_to", dashboardDateTo);
    if (!dashboardDateFrom && !dashboardDateTo) {
      const now = new Date();
      const monthStart = new Date(now.getFullYear(), now.getMonth(), 1);
      const monthEnd = new Date(now.getFullYear(), now.getMonth() + 1, 0);
      params.set("date_from", formatDateInput(monthStart));
      params.set("date_to", formatDateInput(monthEnd));
    }
    if (extra.vendedor) params.set("vendedor", extra.vendedor);
    return params.toString();
  };

  const applyDashboardPreset = (preset) => {
    const now = new Date();
    let start = new Date(now);
    if (preset === "week") {
      start.setDate(now.getDate() - 6);
      const monthStart = new Date(now.getFullYear(), now.getMonth(), 1);
      if (start < monthStart) {
        start = monthStart;
      }
    } else if (preset === "quarter") {
      const quarterStartMonth = Math.floor(now.getMonth() / 3) * 3;
      start = new Date(now.getFullYear(), quarterStartMonth, 1);
    } else {
      start = new Date(now.getFullYear(), now.getMonth(), 1);
    }
    setDashboardRange(preset);
    setDashboardDateFrom(formatDateInput(start));
    setDashboardDateTo(formatDateInput(now));
    setDashboardFetchVersion((value) => value + 1);
  };

  useEffect(() => {
    let active = true;
    if (activeView !== "dashboard") return () => {};
    if (!dashboardDateFrom && !dashboardDateTo) {
      applyDashboardPreset("month");
      return () => {};
    }
    setDashboardLoading(true);
    fetch(`/api/dashboard/?${buildDashboardParams()}`)
      .then((res) => res.json().then((data) => ({ ok: res.ok, data })))
      .then(({ ok, data }) => {
        if (!active) return;
        if (!ok) throw new Error(data.error || "Erro ao carregar dashboard");
        setDashboardData(data);
      })
      .catch((err) => {
        if (!active) return;
        setError(err.message || "Erro desconhecido");
      })
      .finally(() => {
        if (!active) return;
        setDashboardLoading(false);
      });
    return () => {
      active = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeView, dashboardFetchVersion]);

  useEffect(() => {
    let active = true;
    if (activeView !== "dashboard" || dashboardTab !== "vendors") {
      return () => {};
    }
    if (!dashboardVendor) return () => {};
    setVendorBreakdownLoading(true);
    fetch(`/api/dashboard/?${buildDashboardParams({ vendedor: dashboardVendor })}`)
      .then((res) => res.json().then((data) => ({ ok: res.ok, data })))
      .then(({ ok, data }) => {
        if (!active) return;
        if (!ok) throw new Error(data.error || "Erro ao carregar estratificação");
        setVendorBreakdown(data.contacts_breakdown || null);
      })
      .catch((err) => {
        if (!active) return;
        setError(err.message || "Erro desconhecido");
        setVendorBreakdown(null);
      })
      .finally(() => {
        if (!active) return;
        setVendorBreakdownLoading(false);
      });
    return () => {
      active = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeView, dashboardTab, dashboardVendor, dashboardFetchVersion]);

  const dashboardStats = dashboardData?.stats;
  const sdrData = dashboardData?.sdr;
  const vendorData = dashboardData?.vendors;
  const vendorList = useMemo(
    () => vendorData?.summary || EMPTY_LIST,
    [vendorData]
  );
  const vendorScores = useMemo(
    () => vendorData?.scores || EMPTY_MAP,
    [vendorData]
  );

  useEffect(() => {
    if (vendorList.length === 0) return;
    if (!dashboardVendor || !vendorList.some((item) => item.vendedor === dashboardVendor)) {
      setDashboardVendor(vendorList[0].vendedor);
    }
  }, [dashboardVendor, vendorList]);

  const sdrSeries = useMemo(() => {
    const map = new Map();
    (sdrData?.daily || []).forEach((entry) => {
      if (!entry.day) return;
      map.set(entry.day, {
        day: entry.day,
        contacts: entry.contacts || 0,
        sales: entry.sales || 0,
        tracking: entry.tracking || 0,
        sac: entry.sac || 0,
        waiting: entry.waiting || 0,
        dead: entry.dead || 0,
        transferred: 0,
      });
    });
    (sdrData?.transferred_daily || []).forEach((entry) => {
      if (!entry.day) return;
      const current = map.get(entry.day) || {
        day: entry.day,
        contacts: 0,
        sales: 0,
        tracking: 0,
        sac: 0,
        waiting: 0,
        dead: 0,
        transferred: 0,
      };
      current.transferred = entry.transferred || 0;
      map.set(entry.day, current);
    });
    return [...map.values()].sort((a, b) => a.day.localeCompare(b.day));
  }, [sdrData]);

  const sdrSummary = sdrData?.summary || {};
  const sdrTotal = sdrSummary.contacts || 0;
  const sdrFunnel = [
    {
      label: "Em espera",
      value: sdrSummary.waiting || 0,
      note: `${formatPercent(sdrSummary.waiting || 0, sdrTotal)} do total`,
    },
    {
      label: "Vendas",
      value: sdrSummary.sales || 0,
      note: `${formatPercent(sdrSummary.sales || 0, sdrTotal)} do total`,
    },
    {
      label: "SAC",
      value: sdrSummary.sac || 0,
      note: `${formatPercent(sdrSummary.sac || 0, sdrTotal)} do total`,
    },
    {
      label: "Rastreio",
      value: sdrSummary.tracking || 0,
      note: `${formatPercent(sdrSummary.tracking || 0, sdrTotal)} do total`,
    },
  ];

  const sdrSeriesTrimmed = sdrSeries.slice(-14);

  const vendorTotals = vendorList.reduce(
    (acc, item) => ({
      contacts: acc.contacts + (item.contacts_received || 0),
      budgetsCount:
        acc.budgetsCount + (item.budgets_detected_count || item.budgets_count || 0),
      budgetsSum: acc.budgetsSum + (item.budgets_sum || 0),
      budgetsSumDetected:
        acc.budgetsSumDetected + (item.budgets_sum_detected || 0),
      dead: acc.dead + (item.dead_contacts || 0),
    }),
    { contacts: 0, budgetsCount: 0, budgetsSum: 0, budgetsSumDetected: 0, dead: 0 }
  );

  const selectedVendorData = vendorList.find(
    (item) => item.vendedor === dashboardVendor
  );
  const selectedVendorScores = vendorScores?.[dashboardVendor] || [];

  const contactsBreakdownStages = vendorBreakdown?.stages || [];
  const contactsBreakdownTotal =
    Number.isFinite(vendorBreakdown?.total) && vendorBreakdown?.total >= 0
      ? vendorBreakdown.total
      : selectedVendorData?.contacts_received || 0;
  const contactsBreakdownPending =
    Number.isFinite(vendorBreakdown?.pending) && vendorBreakdown?.pending >= 0
      ? vendorBreakdown.pending
      : 0;
  const contactsBreakdownFinalized =
    Number.isFinite(vendorBreakdown?.finalized) && vendorBreakdown?.finalized >= 0
      ? vendorBreakdown.finalized
      : 0;
  const contactsBreakdownActive =
    Number.isFinite(vendorBreakdown?.active) && vendorBreakdown?.active >= 0
      ? vendorBreakdown.active
      : 0;
  const contactsBreakdownOther =
    Number.isFinite(vendorBreakdown?.other) && vendorBreakdown?.other >= 0
      ? vendorBreakdown.other
      : Math.max(
          contactsBreakdownTotal -
            contactsBreakdownFinalized -
            contactsBreakdownActive -
            contactsBreakdownPending,
          0
        );
  const contactsStageList = contactsBreakdownStages
    .filter((stage) => stage && stage.stage_name)
    .map((stage) => ({
      stage_name: String(stage.stage_name),
      total: Number(stage.total) || 0,
    }))
    .sort((a, b) => b.total - a.total);

  return (
    <div className="app-shell">
      <aside className="rail">
        <div className="rail-logo">V</div>
        <div className="rail-icons">
          <button
            className={`rail-btn${activeView === "conversations" ? " is-active" : ""}`}
            type="button"
            onClick={() => setActiveView("conversations")}
            aria-label="Conversas"
          >
            <svg viewBox="0 0 24 24" aria-hidden="true">
              <path
                d="M4 5.5C4 4.12 5.12 3 6.5 3h11C18.88 3 20 4.12 20 5.5v8c0 1.38-1.12 2.5-2.5 2.5H9l-4.5 4.5v-4.5h-1C2.12 16 1 14.88 1 13.5v-8C1 4.12 2.12 3 3.5 3H4Z"
                fill="currentColor"
              />
            </svg>
          </button>
          <button
            className={`rail-btn${activeView === "dashboard" ? " is-active" : ""}`}
            type="button"
            onClick={() => setActiveView("dashboard")}
            aria-label="Dashboard"
          >
            <svg viewBox="0 0 24 24" aria-hidden="true">
              <path
                d="M3 13h6v8H3v-8Zm12-10h6v18h-6V3ZM11 8h6v13h-6V8ZM3 3h6v8H3V3Z"
                fill="currentColor"
              />
            </svg>
          </button>
        </div>
        <div className="rail-footer">
          <button className="rail-btn" type="button" aria-label="Ajuda">
            <svg viewBox="0 0 24 24" aria-hidden="true">
              <path
                d="M12 2a10 10 0 1 0 0 20 10 10 0 0 0 0-20Zm0 15.5a1.25 1.25 0 1 1 0-2.5 1.25 1.25 0 0 1 0 2.5Zm1.7-5.9c-.7.4-.95.7-.95 1.6h-1.7c0-1.6.55-2.4 1.65-3 1-.5 1.45-.85 1.45-1.6 0-.8-.65-1.35-1.55-1.35-.95 0-1.6.55-1.7 1.4H8.1C8.25 6.5 9.5 5.2 11.8 5.2c2.2 0 3.6 1.2 3.6 3 0 1.5-.8 2.3-1.7 2.8Z"
                fill="currentColor"
              />
            </svg>
          </button>
          <button
            className="rail-btn rail-btn-logout"
            type="button"
            onClick={onLogout}
            aria-label="Sair"
          >
            <svg viewBox="0 0 24 24" aria-hidden="true">
              <path
                d="M5 3h8a2 2 0 0 1 2 2v3h-2V5H5v14h8v-3h2v3a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2Zm10.5 4.5L20 12l-4.5 4.5-1.4-1.4 2.1-2.1H9v-2h7.2l-2.1-2.1 1.4-1.4Z"
                fill="currentColor"
              />
            </svg>
          </button>
        </div>
      </aside>

      {activeView === "conversations" ? (
        <>
          <section className="panel left-panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Vogaflex</p>
            <h2>Conversas</h2>
            <p className="muted">
              {loading ? "Carregando..." : `${filteredConversations.length} encontradas`}
            </p>
          </div>
          <span className="pill">{periodLabel}</span>
        </div>

        {error ? (
          <div className="alert" role="alert">
            <strong>Falha ao carregar os dados.</strong>
            <span>{error}</span>
          </div>
        ) : null}

        <label className="field">
          <span>Buscar</span>
          <input
            type="search"
            placeholder="Cliente, protocolo, vendedor"
            value={search}
            onChange={(event) => setSearch(event.target.value)}
          />
        </label>
        <details className="filter-menu">
          <summary>Filtros</summary>
          <label className="field toggle">
            <span>Mês corrente</span>
            <input
              type="checkbox"
              checked={monthFilter}
              onChange={(event) => setMonthFilter(event.target.checked)}
            />
          </label>
          <div className="filter-row">
            <label className="field">
              <span>De</span>
              <input
                type="date"
                value={dateFrom}
                onChange={(event) => setDateFrom(event.target.value)}
              />
            </label>
            <label className="field">
              <span>Até</span>
              <input
                type="date"
                value={dateTo}
                onChange={(event) => setDateTo(event.target.value)}
              />
            </label>
          </div>
          <div className="filter-row">
            <label className="field">
              <span>Status</span>
              <select
                value={statusFilter}
                onChange={(event) => setStatusFilter(event.target.value)}
              >
                {STATUS_OPTIONS.map((value) => (
                  <option key={value} value={value}>
                    {value}
                  </option>
                ))}
              </select>
            </label>
            <label className="field">
              <span>Etapa</span>
              <select
                value={etapaFilter}
                onChange={(event) => setEtapaFilter(event.target.value)}
              >
                {etapaOptions.map((value) => (
                  <option key={value} value={value}>
                    {value}
                  </option>
                ))}
              </select>
            </label>
          </div>
          <label className="field">
            <span>Vendedor</span>
            <select
              value={vendedorFilter}
              onChange={(event) => setVendedorFilter(event.target.value)}
            >
              {vendedorOptions.map((value) => (
                <option key={value} value={value}>
                  {value}
                </option>
              ))}
            </select>
          </label>
          <div className="filter-actions">
            <button
              type="button"
              className="secondary-button"
              onClick={clearConversationFilters}
            >
              Limpar
            </button>
            <button
              type="button"
              className="primary-button"
              onClick={() => setFetchVersion((v) => v + 1)}
            >
              Aplicar filtros
            </button>
          </div>
        </details>
        <div className="active-filters">
          {activeFilterChips.map((chip) => (
            <span key={chip} className="filter-chip">
              {chip}
            </span>
          ))}
        </div>

        <div className="conversation-list" role="listbox">
          {filteredConversations.length === 0 ? (
            <p className="empty">Nenhuma conversa encontrada.</p>
          ) : (
            filteredConversations
              .slice()
              .sort((a, b) => pickTimestamp(b) - pickTimestamp(a))
              .map((item) => {
                const preview = messageContent(item);
                return (
                  <button
                    key={item.chat_id}
                    type="button"
                    className={`conversation-item${
                      item.chat_id === selectedId ? " is-active" : ""
                    }`}
                    onClick={() => setSelectedId(item.chat_id)}
                  >
                    <div className="conversation-title">
                      <strong>{item.cliente_nome || item.chat_id}</strong>
                    </div>
                    <div className="conversation-meta">
                      <span>{item.status_normalizado || "--"}</span>
                      <span>{formatCurrency(item.valor_orcamento)}</span>
                    </div>
                    <div className="conversation-preview">
                      {`${preview.slice(0, 140)}${preview.length > 140 ? "..." : ""}`}
                    </div>
                  </button>
                );
              })
            )}
          </div>
        </section>

        <section className="panel chat-panel">
          <div className="chat-header">
            <div>
              <p className="eyebrow">Histórico</p>
              <h2>{selected?.cliente_nome || "Selecione uma conversa"}</h2>
              <p className="vendor-tag">
                {selected
                  ? `vendedor: "${selected.vendedor_nome || "--"}"`
                  : 'vendedor: "--"'}
              </p>
            </div>
            <div className="tag">
            {selected?.status_normalizado || "Sem status"}
            </div>
          </div>

        <div className="chat-window" role="log">
          {!selected ? (
            <p className="empty">
              Selecione uma conversa para ver o histórico completo.
            </p>
          ) : messagesLoading ? (
            <p className="empty">Carregando mensagens...</p>
          ) : dedupedMessages.length === 0 ? (
            <p className="empty">Sem mensagens para este chat na base atual.</p>
          ) : (
            dedupedMessages.map((entry, index) => {
              const content = cleanMessageText(messageContent(entry));
              const { name: vendorName, content: cleanedContent } =
                splitVendorNameFromContent(content);
              const fromClient = entry.msg_from_client === true;
              const bot = isBotContent(entry, content) || (!fromClient && !vendorName);
              const sender = {
                name: bot
                  ? "Bot"
                  : fromClient
                  ? selected.cliente_nome || "Cliente"
                  : vendorName || selected.vendedor_nome || "Vendedor",
                role: fromClient ? "client" : "vendor",
                content: fromClient ? content : vendorName ? cleanedContent : content,
              };
              const statusText = String(entry.msg_status_envio || "").trim();
              const showStatus =
                statusText && statusText.toLowerCase() !== "true";

              return (
                <div
                  key={entry.id || `${selected.chat_id}-${index}`}
                  className={`message-row${
                    sender.role === "vendor" ? " is-outgoing" : ""
                  }`}
                >
                  <article
                    className={`message-bubble${
                      sender.role === "vendor" ? " is-outgoing" : ""
                    }`}
                  >
                    <div className="message-meta">
                      <span>
                        {sender.name}
                        {bot ? " · BOT" : ""}
                      </span>
                    </div>
                    <p className="message-text">{sender.content}</p>
                    {showStatus ? (
                      <div className="message-status">{statusText}</div>
                    ) : null}
                  </article>
                  <span className="message-time">
                    {formatTime(entry.evento_timestamp)}
                  </span>
                </div>
              );
            })
            )}
          </div>
        </section>

        <aside className="panel right-panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Detalhes</p>
            <h3>Perfil do cliente</h3>
            <p className="muted">{selected?.protocolo || "--"}</p>
          </div>
        </div>
        {!selected ? (
          <p className="empty">Nenhuma conversa selecionada.</p>
        ) : (
          <div className="detail-grid">
            <details className="detail-menu">
              <summary>Informações principais</summary>
              <dl className="detail-list">
                <div>
                  <dt>Status</dt>
                  <dd>{selected.status_conversa || "--"}</dd>
                </div>
                <div>
                  <dt>Valor do orçamento</dt>
                  <dd>{formatCurrency(selected.valor_orcamento)}</dd>
                </div>
                <div>
                  <dt>Data de criação</dt>
                  <dd>{formatDate(selected.data_criacao_chat)}</dd>
                </div>
                <div>
                  <dt>Data de fechamento</dt>
                  <dd>{formatDate(selected.data_fechamento)}</dd>
                </div>
                <div>
                  <dt>Média IA (vendedor)</dt>
                  <dd>{formatScore(selectedVendorAvgScore)}</dd>
                </div>
                <div>
                  <dt>Sentimento IA</dt>
                  <dd>{selected.ai_customer_sentiment ?? "--"}</dd>
                </div>
              </dl>
            </details>
            <details className="detail-menu">
              <summary>Contato</summary>
              <dl className="detail-list">
                <div>
                  <dt>Cliente</dt>
                  <dd>{selected.cliente_nome || "--"}</dd>
                </div>
                <div>
                  <dt>Telefone</dt>
                  <dd>{selected.cliente_telefone || "--"}</dd>
                </div>
                <div>
                  <dt>Vendedor</dt>
                  <dd>{selected.vendedor_nome || "--"}</dd>
                </div>
                <div>
                  <dt>Email</dt>
                  <dd>{selected.vendedor_email || "--"}</dd>
                </div>
              </dl>
            </details>
            <details className="detail-menu">
              <summary>Funil & IA</summary>
              <dl className="detail-list">
                <div>
                  <dt>Etapa</dt>
                  <dd>{selected.etapa_funil || "--"}</dd>
                </div>
                <div>
                  <dt>Coluna Kanban</dt>
                  <dd>{selected.coluna_kanban || "--"}</dd>
                </div>
                <div>
                  <dt>Departamento</dt>
                  <dd>{selected.departamento || "--"}</dd>
                </div>
                <div>
                  <dt>Produto</dt>
                  <dd>{selected.produto_interesse || "--"}</dd>
                </div>
                <div>
                  <dt>Motivo perda</dt>
                  <dd>{selected.motivo_perda || "--"}</dd>
                </div>
                <div>
                  <dt>Resumo IA</dt>
                  <dd>{selected.ai_summary || "--"}</dd>
                </div>
                <div>
                  <dt>Sugestão IA</dt>
                  <dd>{selected.ai_suggestion || "--"}</dd>
                </div>
                <div>
                  <dt>Motivo contato</dt>
                  <dd>{selected.contact_reason || "--"}</dd>
                </div>
              </dl>
            </details>
          </div>
        )}
        </aside>
        </>
      ) : (
        <section className="panel dashboard-panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow">Vogaflex</p>
              <h2>Dashboard</h2>
              <p className="muted">Indicadores operacionais do time.</p>
            </div>
          </div>
          <div className="dashboard-shell">
            <aside className="dashboard-side">
              <button
                type="button"
                className={`dashboard-tab${dashboardTab === "sdr" ? " is-active" : ""}`}
                onClick={() => setDashboardTab("sdr")}
              >
                SDR
              </button>
              <button
                type="button"
                className={`dashboard-tab${
                  dashboardTab === "vendors" ? " is-active" : ""
                }`}
                onClick={() => setDashboardTab("vendors")}
              >
                Vendedores
              </button>
              {dashboardTab === "vendors" ? (
                <div className="dashboard-submenu">
                  {vendorList.length === 0 ? (
                    <p className="dashboard-submenu-empty">Sem vendedores no período.</p>
                  ) : (
                    vendorList.map((vendor) => (
                      <button
                        key={vendor.vendedor}
                        type="button"
                        className={`dashboard-subitem${
                          dashboardVendor === vendor.vendedor ? " is-active" : ""
                        }`}
                        onClick={() => setDashboardVendor(vendor.vendedor)}
                      >
                        <span className="dashboard-subitem-name">{vendor.vendedor}</span>
                        <span className="dashboard-subitem-meta">
                          {formatCount(vendor.contacts_received)}
                        </span>
                      </button>
                    ))
                  )}
                </div>
              ) : null}
            </aside>
            <div className="dashboard-content">
              <div className="dashboard-filters">
                <div className="range-buttons">
                  <button
                    type="button"
                    className={`range-btn${dashboardRange === "week" ? " is-active" : ""}`}
                    onClick={() => applyDashboardPreset("week")}
                  >
                    Semana
                  </button>
                  <button
                    type="button"
                    className={`range-btn${dashboardRange === "month" ? " is-active" : ""}`}
                    onClick={() => applyDashboardPreset("month")}
                  >
                    Mês
                  </button>
                  <button
                    type="button"
                    className={`range-btn${dashboardRange === "quarter" ? " is-active" : ""}`}
                    onClick={() => applyDashboardPreset("quarter")}
                  >
                    Trimestre
                  </button>
                </div>
                <label className="field">
                  <span>De</span>
                  <input
                    type="date"
                    value={dashboardDateFrom}
                    onChange={(event) => {
                      setDashboardRange("custom");
                      setDashboardDateFrom(event.target.value);
                    }}
                  />
                </label>
                <label className="field">
                  <span>Até</span>
                  <input
                    type="date"
                    value={dashboardDateTo}
                    onChange={(event) => {
                      setDashboardRange("custom");
                      setDashboardDateTo(event.target.value);
                    }}
                  />
                </label>
                <button
                  type="button"
                  className="primary-button"
                  onClick={() => setDashboardFetchVersion((v) => v + 1)}
                >
                  Aplicar período
                </button>
              </div>
              {dashboardLoading ? (
                <div className="empty">Carregando indicadores...</div>
              ) : dashboardData ? (
                <>
                  {dashboardTab === "sdr" ? (
                    <>
                      <div className="dashboard-section">
                        <div className="section-head">
                          <h3>Resumo SDR</h3>
                          <p className="muted">Leitura do funil de novos contatos.</p>
                        </div>
                        <div className="funnel-grid">
                          {sdrFunnel.map((item) => (
                            <article className="funnel-step" key={item.label}>
                              <p className="stat-label">{item.label}</p>
                              <p className="stat-value">{formatCount(item.value)}</p>
                              <p className="stat-foot">{item.note}</p>
                            </article>
                          ))}
                        </div>
                      </div>
                      <div className="dashboard-section">
                        <div className="section-head">
                          <h3>Histórico diário</h3>
                          <p className="muted">Últimos 14 dias do período.</p>
                        </div>
                        <div className="metric-grid">
                          <article className="metric-card">
                            <p className="stat-label">Novos contatos</p>
                            <p className="stat-value">
                              {formatCount(sdrSummary.contacts || 0)}
                            </p>
                            <MiniBars
                              data={sdrSeriesTrimmed}
                              valueKey="contacts"
                              color="var(--accent)"
                            />
                          </article>
                          <article className="metric-card">
                            <p className="stat-label">Vendas</p>
                            <p className="stat-value">
                              {formatCount(sdrSummary.sales || 0)}
                            </p>
                            <MiniBars
                              data={sdrSeriesTrimmed}
                              valueKey="sales"
                              color="rgba(47, 48, 61, 0.75)"
                            />
                          </article>
                          <article className="metric-card">
                            <p className="stat-label">Transferidos</p>
                            <p className="stat-value">
                              {formatCount(sdrSummary.transferred || 0)}
                            </p>
                            <MiniBars
                              data={sdrSeriesTrimmed}
                              valueKey="transferred"
                              color="rgba(200, 38, 80, 0.55)"
                            />
                          </article>
                          <article className="metric-card">
                            <p className="stat-label">Morreram</p>
                            <p className="stat-value">
                              {formatCount(sdrSummary.dead || 0)}
                            </p>
                            <MiniBars
                              data={sdrSeriesTrimmed}
                              valueKey="dead"
                              color="rgba(148, 159, 166, 0.8)"
                            />
                          </article>
                        </div>
                      </div>
                    </>
                  ) : (
                    <>
                      <div className="dashboard-section">
                        <div className="section-head">
                          <h3>Painel geral comparativo</h3>
                          <p className="muted">Resumo consolidado da equipe.</p>
                        </div>
                        <div className="metric-grid">
                          <article className="metric-card">
                            <p className="stat-label">Contatos recebidos</p>
                            <p className="stat-value">{formatCount(vendorTotals.contacts)}</p>
                            <p className="stat-foot">Base do período</p>
                          </article>
                          <article className="metric-card">
                            <p className="stat-label">Orçamentos detectados</p>
                            <p className="stat-value">{formatCount(vendorTotals.budgetsCount)}</p>
                            <p className="stat-foot">
                              {formatPercent(vendorTotals.budgetsCount, vendorTotals.contacts)} de conversão
                            </p>
                          </article>
                          <article className="metric-card">
                            <p className="stat-label">Somatória orçada (registrado)</p>
                            <p className="stat-value">{formatCurrency(vendorTotals.budgetsSum)}</p>
                            <p className="stat-foot">Volume financeiro</p>
                          </article>
                          <article className="metric-card">
                            <p className="stat-label">Somatória orçada (mensagens)</p>
                            <p className="stat-value">
                              {formatCurrency(vendorTotals.budgetsSumDetected)}
                            </p>
                            <p className="stat-foot">Não registrado no CRM</p>
                          </article>
                          <article className="metric-card">
                            <p className="stat-label">TMA</p>
                            <p className="stat-value">
                              {formatDuration(dashboardStats?.avg_duration_seconds || 0)}
                            </p>
                            <p className="stat-foot">Tempo médio de atendimento</p>
                          </article>
                          <article className="metric-card">
                            <p className="stat-label">TME</p>
                            <p className="stat-value">
                              {formatDuration(dashboardStats?.avg_handoff_seconds || 0)}
                            </p>
                            <p className="stat-foot">SLA bot → vendedor</p>
                          </article>
                          <article className="metric-card">
                            <p className="stat-label">Contatos mortos</p>
                            <p className="stat-value">{formatCount(vendorTotals.dead)}</p>
                            <p className="stat-foot">Sem resposta</p>
                          </article>
                        </div>
                      </div>
                      <div className="dashboard-section">
                        <div className="section-head">
                          <h3>Comparativo por vendedor</h3>
                          <p className="muted">
                            Selecione um vendedor no menu lateral para ver os detalhes.
                          </p>
                        </div>
                        <div className="vendor-detail">
                          {!selectedVendorData ? (
                            <p className="empty">Selecione um vendedor.</p>
                          ) : (
                            <>
                              <div className="vendor-header">
                                <h3>{selectedVendorData.vendedor}</h3>
                                <span className="tag">
                                  {formatCount(selectedVendorData.contacts_received)} contatos
                                </span>
                              </div>
                              <div className="funnel-grid">
                                {[
                                  {
                                    label: "Contatos",
                                    value: selectedVendorData.contacts_received || 0,
                                    note: "Base do vendedor",
                                    clickable: true,
                                  },
                                  {
                                    label: "Orçamentos detectados",
                                    value:
                                      selectedVendorData.budgets_detected_count ||
                                      selectedVendorData.budgets_count ||
                                      0,
                                    note: `${formatPercent(
                                      selectedVendorData.budgets_detected_count ||
                                        selectedVendorData.budgets_count ||
                                        0,
                                      selectedVendorData.contacts_received || 0
                                    )} de conversão`,
                                  },
                                  {
                                    label: "Morreu",
                                    value: selectedVendorData.dead_contacts || 0,
                                    note: `${formatPercent(
                                      selectedVendorData.dead_contacts || 0,
                                      selectedVendorData.contacts_received || 0
                                    )} do total`,
                                  },
                                ].map((item) =>
                                  item.clickable ? (
                                    <button
                                      key={item.label}
                                      type="button"
                                      className={`funnel-step is-clickable${
                                        vendorBreakdownOpen ? " is-active" : ""
                                      }`}
                                      onClick={() =>
                                        setVendorBreakdownOpen((open) => !open)
                                      }
                                      aria-expanded={vendorBreakdownOpen}
                                    >
                                      <p className="stat-label">{item.label}</p>
                                      <p className="stat-value">
                                        {formatCount(item.value)}
                                      </p>
                                      <p className="stat-foot">{item.note}</p>
                                    </button>
                                  ) : (
                                    <article className="funnel-step" key={item.label}>
                                      <p className="stat-label">{item.label}</p>
                                      <p className="stat-value">
                                        {formatCount(item.value)}
                                      </p>
                                      <p className="stat-foot">{item.note}</p>
                                    </article>
                                  )
                                )}
                              </div>
                              <div className="vendor-metrics">
                                <article className="metric-card">
                                  <p className="stat-label">TMA</p>
                                  <p className="stat-value">
                                    {formatDuration(
                                      selectedVendorData.avg_duration_seconds || 0
                                    )}
                                  </p>
                                  <p className="stat-foot">Tempo médio</p>
                                </article>
                                <article className="metric-card">
                                  <p className="stat-label">TME</p>
                                  <p className="stat-value">
                                    {formatDuration(
                                      selectedVendorData.avg_handoff_seconds || 0
                                    )}
                                  </p>
                                  <p className="stat-foot">Tempo de espera</p>
                                </article>
                                <article className="metric-card">
                                  <p className="stat-label">Score IA</p>
                                  <p className="stat-value">
                                    {formatScore(selectedVendorData.avg_score || 0)}
                                  </p>
                                  <p className="stat-foot">Média 0–10</p>
                                </article>
                                <article className="metric-card">
                                  <p className="stat-label">Somatória orçada (registrado)</p>
                                  <p className="stat-value">
                                    {formatCurrency(selectedVendorData.budgets_sum || 0)}
                                  </p>
                                  <p className="stat-foot">Valor total</p>
                                </article>
                                <article className="metric-card">
                                  <p className="stat-label">Somatória orçada (mensagens)</p>
                                  <p className="stat-value">
                                    {formatCurrency(
                                      selectedVendorData.budgets_sum_detected || 0
                                    )}
                                  </p>
                                  <p className="stat-foot">Não registrado no CRM</p>
                                </article>
                              </div>
                              <div className="score-panel">
                                <h4>Score do bot</h4>
                                {selectedVendorScores.length === 0 ? (
                                  <p className="empty">Sem scores no período.</p>
                                ) : (
                                  <div className="score-grid">
                                    {selectedVendorScores.map((score) => (
                                      <div className="score-chip" key={score.score}>
                                        <span>{score.score}</span>
                                        <strong>{formatCount(score.total)}</strong>
                                      </div>
                                    ))}
                                  </div>
                                )}
                              </div>
                            </>
                          )}
                        </div>
                      </div>
                    </>
                  )}
                </>
              ) : (
                <div className="empty">Sem dados para o período.</div>
              )}
            </div>
          </div>
        </section>
      )}
      {vendorBreakdownOpen && (
        <div
          className="modal-overlay"
          role="dialog"
          aria-modal="true"
          onClick={() => setVendorBreakdownOpen(false)}
        >
          <div
            className="modal-card"
            onClick={(event) => event.stopPropagation()}
          >
            <div className="modal-header">
              <div>
                <p className="stat-label">Estratificação de contatos</p>
                <p className="stat-foot">
                  {selectedVendorData?.vendedor || "Vendedor"} · Base{" "}
                  {formatCount(contactsBreakdownTotal)}
                </p>
              </div>
              <button
                type="button"
                className="modal-close"
                onClick={() => setVendorBreakdownOpen(false)}
                aria-label="Fechar"
              >
                ×
              </button>
            </div>
            {vendorBreakdownLoading ? (
              <p className="empty">Carregando estratificação...</p>
            ) : (
              <>
                <div className="breakdown-grid">
                  <div className="stat-card">
                    <p className="stat-label">Em atendimento</p>
                    <p className="stat-value">
                      {formatCount(contactsBreakdownActive)}
                    </p>
                    <p className="stat-foot">
                      {formatPercent(
                        contactsBreakdownActive,
                        contactsBreakdownTotal
                      )}{" "}
                      do total
                    </p>
                  </div>
                  <div className="stat-card">
                    <p className="stat-label">Pendentes</p>
                    <p className="stat-value">
                      {formatCount(contactsBreakdownPending)}
                    </p>
                    <p className="stat-foot">
                      {formatPercent(
                        contactsBreakdownPending,
                        contactsBreakdownTotal
                      )}{" "}
                      do total
                    </p>
                  </div>
                  <div className="stat-card">
                    <p className="stat-label">Finalizados</p>
                    <p className="stat-value">
                      {formatCount(contactsBreakdownFinalized)}
                    </p>
                    <p className="stat-foot">
                      {formatPercent(
                        contactsBreakdownFinalized,
                        contactsBreakdownTotal
                      )}{" "}
                      do total
                    </p>
                  </div>
                  {contactsBreakdownOther > 0 && (
                    <div className="stat-card">
                      <p className="stat-label">Outros</p>
                      <p className="stat-value">
                        {formatCount(contactsBreakdownOther)}
                      </p>
                      <p className="stat-foot">
                        {formatPercent(
                          contactsBreakdownOther,
                          contactsBreakdownTotal
                        )}{" "}
                        do total
                      </p>
                    </div>
                  )}
                </div>
                {contactsStageList.length > 0 ? (
                  <div className="breakdown-stage-grid">
                    {contactsStageList.map((stage, index) => (
                      <div
                        className="breakdown-chip"
                        key={`${stage.stage_name}-${index}`}
                      >
                        <span>{stage.stage_name}</span>
                        <strong>{formatCount(stage.total)}</strong>
                      </div>
                    ))}
                  </div>
                ) : (
                  <p className="empty">Sem estratificação disponível.</p>
                )}
              </>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

function App() {
  const [isLoggedIn, setIsLoggedIn] = useState(false);

  if (!isLoggedIn) {
    return <LoginScreen onLogin={() => setIsLoggedIn(true)} />;
  }

  return <AppContent onLogout={() => setIsLoggedIn(false)} />;
}

export default App;
