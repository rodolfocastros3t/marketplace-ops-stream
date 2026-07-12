import { useEffect, useId, useReducer, useRef, useState } from "react";
import {
  Chart as ChartJS,
  ArcElement,
  BarElement,
  CategoryScale,
  LinearScale,
  Tooltip,
  Legend,
} from "chart.js";
import { Doughnut, Bar } from "react-chartjs-2";

ChartJS.register(ArcElement, BarElement, CategoryScale, LinearScale, Tooltip, Legend);

const KANBAN = ["CRIADO", "PAGO", "SEPARACAO", "ENVIADO", "EM_TRANSITO", "ENTREGUE"];
const LANE_LABEL = {
  CRIADO: "Criado",
  PAGO: "Pago",
  SEPARACAO: "Separação",
  ENVIADO: "Enviado",
  EM_TRANSITO: "Em trânsito",
  ENTREGUE: "Entregue",
};

const WS_URL = `${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/ws`;

const HELP = {
  kpis: "Indicadores do marketplace em tempo real. Atualizam conforme o stream de pedidos, pagamentos e lives chega na API.",
  gmv: "Gross Merchandise Value — soma do valor de todos os pedidos processados na sessão (em R$).",
  ppm: "Quantidade aproximada de pedidos recebidos no último minuto.",
  ticket: "Valor médio por pedido (GMV ÷ quantidade de pedidos).",
  pix: "Percentual de pedidos pagos com PIX — diferencial competitivo no Brasil.",
  livePct: "Percentual de pedidos originados em live commerce (compra durante transmissão).",
  criticos: "Quantidade de alertas com severidade CRÍTICO ainda visíveis no painel.",
  kanban: "Quadro do ciclo de vida dos pedidos. Cada coluna é um status; os cards se movem sozinhos via WebSocket, sem recarregar a página.",
  pagamento: "Distribuição entre PIX e outros meios (cartão, boleto). Ajuda a acompanhar a estratégia PIX First.",
  hubsChart: "Ocupação percentual dos hubs logísticos por UF. Barras vermelhas indicam saturação (≥ 85% da capacidade).",
  alertas: "Eventos roteados automaticamente pelo stream (fraude, logística, marketing, operações). A faixa colorida indica a severidade.",
  mapa: "Mapa dos hubs logísticos no Brasil. Clique em um ponto para ver carga, capacidade livre, risco de atraso, região e ação sugerida. Abaixo, a live commerce ativa.",
  hubDetail: "Detalhe operacional do hub: ocupação, fila estimada, SLA de frete da UF e recomendação em caso de saturação.",
  stream: "Status da conexão WebSocket com a API. Se cair, o dashboard tenta reconectar automaticamente.",
  produtor: "Liga/desliga o gerador de eventos do simulador (modo local). No pipeline Kafka, o produtor roda em container separado.",
};

const initial = {
  connected: false,
  kpis: null,
  pedidos: {},
  alertas: [],
  hubs: {},
  live: null,
};

function reducer(state, action) {
  switch (action.type) {
    case "connected":
      return { ...state, connected: action.value };
    case "kpis":
      return { ...state, kpis: action.payload };
    case "pedido": {
      const pedidos = { ...state.pedidos, [action.payload.id]: action.payload };
      return { ...state, pedidos };
    }
    case "pedido_status": {
      const prev = state.pedidos[action.payload.id];
      if (!prev) return state;
      return {
        ...state,
        pedidos: {
          ...state.pedidos,
          [action.payload.id]: { ...prev, status: action.payload.status },
        },
      };
    }
    case "alerta":
      return { ...state, alertas: [action.payload, ...state.alertas].slice(0, 40) };
    case "hub":
      return { ...state, hubs: { ...state.hubs, [action.payload.id]: action.payload } };
    case "live":
      return { ...state, live: action.payload };
    default:
      return state;
  }
}

function brl(centavos) {
  return (centavos / 100).toLocaleString("pt-BR", { style: "currency", currency: "BRL" });
}

const REGIAO_UF = {
  AC: "Norte",
  AP: "Norte",
  AM: "Norte",
  PA: "Norte",
  RO: "Norte",
  RR: "Norte",
  TO: "Norte",
  AL: "Nordeste",
  BA: "Nordeste",
  CE: "Nordeste",
  MA: "Nordeste",
  PB: "Nordeste",
  PE: "Nordeste",
  PI: "Nordeste",
  RN: "Nordeste",
  SE: "Nordeste",
  DF: "Centro-Oeste",
  GO: "Centro-Oeste",
  MT: "Centro-Oeste",
  MS: "Centro-Oeste",
  ES: "Sudeste",
  MG: "Sudeste",
  RJ: "Sudeste",
  SP: "Sudeste",
  PR: "Sul",
  RS: "Sul",
  SC: "Sul",
};

/** Projeção simples do território brasileiro no painel do mapa */
function hubMapPosition(lat, lng) {
  const left = ((lng + 74) / 40) * 100;
  const top = ((5 - lat) / 38) * 100;
  return {
    left: `${Math.min(92, Math.max(8, left))}%`,
    top: `${Math.min(88, Math.max(10, top))}%`,
  };
}

function hubInsights(hub) {
  const pct = Math.round((hub.carga_atual / hub.capacidade) * 100);
  const livres = Math.max(0, hub.capacidade - hub.carga_atual);
  const regiao = REGIAO_UF[hub.uf] || "Brasil";
  const filaEstimada = Math.max(3, Math.round(hub.carga_atual * 0.45));
  const prazoBase = { Norte: 5, Nordeste: 4, "Centro-Oeste": 3, Sudeste: 2, Sul: 2 }[regiao] || 3;
  const prazoDias = hub.saturado ? prazoBase + 2 : prazoBase;
  let risco = "Baixo";
  let riscoClass = "text-brand bg-brand/10";
  if (pct >= 85) {
    risco = "Alto";
    riscoClass = "text-clay bg-clay/10";
  } else if (pct >= 65) {
    risco = "Médio";
    riscoClass = "text-ink bg-gold/30";
  }
  const acao = hub.saturado
    ? `Redistribuir pedidos de ${hub.uf} para hub vizinho e priorizar PIX/express na saída.`
    : `Hub estável — manter janela de coleta e campanhas locais em ${hub.cidade}.`;
  return { pct, livres, regiao, filaEstimada, prazoDias, risco, riscoClass, acao };
}

const sevBorder = {
  CRITICO: "border-l-clay",
  ATENCAO: "border-l-gold",
  INFO: "border-l-sky",
};

const sevBadge = {
  CRITICO: "bg-clay/12 text-clay",
  ATENCAO: "bg-gold/30 text-ink",
  INFO: "bg-sky/10 text-sky",
};

function HelpTip({ text, label = "Ajuda sobre esta área" }) {
  const [open, setOpen] = useState(false);
  const tipId = useId();
  const rootRef = useRef(null);

  useEffect(() => {
    if (!open) return;
    function onDoc(e) {
      if (rootRef.current && !rootRef.current.contains(e.target)) setOpen(false);
    }
    function onKey(e) {
      if (e.key === "Escape") setOpen(false);
    }
    document.addEventListener("mousedown", onDoc);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onDoc);
      document.removeEventListener("keydown", onKey);
    };
  }, [open]);

  return (
    <span ref={rootRef} className="relative inline-flex shrink-0">
      <button
        type="button"
        aria-label={label}
        aria-expanded={open}
        aria-controls={tipId}
        onClick={() => setOpen((v) => !v)}
        className="grid h-6 w-6 place-items-center rounded-full border border-line bg-cream text-[0.72rem] font-bold text-sky transition hover:border-sky hover:bg-white hover:text-sky focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky"
      >
        ?
      </button>
      {open && (
        <span
          id={tipId}
          role="tooltip"
          className="absolute left-0 top-[calc(100%+0.4rem)] z-30 w-[min(18rem,70vw)] border border-line bg-white p-3 text-left text-[0.8rem] leading-snug text-ink shadow-[0_12px_28px_rgb(10_31_20/0.12)] animate-pop sm:left-auto sm:right-0"
        >
          {text}
        </span>
      )}
    </span>
  );
}

function SectionHead({ title, help, subtitle }) {
  return (
    <div className="mb-3.5">
      <div className="flex items-start justify-between gap-2">
        <h2 className="font-display text-[1.35rem] leading-tight">{title}</h2>
        <HelpTip text={help} label={`Ajuda: ${title}`} />
      </div>
      {subtitle ? <p className="mt-1 text-[0.92rem] text-soft">{subtitle}</p> : null}
    </div>
  );
}

function App() {
  const [state, dispatch] = useReducer(reducer, initial);
  const [simulatorOn, setSimulatorOn] = useState(true);
  const [busy, setBusy] = useState(false);
  const [selectedHubId, setSelectedHubId] = useState(null);
  const wsRef = useRef(null);

  useEffect(() => {
    let closed = false;
    let retry;

    function connect() {
      const ws = new WebSocket(WS_URL);
      wsRef.current = ws;
      ws.onopen = () => {
        dispatch({ type: "connected", value: true });
        ws.send(
          JSON.stringify({
            event: "client.subscribe",
            channels: ["pedidos", "alertas", "kpis", "hubs", "lives"],
          })
        );
      };
      ws.onclose = () => {
        dispatch({ type: "connected", value: false });
        if (!closed) retry = setTimeout(connect, 2000);
      };
      ws.onmessage = (ev) => {
        const msg = JSON.parse(ev.data);
        const { event, payload } = msg;
        if (event === "kpis.updated") dispatch({ type: "kpis", payload });
        else if (event === "pedido.created") dispatch({ type: "pedido", payload });
        else if (event === "pedido.status_changed") dispatch({ type: "pedido_status", payload });
        else if (event === "alerta.routed") dispatch({ type: "alerta", payload });
        else if (event === "hub.updated") dispatch({ type: "hub", payload });
        else if (event === "live.updated") dispatch({ type: "live", payload });
      };
    }

    connect();
    return () => {
      closed = true;
      clearTimeout(retry);
      wsRef.current?.close();
    };
  }, []);

  async function toggleSimulator() {
    setBusy(true);
    try {
      const path = simulatorOn ? "/api/v1/simulator/stop" : "/api/v1/simulator/start";
      await fetch(path, { method: "POST" });
      setSimulatorOn(!simulatorOn);
    } finally {
      setBusy(false);
    }
  }

  const k = state.kpis;
  const pedidosList = Object.values(state.pedidos);
  const hubs = Object.values(state.hubs);
  const selectedHub = selectedHubId ? state.hubs[selectedHubId] : null;
  const pedidosNaUf = selectedHub
    ? pedidosList.filter((p) => p.frete?.uf_destino === selectedHub.uf).length
    : 0;
  const atualizado = k?.atualizado_em
    ? new Date(k.atualizado_em).toLocaleTimeString("pt-BR", {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
      })
    : null;

  const payChart = {
    labels: ["PIX", "Outros"],
    datasets: [
      {
        data: [k?.percentual_pix ?? 0, 100 - (k?.percentual_pix ?? 0)],
        backgroundColor: ["#009b3a", "#002776"],
        borderWidth: 0,
      },
    ],
  };

  const hubChart = {
    labels: hubs.map((h) => h.uf),
    datasets: [
      {
        label: "Carga %",
        data: hubs.map((h) => Math.round((h.carga_atual / h.capacidade) * 100)),
        backgroundColor: hubs.map((h) => (h.saturado ? "#b42318" : "#009b3a")),
      },
    ],
  };

  return (
    <div className="mx-auto max-w-[1280px] px-5 pb-14 pt-5">
      <header className="animate-rise mb-5 flex flex-wrap items-end justify-between gap-4 border-b border-line pb-5">
        <div>
          <h1 className="font-display text-[clamp(2.35rem,5.2vw,3.5rem)] leading-[0.95] tracking-tight text-ink">
            <span className="text-pine">Ops</span>
            <span className="mx-1.5 text-gold" aria-hidden>
              ·
            </span>
            <span className="text-sky">Stream</span>
          </h1>
          <span className="brasil-rail" aria-hidden />
          <p className="mt-2.5 max-w-md text-[0.98rem] leading-snug text-soft">
            Marketplace brasileiro em tempo real — operação, live e PIX no mesmo stream.
          </p>
          {atualizado && (
            <p className="mt-2 text-[0.78rem] tabular-nums text-soft">
              Última atualização do stream: {atualizado}
            </p>
          )}
        </div>
        <div className="flex flex-wrap items-center gap-2.5">
          <div className="flex items-center gap-1.5">
            <span
              className={`inline-flex items-center gap-2 border bg-panel px-3 py-1.5 text-sm ${
                state.connected
                  ? "border-sky/25 text-sky"
                  : "border-clay/30 text-clay"
              }`}
            >
              <span
                className={`h-2 w-2 rounded-full ${
                  state.connected ? "bg-brand animate-pulse-hub" : "bg-clay"
                }`}
                aria-hidden
              />
              {state.connected ? "Stream ao vivo" : "Reconectando…"}
            </span>
            <HelpTip text={HELP.stream} label="Ajuda: conexão do stream" />
          </div>
          <div className="flex items-center gap-1.5">
            <button
              type="button"
              disabled={busy}
              onClick={toggleSimulator}
              className="cursor-pointer border-0 bg-pine px-4 py-2.5 font-sans font-medium text-white transition hover:-translate-y-px hover:bg-brand disabled:cursor-wait disabled:opacity-70 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-gold"
            >
              {simulatorOn ? "Pausar produtor" : "Iniciar produtor"}
            </button>
            <HelpTip text={HELP.produtor} label="Ajuda: produtor de eventos" />
          </div>
        </div>
      </header>

      <section aria-label="KPIs" className="mb-5">
        <div className="mb-2.5 flex items-center gap-2">
          <h2 className="text-[0.78rem] font-medium uppercase tracking-wide text-soft">
            Indicadores
          </h2>
          <HelpTip text={HELP.kpis} label="Ajuda: indicadores" />
        </div>
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
          <Kpi label="GMV" value={k ? brl(k.gmv_centavos) : "—"} help={HELP.gmv} />
          <Kpi
            label="Pedidos / min"
            value={k ? k.pedidos_por_minuto.toFixed(0) : "—"}
            help={HELP.ppm}
          />
          <Kpi
            label="Ticket médio"
            value={k ? brl(k.ticket_medio_centavos) : "—"}
            help={HELP.ticket}
          />
          <Kpi label="% PIX" value={k ? `${k.percentual_pix}%` : "—"} help={HELP.pix} />
          <Kpi label="% Live" value={k ? `${k.percentual_live}%` : "—"} help={HELP.livePct} />
          <Kpi
            label="Alertas críticos"
            value={k ? String(k.alertas_criticos_abertos) : "—"}
            help={HELP.criticos}
            accent
          />
        </div>
      </section>

      <div className="mb-4 grid grid-cols-1 gap-4 lg:grid-cols-[1.6fr_0.9fr]">
        <section className="animate-rise border border-line bg-panel p-4 shadow-[0_18px_40px_rgb(10_31_20/0.08)]">
          <SectionHead
            title="Kanban de pedidos"
            help={HELP.kanban}
            subtitle="Ciclo de vida ao vivo — os cards mudam de coluna sem reload"
          />
          <div className="grid grid-cols-2 gap-2 overflow-x-auto md:grid-cols-3 xl:grid-cols-6">
            {KANBAN.map((lane) => {
              const items = pedidosList.filter((p) => p.status === lane);
              return (
                <div key={lane} className="min-w-0 border border-line bg-brand/[0.04] p-2">
                  <h3 className="mb-2 flex items-center justify-between gap-1.5 text-[0.68rem] uppercase tracking-wide text-soft">
                    <span>{LANE_LABEL[lane]}</span>
                    <span className="rounded-sm bg-sky/10 px-1.5 py-0.5 tabular-nums text-sky">
                      {items.length}
                    </span>
                  </h3>
                  <ul className="m-0 flex min-h-[4.5rem] list-none flex-col gap-1.5 p-0">
                    {items.length === 0 && (
                      <li className="px-1 py-2 text-[0.7rem] text-soft/80">Vazio</li>
                    )}
                    {items.slice(0, 6).map((p) => (
                      <li
                        key={p.id}
                        className="animate-pop border border-line bg-white px-2 py-1.5 transition hover:border-brand/40"
                        title={`Pedido ${p.id.slice(0, 8)}…`}
                      >
                        <strong className="block text-sm tabular-nums text-ink">
                          {brl(p.total_centavos)}
                        </strong>
                        <span className="text-[0.7rem] text-soft">
                          {p.pagamento_metodo} · {p.origem} · {p.frete.uf_destino}
                        </span>
                      </li>
                    ))}
                  </ul>
                </div>
              );
            })}
          </div>
        </section>

        <aside className="flex flex-col gap-4">
          <section className="animate-rise border border-line bg-panel p-4 shadow-[0_18px_40px_rgb(10_31_20/0.08)]">
            <SectionHead title="Mix de pagamento" help={HELP.pagamento} />
            <div className="mx-auto mt-1 max-w-[220px]">
              <Doughnut
                data={payChart}
                options={{
                  plugins: {
                    legend: {
                      position: "bottom",
                      labels: { boxWidth: 10, font: { size: 11 }, color: "#0a1f14" },
                    },
                  },
                  cutout: "62%",
                }}
              />
            </div>
            <p className="mt-2 text-center text-[0.72rem] text-soft">
              <span className="text-brand">Verde</span> = PIX ·{" "}
              <span className="text-sky">Azul</span> = outros
            </p>
          </section>

          <section className="animate-rise border border-line bg-panel p-4 shadow-[0_18px_40px_rgb(10_31_20/0.08)]">
            <SectionHead
              title="Carga dos hubs"
              help={HELP.hubsChart}
              subtitle="Verde = operacional · Vermelho = saturado"
            />
            <div className="mt-1 h-[180px]">
              {hubs.length === 0 ? (
                <p className="grid h-full place-items-center text-sm text-soft">
                  Aguardando dados dos hubs…
                </p>
              ) : (
                <Bar
                  data={hubChart}
                  options={{
                    maintainAspectRatio: false,
                    plugins: { legend: { display: false } },
                    scales: {
                      y: {
                        max: 100,
                        ticks: { callback: (v) => `${v}%`, font: { size: 10 }, color: "#3a5548" },
                      },
                      x: { ticks: { font: { size: 10 }, color: "#3a5548" } },
                    },
                  }}
                />
              )}
            </div>
          </section>
        </aside>
      </div>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <section className="animate-rise border border-line bg-panel p-4 shadow-[0_18px_40px_rgb(10_31_20/0.08)]">
          <SectionHead
            title="Alertas roteados"
            help={HELP.alertas}
            subtitle="Destino definido por regras do stream (fraude, logística, marketing, operações)"
          />
          <ul className="m-0 flex max-h-[360px] list-none flex-col gap-2.5 overflow-auto p-0">
            {state.alertas.length === 0 && (
              <li className="border border-dashed border-line px-3 py-6 text-center text-soft">
                Nenhum alerta ainda — o stream publicará aqui quando houver incidentes.
              </li>
            )}
            {state.alertas.map((a) => (
              <li
                key={a.id}
                className={`animate-pop border-l-[3px] bg-white/70 px-3 py-2.5 transition hover:bg-white ${sevBorder[a.severidade] || "border-l-sand"}`}
              >
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <strong className="text-[0.95rem] text-ink">{a.tipo.replaceAll("_", " ")}</strong>
                  <span className="flex items-center gap-1.5">
                    <span
                      className={`px-1.5 py-0.5 text-[0.65rem] font-medium uppercase tracking-wide ${sevBadge[a.severidade] || ""}`}
                    >
                      {a.severidade}
                    </span>
                    <span className="text-[0.68rem] uppercase tracking-wide text-soft">
                      {a.destino}
                    </span>
                  </span>
                </div>
                <p className="mt-1.5 mb-0 text-[0.9rem] text-ink/90">{a.mensagem}</p>
              </li>
            ))}
          </ul>
        </section>

        <section className="animate-rise border border-line bg-panel p-4 shadow-[0_18px_40px_rgb(10_31_20/0.08)]">
          <SectionHead
            title="Mapa de hubs + Live"
            help={HELP.mapa}
            subtitle="Clique em um hub para abrir o painel operacional"
          />
          <div
            className="relative my-2 h-[240px] overflow-hidden border border-line"
            style={{
              backgroundColor: "#f4f8fb",
              backgroundImage:
                "linear-gradient(rgb(0 39 118 / 0.07) 1px, transparent 1px), linear-gradient(90deg, rgb(0 155 58 / 0.07) 1px, transparent 1px)",
              backgroundSize: "24px 24px, 24px 24px",
            }}
          >
            {hubs.length === 0 && (
              <p className="absolute inset-0 grid place-items-center text-sm text-soft">
                Mapa aguardando hubs…
              </p>
            )}
            {hubs.map((h) => {
              const pct = Math.round((h.carga_atual / h.capacidade) * 100);
              const selected = selectedHubId === h.id;
              const pos = hubMapPosition(h.lat, h.lng);
              return (
                <button
                  key={h.id}
                  type="button"
                  onClick={() => setSelectedHubId((id) => (id === h.id ? null : h.id))}
                  className={`absolute grid h-8 w-8 -translate-x-1/2 -translate-y-1/2 cursor-pointer place-items-center rounded-full border-2 text-[0.65rem] font-bold text-white transition hover:scale-110 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-gold ${
                    h.saturado ? "animate-pulse-hot bg-clay" : "animate-pulse-hub bg-brand"
                  } ${selected ? "z-10 scale-110 border-gold shadow-[0_0_0_4px_rgb(230_184_0/0.35)]" : "border-white/70"}`}
                  style={pos}
                  title={`${h.nome} — ${pct}% · clique para detalhes`}
                  aria-pressed={selected}
                  aria-label={`${h.nome}, carga ${pct}%${h.saturado ? ", saturado" : ""}. Clique para detalhes.`}
                >
                  {h.uf}
                </button>
              );
            })}
          </div>
          <div className="mb-3 flex flex-wrap gap-3 text-[0.72rem] text-soft">
            <span className="inline-flex items-center gap-1.5">
              <span className="h-2.5 w-2.5 rounded-full bg-brand" aria-hidden /> Operacional
            </span>
            <span className="inline-flex items-center gap-1.5">
              <span className="h-2.5 w-2.5 rounded-full bg-clay" aria-hidden /> Saturado
            </span>
            <span className="inline-flex items-center gap-1.5">
              <span className="h-2.5 w-2.5 rounded-full border-2 border-gold bg-white" aria-hidden />{" "}
              Selecionado
            </span>
          </div>

          {selectedHub ? (
            <HubDetail
              hub={selectedHub}
              pedidosNaUf={pedidosNaUf}
              onClose={() => setSelectedHubId(null)}
            />
          ) : (
            <p className="mb-3 border border-dashed border-line px-3 py-3 text-center text-[0.88rem] text-soft">
              Selecione um hub no mapa para ver carga, fila, SLA e ação sugerida.
            </p>
          )}

          {state.live ? (
            <div className="border border-gold/40 bg-gradient-to-br from-gold/25 via-white to-sky/5 px-4 py-3.5">
              <div className="mb-1.5 flex items-center gap-2">
                <span className="inline-flex items-center gap-1.5 rounded-sm bg-sky px-1.5 py-0.5 text-[0.65rem] font-semibold uppercase tracking-wider text-white">
                  <span className="h-1.5 w-1.5 rounded-full bg-gold animate-pulse-hot" aria-hidden />
                  {state.live.status.replace("_", " ")}
                </span>
              </div>
              <strong className="block font-display text-[1.05rem] text-ink">
                {state.live.titulo}
              </strong>
              <p className="mt-1.5 mb-0 text-[0.9rem] text-soft">
                {state.live.espectadores.toLocaleString("pt-BR")} espectadores · UF{" "}
                {state.live.vendedor_uf} · {state.live.pedidos_na_live} pedidos na live
              </p>
            </div>
          ) : (
            <p className="border border-dashed border-line px-3 py-4 text-center text-soft">
              Nenhuma live ativa no momento
            </p>
          )}
        </section>
      </div>
    </div>
  );
}

function HubDetail({ hub, pedidosNaUf, onClose }) {
  const info = hubInsights(hub);
  return (
    <div className="animate-pop mb-3 border border-sky/20 bg-white p-3.5 shadow-[0_10px_24px_rgb(0_39_118/0.06)]">
      <div className="mb-3 flex items-start justify-between gap-2">
        <div>
          <div className="flex flex-wrap items-center gap-2">
            <h3 className="font-display text-[1.15rem] text-ink">{hub.nome}</h3>
            <HelpTip text={HELP.hubDetail} label="Ajuda: detalhe do hub" />
          </div>
          <p className="mt-0.5 text-[0.85rem] text-soft">
            {hub.cidade}/{hub.uf} · Região {info.regiao}
          </p>
        </div>
        <button
          type="button"
          onClick={onClose}
          className="border border-line bg-cream px-2 py-1 text-[0.75rem] text-soft transition hover:border-sky hover:text-sky"
        >
          Fechar
        </button>
      </div>

      <div className="mb-3 flex flex-wrap gap-2">
        <span
          className={`px-2 py-0.5 text-[0.68rem] font-semibold uppercase tracking-wide ${
            hub.saturado ? "bg-clay/12 text-clay" : "bg-brand/12 text-brand"
          }`}
        >
          {hub.saturado ? "Saturado" : "Operacional"}
        </span>
        <span
          className={`px-2 py-0.5 text-[0.68rem] font-semibold uppercase tracking-wide ${info.riscoClass}`}
        >
          Risco de atraso: {info.risco}
        </span>
      </div>

      <div className="mb-2 flex items-end justify-between gap-2 text-[0.8rem]">
        <span className="text-soft">Ocupação</span>
        <strong className="tabular-nums text-ink">{info.pct}%</strong>
      </div>
      <div
        className="mb-3 h-2.5 overflow-hidden bg-mist"
        role="progressbar"
        aria-valuenow={info.pct}
        aria-valuemin={0}
        aria-valuemax={100}
      >
        <div
          className={`h-full transition-all ${
            hub.saturado ? "bg-clay" : info.pct >= 65 ? "bg-gold" : "bg-brand"
          }`}
          style={{ width: `${Math.min(100, info.pct)}%` }}
        />
      </div>

      <dl className="mb-3 grid grid-cols-2 gap-2 sm:grid-cols-3">
        <Metric label="Carga" value={`${hub.carga_atual}/${hub.capacidade}`} />
        <Metric label="Vagas livres" value={String(info.livres)} />
        <Metric label="Fila estimada" value={`${info.filaEstimada} ped.`} />
        <Metric label="SLA frete" value={`${info.prazoDias} dias`} />
        <Metric label="Pedidos p/ UF" value={String(pedidosNaUf)} />
        <Metric label="Cobertura" value={`Destinos ${hub.uf}`} />
      </dl>

      <div
        className={`border-l-[3px] px-3 py-2 text-[0.86rem] leading-snug ${
          hub.saturado ? "border-l-gold bg-gold/15 text-ink" : "border-l-brand bg-brand/5 text-ink"
        }`}
      >
        <p className="mb-0.5 text-[0.68rem] font-semibold uppercase tracking-wide text-soft">
          Ação sugerida
        </p>
        <p className="m-0">{info.acao}</p>
      </div>
    </div>
  );
}

function Metric({ label, value }) {
  return (
    <div className="border border-line bg-cream/60 px-2.5 py-2">
      <dt className="text-[0.68rem] uppercase tracking-wide text-soft">{label}</dt>
      <dd className="m-0 mt-0.5 text-[0.95rem] font-semibold tabular-nums text-ink">{value}</dd>
    </div>
  );
}

function Kpi({ label, value, accent, help }) {
  return (
    <article className="animate-rise border border-line bg-panel px-4 py-3.5 shadow-[0_18px_40px_rgb(10_31_20/0.08)] transition hover:border-brand/35">
      <div className="flex items-start justify-between gap-1">
        <span className="block text-[0.78rem] font-medium uppercase tracking-wide text-soft">
          {label}
        </span>
        {help ? <HelpTip text={help} label={`Ajuda: ${label}`} /> : null}
      </div>
      <strong
        className={`mt-1.5 block text-[1.25rem] tabular-nums ${accent ? "text-clay" : "text-ink"}`}
      >
        {value}
      </strong>
    </article>
  );
}

export default App;
