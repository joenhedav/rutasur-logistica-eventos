// Simulador de sistemas de origen (depósito, app del repartidor, comercio). No consume Kafka.
// Flujo continuo + escenarios a pedido + lote reproducible, controlados por una API HTTP mínima (puerto 4000).
const http = require('http');
const cfg = require('../shared/config');
const { makeProducer } = require('../shared/kafka');
const { connect } = require('../shared/mongo');
const { planShipment, inyectar, planBatch } = require('../shared/planner');
const { planScenario, nombres } = require('./scenarios');
const log = require('../shared/log')('simulador');

const sim = { sim0: Date.parse(cfg.simStart), real0: Date.now(), nextIndex: 0, demoSeq: 0, paused: false, rate: cfg.rate };
const simNow = () => sim.sim0 + (Date.now() - sim.real0) * cfg.accel;
const realAt = (simMs) => sim.real0 + (simMs - sim.sim0) / cfg.accel;
let queue = [];
let dirty = false;
let producer;

async function publish(items) {
  const by = new Map();
  for (const it of items) {
    if (!by.has(it.topic)) by.set(it.topic, []);
    by.get(it.topic).push({ key: it.key, value: it.value });
  }
  const topicMessages = [...by].map(([topic, messages]) => ({ topic, messages }));
  if (topicMessages.length) await producer.sendBatch({ topicMessages, acks: -1 });
}
function encolar(items) {
  for (const it of items) queue.push({ ...it, at: realAt(it.simAt) });
  dirty = true;
}

function crearEnvio() {
  const index = sim.nextIndex++;
  const plan = planShipment({ shipmentId: `RS-${String(index).padStart(7, '0')}`, index, seed: cfg.seed, creadoMs: simNow(), demoraHub: cfg.inject.demoraHub });
  inyectar(plan, { seed: cfg.seed, index, p: cfg.inject });
  encolar(plan.items);
  return plan;
}

async function main() {
  const m = await connect('simulador');
  const conf = m.col('config');
  const saved = await conf.findOne({ _id: 'simulador' });
  if (saved) { // el reloj simulado nunca retrocede entre reinicios (+10 min de margen)
    sim.sim0 = saved.simNow + 10 * 60e3; sim.nextIndex = saved.nextIndex; sim.demoSeq = saved.demoSeq || 0;
  }
  sim.real0 = Date.now();
  producer = await makeProducer('simulador');
  const guardar = () => conf.updateOne({ _id: 'simulador' }, { $set: { simNow: simNow(), nextIndex: sim.nextIndex, demoSeq: sim.demoSeq } }, { upsert: true }).catch(() => {});
  setInterval(guardar, 2000);

  // Despacho: publica los eventos cuyo instante (acelerado) ya llegó
  let enviando = false;
  setInterval(async () => {
    if (enviando) return;
    if (dirty) { queue.sort((a, b) => a.at - b.at); dirty = false; }
    const now = Date.now();
    let n = 0;
    while (n < queue.length && queue[n].at <= now && n < 2000) n++;
    if (!n) return;
    const due = queue.splice(0, n);
    enviando = true;
    try { await publish(due); } catch (e) { log('error publicando, se reintenta', { error: e.message }); queue = due.concat(queue); dirty = true; }
    finally { enviando = false; }
  }, 100);

  // Flujo continuo
  let timer;
  const programar = () => { clearInterval(timer); if (sim.rate > 0) timer = setInterval(() => { if (!sim.paused) crearEnvio(); }, 60000 / sim.rate); };
  sim.paused = !cfg.autostart;
  programar();
  log('simulador activo', { accel: cfg.accel, seed: cfg.seed, enviosPorMinuto: sim.rate, flujoContinuo: !sim.paused, relojSim: new Date(simNow()).toISOString() });

  const json = (res, code, body) => { res.writeHead(code, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(body)); };
  http.createServer(async (req, res) => {
    const url = new URL(req.url, 'http://x');
    const p = url.pathname.split('/').filter(Boolean);
    try {
      if (req.method === 'GET' && p[0] === 'status')
        return json(res, 200, { relojSim: new Date(simNow()).toISOString(), accel: cfg.accel, pausado: sim.paused, enviosPorMinuto: sim.rate, pendientesEnCola: queue.length, proximoIndice: sim.nextIndex, semilla: cfg.seed, escenarios: nombres });
      if (req.method !== 'POST') return json(res, 405, { error: 'usar POST' });
      if (p[0] === 'scenario' && nombres.includes(p[1])) {
        const shipmentId = `DEMO-${p[1]}-${++sim.demoSeq}`;
        const items = planScenario(p[1], { shipmentId, ahora: simNow(), accel: cfg.accel, horas: Number(url.searchParams.get('horas')) || undefined });
        encolar(items);
        return json(res, 200, { escenario: p[1], shipmentId, eventos: items.length });
      }
      if (p[0] === 'rafaga') {
        const n = Number(url.searchParams.get('n')) || 500;
        const ahora = simNow();
        for (let i = 0; i < n; i++) {
          const index = sim.nextIndex++;
          const plan = planShipment({ shipmentId: `RS-${String(index).padStart(7, '0')}`, index, seed: cfg.seed, creadoMs: ahora + i * (5000 / n) * cfg.accel, demoraHub: cfg.inject.demoraHub });
          inyectar(plan, { seed: cfg.seed, index, p: cfg.inject });
          encolar(plan.items);
        }
        return json(res, 200, { escenario: 'rafaga', envios: n });
      }
      if (p[0] === 'malformado') {
        const raw = url.searchParams.get('tipo') === 'json' ? '{"eventId":"roto","tipo":' : JSON.stringify({ eventId: `MAL-${Date.now()}`, shipmentId: 'DEMO-malformado', tipo: 'EXPLOTADO', ocurridoEn: 'nunca', secuencia: 'x', version: 1 });
        await publish([{ topic: cfg.topics.ESTADOS, key: 'DEMO-malformado', value: raw }]);
        return json(res, 200, { escenario: 'malformado', publicado: raw });
      }
      if (p[0] === 'pausa') { sim.paused = true; return json(res, 200, { pausado: true }); }
      if (p[0] === 'reanudar') { sim.paused = false; return json(res, 200, { pausado: false }); }
      if (p[0] === 'batch') { // lote reproducible, sin pacing (para validar)
        const count = Number(url.searchParams.get('count')) || 2000;
        const seed = Number(url.searchParams.get('seed')) || cfg.seed;
        const b = planBatch({ seed, count, baseMs: Date.parse(cfg.simStart), p: cfg.inject });
        for (let i = 0; i < b.items.length; i += 500) await publish(b.items.slice(i, i + 500));
        return json(res, 200, { envios: count, seed, mensajes: b.items.length, ...b.stats });
      }
      return json(res, 404, { error: 'ruta desconocida' });
    } catch (e) { return json(res, 500, { error: e.message }); }
  }).listen(cfg.port, () => log('API de control', { puerto: cfg.port }));

  const stop = async () => { await guardar(); process.exit(0); };
  process.on('SIGTERM', stop); process.on('SIGINT', stop);
}
main().catch((e) => { console.error(e); process.exit(1); });
