// CU3: procesamiento con estado + tiempo. Cruza envios.estados con envios.promesas y evalúa:
//  (a) al recibir cada evento y (b) periódicamente para todos los envíos abiertos.
// Usa el RELOJ SIMULADO que viaja en los eventos (no el reloj de la máquina) extrapolado con el factor de aceleración.
const { startConsumer } = require('../shared/consumer');
const { connect, ensureIndexes } = require('../shared/mongo');
const { send } = require('../shared/kafka');
const { reglasActivas } = require('../shared/rules');
const cfg = require('../shared/config');
const log = require('../shared/log')('monitor-sla');
const T = cfg.topics;
const G = 'monitor-sla';

const estado = new Map();       // shipmentId -> estado interno en memoria (respaldado en MongoDB)
const cerrados = new Set();     // envíos ya entregados y cerrados
let ref = { sim: 0, wall: Date.now() };
const simNow = () => ref.sim + (Date.now() - ref.wall) * cfg.accel;
const observar = (simMs) => { if (simMs > simNow()) ref = { sim: simMs, wall: Date.now() }; };

(async () => {
  const m = await connect(G);
  await ensureIndexes(m.db);
  const col = m.col('sla_estado');
  const meta = m.col('sla_meta');
  let producer;

  // Tolerancia a reinicios: recupera estado interno y reloj desde MongoDB
  for await (const d of col.find({})) {
    if (d.cerrada) cerrados.add(d.shipmentId);
    else estado.set(d.shipmentId, { ...d, enCurso: false });
  }
  const r = await meta.findOne({ _id: 'reloj' });
  if (r) ref = { sim: r.sim, wall: Date.now() };
  log('estado recuperado', { abiertos: estado.size, cerrados: cerrados.size, relojSim: new Date(ref.sim).toISOString() });

  const persistir = (s) => {
    const { _id, enCurso, ...doc } = s;
    return col.replaceOne({ shipmentId: s.shipmentId }, doc, { upsert: true });
  };
  const obtener = (ev) => {
    let s = estado.get(ev.shipmentId);
    if (!s) {
      s = { shipmentId: ev.shipmentId, estado: null, secuencia: -1, venceEn: null, ultimoFalloEn: null, emitidas: [], cerrada: false, hubId: ev.hubId, zonaId: ev.zonaId };
      estado.set(ev.shipmentId, s);
    }
    return s;
  };

  async function evaluar(s) {
    if (s.enCurso || s.cerrada) return;
    s.enCurso = true;
    try {
      for (const a of reglasActivas(s, simNow())) {
        if (s.emitidas.includes(a.regla)) continue; // sin repetición: una regla alerta una sola vez por envío
        // Se publica ANTES de persistir; si hay caída entre ambos pasos, la alerta se republica con el mismo
        // eventId determinista y el proyector la descarta (no se pierde ni se duplica).
        await send(producer, T.ALERTAS, {
          eventId: `alerta-${s.shipmentId}-${a.regla}`, shipmentId: s.shipmentId, tipo: 'ALERTA_ABIERTA',
          regla: a.regla, prioridad: a.prioridad, causa: a.causa, ocurridoEn: new Date(simNow()).toISOString(),
          secuencia: Math.max(s.secuencia, 0), hubId: s.hubId, zonaId: s.zonaId, repartidorId: null,
          venceEn: new Date(s.venceEn).toISOString(), version: 1,
        });
        s.emitidas.push(a.regla);
        await persistir(s);
        log('alerta emitida', { shipmentId: s.shipmentId, regla: a.regla, prioridad: a.prioridad });
      }
    } finally { s.enCurso = false; }
  }

  async function cerrar(s, ev) {
    if (s.emitidas.length) {
      await send(producer, T.ALERTAS, {
        eventId: `cierre-${s.shipmentId}`, shipmentId: s.shipmentId, tipo: 'ALERTA_CERRADA', reglas: s.emitidas,
        ocurridoEn: ev.ocurridoEn, secuencia: ev.secuencia, hubId: s.hubId, zonaId: s.zonaId, repartidorId: null, version: 1,
      });
      log('alerta cerrada', { shipmentId: s.shipmentId });
    }
    s.cerrada = true;
    await persistir(s);
    estado.delete(s.shipmentId);
    cerrados.add(s.shipmentId);
  }

  const c = await startConsumer({
    service: G, groupId: G, topics: cfg.groups[G],
    handler: async (ev, ctx) => {
      producer = ctx.producer;
      if (cerrados.has(ev.shipmentId)) return 'envío ya cerrado';
      const s = obtener(ev);
      if (ctx.topic === T.PROMESAS) {
        s.venceEn = Date.parse(ev.venceEn);
        observar(Date.parse(ev.ocurridoEn));
        await persistir(s);
        await evaluar(s);
        return 'promesa registrada';
      }
      if (ev.secuencia <= s.secuencia) return 'descartado (secuencia vieja)';
      observar(Date.parse(ev.ocurridoEn));
      s.estado = ev.tipo; s.secuencia = ev.secuencia; s.hubId = ev.hubId; s.zonaId = ev.zonaId;
      if (ev.tipo === 'ENTREGA_FALLIDA') s.ultimoFalloEn = Date.parse(ev.ocurridoEn);
      if (ev.tipo === 'ENTREGADO') { await cerrar(s, ev); return 'envío cerrado'; }
      await persistir(s);
      await evaluar(s);
      return 'estado actualizado';
    },
  });
  producer = c.producer;

  // Evaluación periódica: la alerta nace del paso del tiempo, no de un evento
  setInterval(async () => {
    for (const s of [...estado.values()]) {
      try { await evaluar(s); } catch (e) { log('error evaluando', { error: e.message }); }
    }
  }, cfg.slaEvalMs);
  setInterval(() => meta.updateOne({ _id: 'reloj' }, { $set: { sim: simNow() } }, { upsert: true }).catch(() => {}), 5000);
})().catch((e) => { console.error(e); process.exit(1); });
