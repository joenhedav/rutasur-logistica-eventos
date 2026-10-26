// CQRS lado lectura: construye envios, alertas y notificaciones en MongoDB. Consumidor idempotente.
const { startConsumer } = require('../shared/consumer');
const { connect, ensureIndexes, applyOnce } = require('../shared/mongo');
const { PRIORIDAD_N } = require('../shared/rules');
const cfg = require('../shared/config');
const T = cfg.topics;

const D = (x) => new Date(x);
const base = (ev) => ({ shipmentId: ev.shipmentId });
const shell = (db, ev, session) => db.collection('envios').updateOne(base(ev), {
  $setOnInsert: { shipmentId: ev.shipmentId, secuenciaAplicada: -1, intentos: 0, historial: [], reprogramaciones: [], notificaciones: [], alertas: [] },
}, { upsert: true, session });

const H = {
  // CU1: estado actual + historial. Regla de orden: secuencia <= aplicada se descarta.
  async [T.ESTADOS](db, s, ev) {
    await shell(db, ev, s);
    const set = {
      estado: ev.tipo, secuenciaAplicada: ev.secuencia, hubId: ev.hubId, zonaId: ev.zonaId,
      'promesa.venceEn': D(ev.venceEn), ultimoEventoEn: D(ev.ocurridoEn), estadoDesde: D(ev.ocurridoEn),
    };
    if (ev.repartidorId) set.repartidorId = ev.repartidorId;
    if (ev.tipo === 'CREADO') set.creadoEn = D(ev.ocurridoEn);
    if (ev.tipo === 'ENTREGA_FALLIDA') set.ultimoMotivo = ev.motivo;
    const entrada = { eventId: ev.eventId, tipo: ev.tipo, ocurridoEn: D(ev.ocurridoEn), secuencia: ev.secuencia };
    if (ev.intento) entrada.intento = ev.intento;
    if (ev.motivo) entrada.motivo = ev.motivo;
    const upd = { $set: set, $push: { historial: entrada } };
    if (ev.intento) upd.$max = { intentos: ev.intento };
    const r = await db.collection('envios').updateOne({ shipmentId: ev.shipmentId, secuenciaAplicada: { $lt: ev.secuencia } }, upd, { session: s });
    return r.matchedCount ? 'aplicado' : 'descartado (secuencia vieja)';
  },
  async [T.REPROGRAMACIONES](db, s, ev) {
    await shell(db, ev, s);
    const set = { destino: ev.tipo === 'REPROGRAMADO' ? 'EN_CIRCUITO' : ev.tipo };
    set.proximoIntento = ev.proximoIntento || null;
    await db.collection('envios').updateOne(base(ev), {
      $set: set,
      $push: { reprogramaciones: { eventId: ev.eventId, tipo: ev.tipo, motivo: ev.motivo, intento: ev.intento, razon: ev.razon, proximoIntento: ev.proximoIntento || null, origenEventId: ev.origenEventId, ocurridoEn: D(ev.ocurridoEn) } },
    }, { session: s });
    return 'aplicado';
  },
  async [T.NOTIFICACIONES](db, s, ev) {
    await shell(db, ev, s);
    const r = await db.collection('notificaciones').updateOne({ eventId: ev.eventId }, {
      $setOnInsert: { eventId: ev.eventId, shipmentId: ev.shipmentId, origen: ev.origen, origenEventId: ev.origenEventId, canal: ev.canal, mensaje: ev.mensaje, opciones: ev.opciones || [], enviadaEn: D(ev.ocurridoEn) },
    }, { upsert: true, session: s });
    if (r.upsertedCount) await db.collection('envios').updateOne(base(ev), {
      $push: { notificaciones: { eventId: ev.eventId, origen: ev.origen, mensaje: ev.mensaje, ocurridoEn: D(ev.ocurridoEn) } },
    }, { session: s });
    return 'aplicado';
  },
  async [T.ALERTAS](db, s, ev) {
    await shell(db, ev, s);
    if (ev.tipo === 'ALERTA_ABIERTA') {
      const r = await db.collection('alertas').updateOne({ shipmentId: ev.shipmentId, regla: ev.regla }, {
        $setOnInsert: { shipmentId: ev.shipmentId, regla: ev.regla, estado: 'ABIERTA', prioridad: ev.prioridad, prioridadN: PRIORIDAD_N[ev.prioridad] || 0, causa: ev.causa, abiertaEn: D(ev.ocurridoEn), venceEn: ev.venceEn ? D(ev.venceEn) : null, hubId: ev.hubId, zonaId: ev.zonaId, eventId: ev.eventId },
      }, { upsert: true, session: s });
      if (r.upsertedCount) await db.collection('envios').updateOne(base(ev), {
        $push: { alertas: { eventId: ev.eventId, regla: ev.regla, prioridad: ev.prioridad, causa: ev.causa, ocurridoEn: D(ev.ocurridoEn) } },
      }, { session: s });
      return 'aplicado';
    }
    await db.collection('alertas').updateMany({ shipmentId: ev.shipmentId, estado: 'ABIERTA' }, { $set: { estado: 'CERRADA', cerradaEn: D(ev.ocurridoEn) } }, { session: s });
    await db.collection('envios').updateOne(base(ev), { $set: { 'alertas.$[a].cerradaEn': D(ev.ocurridoEn) } }, { arrayFilters: [{ 'a.cerradaEn': { $exists: false } }], session: s });
    return 'aplicado';
  },
};

(async () => {
  const m = await connect('proyector');
  await ensureIndexes(m.db);
  await startConsumer({
    service: 'proyector', groupId: 'proyector', topics: cfg.groups.proyector,
    handler: async (ev, ctx) => {
      const r = await applyOnce(m, 'proyector', ev, (s) => H[ctx.topic](m.db, s, ev));
      return r.duplicado ? 'duplicado ignorado' : r.resultado;
    },
  });
})().catch((e) => { console.error(e); process.exit(1); });
