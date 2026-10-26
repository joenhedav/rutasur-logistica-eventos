// CU4: indicadores 1-3 con incrementos atómicos ($inc) sobre metricas_diarias.
const { startConsumer } = require('../shared/consumer');
const { connect, ensureIndexes, applyOnce } = require('../shared/mongo');
const { diaISO, franjaDe } = require('../shared/rules');
const cfg = require('../shared/config');
const G = 'analitica';

(async () => {
  const m = await connect(G);
  await ensureIndexes(m.db);
  await startConsumer({
    service: G, groupId: G, topics: cfg.groups[G],
    handler: async (ev) => {
      if (ev.tipo !== 'ENTREGADO' && ev.tipo !== 'ENTREGA_FALLIDA') return 'ignorado';
      const inc = {};
      if (ev.intento === 1) { // tasa de entrega en primer intento
        inc['primerIntento.total'] = 1;
        if (ev.tipo === 'ENTREGADO') inc['primerIntento.entregados'] = 1;
      }
      if (ev.tipo === 'ENTREGA_FALLIDA') { inc.fallos = 1; inc[`motivos.${ev.motivo}`] = 1; } // distribución de motivos
      if (ev.tipo === 'ENTREGADO') { // cumplimiento de SLA por hub y día
        inc['sla.entregados'] = 1;
        if (Date.parse(ev.ocurridoEn) > Date.parse(ev.venceEn)) inc['sla.tarde'] = 1;
      }
      const filtro = { fecha: diaISO(ev.ocurridoEn), zonaId: ev.zonaId, franja: franjaDe(ev.ocurridoEn) };
      const r = await applyOnce(m, G, ev, (session) =>
        m.col('metricas_diarias').updateOne(filtro, { $inc: inc, $setOnInsert: { hubId: ev.hubId } }, { upsert: true, session }));
      return r.duplicado ? 'duplicado ignorado' : 'métricas incrementadas';
    },
  });
})().catch((e) => { console.error(e); process.exit(1); });
