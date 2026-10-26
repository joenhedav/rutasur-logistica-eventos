// CU4 (reprocesamiento): CUARTO indicador = tiempo medio entre INGRESADO_HUB y EN_REPARTO por hub.
// Es un grupo de consumidores NUEVO que lee envios.estados desde el offset 0 (fromBeginning) y completa
// el histórico sin tocar a los demás servicios. Independiente del orden de llegada de los dos eventos.
const { startConsumer } = require('../shared/consumer');
const { connect, ensureIndexes, applyOnce } = require('../shared/mongo');
const { diaISO, franjaDe } = require('../shared/rules');
const cfg = require('../shared/config');
const G = 'analitica-tiempo-hub';

(async () => {
  const m = await connect(G);
  await ensureIndexes(m.db);
  await startConsumer({
    service: G, groupId: G, topics: cfg.groups[G], fromBeginning: true,
    handler: async (ev) => {
      if (ev.tipo !== 'INGRESADO_HUB' && ev.tipo !== 'EN_REPARTO') return 'ignorado';
      const campo = ev.tipo === 'INGRESADO_HUB' ? 'ingresoEn' : 'repartoEn';
      const r = await applyOnce(m, G, ev, async (session) => {
        const col = m.col('tiempo_hub_estado');
        await col.updateOne({ shipmentId: ev.shipmentId }, { $min: { [campo]: new Date(ev.ocurridoEn) }, $setOnInsert: { hubId: ev.hubId, zonaId: ev.zonaId, contado: false } }, { upsert: true, session });
        const d = await col.findOne({ shipmentId: ev.shipmentId }, { session });
        if (d.contado || !d.ingresoEn || !d.repartoEn || d.repartoEn < d.ingresoEn) return 'parcial';
        await col.updateOne({ shipmentId: ev.shipmentId }, { $set: { contado: true } }, { session });
        const filtro = { fecha: diaISO(d.repartoEn), zonaId: d.zonaId, franja: franjaDe(d.repartoEn) };
        await m.col('metricas_diarias').updateOne(filtro,
          { $inc: { 'tiempoHub.sumMs': d.repartoEn - d.ingresoEn, 'tiempoHub.n': 1 }, $setOnInsert: { hubId: d.hubId } }, { upsert: true, session });
        return 'tiempo contado';
      });
      return r.duplicado ? 'duplicado ignorado' : r.resultado;
    },
  });
})().catch((e) => { console.error(e); process.exit(1); });
