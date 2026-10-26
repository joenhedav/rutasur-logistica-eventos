// CU2 (reacción 2): decide el próximo intento tras una entrega fallida.
const { startConsumer } = require('../shared/consumer');
const { send } = require('../shared/kafka');
const { decidirReprogramacion } = require('../shared/rules');
const cfg = require('../shared/config');
const G = 'reprogramacion';

(async () => {
  await startConsumer({
    service: G, groupId: G, topics: cfg.groups[G],
    handler: async (ev, { producer }) => {
      if (ev.tipo !== 'ENTREGA_FALLIDA') return 'ignorado';
      const d = decidirReprogramacion({ motivo: ev.motivo, intento: ev.intento, ocurridoEn: ev.ocurridoEn });
      await send(producer, cfg.topics.REPROGRAMACIONES, {
        eventId: `rep-${ev.eventId}`, // determinista => idempotente aguas abajo
        shipmentId: ev.shipmentId, tipo: d.tipo, motivo: ev.motivo, intento: ev.intento, razon: d.razon,
        proximoIntento: d.proximoIntento || null, origenEventId: ev.eventId,
        ocurridoEn: ev.ocurridoEn, secuencia: ev.secuencia, hubId: ev.hubId, zonaId: ev.zonaId, repartidorId: null, version: 1,
      });
      return `decisión ${d.tipo}`;
    },
  });
})().catch((e) => { console.error(e); process.exit(1); });
