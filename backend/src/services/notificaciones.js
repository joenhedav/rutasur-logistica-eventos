// CU2 (reacción 1): registra el aviso al destinatario ante fallos y alertas. No envía correos reales.
const { startConsumer } = require('../shared/consumer');
const { connect, ensureIndexes } = require('../shared/mongo');
const { send } = require('../shared/kafka');
const cfg = require('../shared/config');
const T = cfg.topics;
const G = 'notificaciones';

(async () => {
  const m = await connect(G);
  await ensureIndexes(m.db);
  await startConsumer({
    service: G, groupId: G, topics: cfg.groups[G],
    handler: async (ev, { topic, producer }) => {
      let n = null;
      if (topic === T.ESTADOS && ev.tipo === 'ENTREGA_FALLIDA') {
        n = { origen: 'ENTREGA_FALLIDA', mensaje: `No pudimos entregar tu envío (${ev.motivo}). Elegí cómo reprogramar.`,
          opciones: ['REPROGRAMAR_MANANA', 'REPROGRAMAR_TARDE', 'RETIRAR_EN_SUCURSAL'] };
      } else if (topic === T.ALERTAS && ev.tipo === 'ALERTA_ABIERTA' && ['ALTA', 'CRITICA'].includes(ev.prioridad) && ev.regla !== 'FALLO_DIA_LIMITE') {
        n = { origen: 'ALERTA_SLA', mensaje: `Tu envío podría demorarse más de lo prometido (${ev.regla}).`, opciones: [] };
      }
      if (!n) return 'ignorado';
      // Idempotencia: eventId determinista. Si el servicio cae después de publicar y antes de confirmar
      // el offset, el aviso se republica con el MISMO eventId y el proyector lo descarta.
      const out = {
        eventId: `notif-${ev.eventId}`, shipmentId: ev.shipmentId, tipo: 'NOTIFICACION', canal: 'EMAIL (simulado)',
        ...n, origenEventId: ev.eventId, ocurridoEn: ev.ocurridoEn, secuencia: ev.secuencia,
        hubId: ev.hubId, zonaId: ev.zonaId, repartidorId: null, version: 1,
      };
      await send(producer, T.NOTIFICACIONES, out);
      return 'aviso publicado';
    },
  });
})().catch((e) => { console.error(e); process.exit(1); });
