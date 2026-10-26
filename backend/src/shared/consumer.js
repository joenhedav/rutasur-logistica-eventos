const { makeKafka } = require('./kafka');
const { validate } = require('./schema');
const T = require('./config').topics;
const mkLog = require('./log');

// Consumidor con: confirmación MANUAL de offsets (después de aplicar el efecto), validación de esquema,
// DLQ para eventos inválidos y log con eventId/partición/offset. Semántica: al menos una vez + idempotencia.
async function startConsumer({ service, groupId, topics, fromBeginning = false, handler }) {
  const log = mkLog(service);
  const kafka = makeKafka(service);
  const producer = kafka.producer({ allowAutoTopicCreation: false });
  const consumer = kafka.consumer({ groupId, maxWaitTimeInMs: 100, sessionTimeout: 15000, heartbeatInterval: 3000 });
  await producer.connect();
  await consumer.connect();
  for (const topic of topics) await consumer.subscribe({ topic, fromBeginning });

  const aDlq = async (key, origen, motivo, raw, eventId) => {
    await producer.send({
      topic: T.DLQ,
      messages: [{ key: key || 'desconocido', value: JSON.stringify({
        eventId: `dlq-${groupId}-${origen.topic}-${origen.partition}-${origen.offset}`,
        shipmentId: key || 'desconocido', tipo: 'EVENTO_INVALIDO', consumidor: groupId,
        origen, motivo, eventIdOriginal: eventId || null, payload: raw, registradoEn: new Date().toISOString(),
      }) }],
    });
    log('evento inválido -> DLQ', { ...origen, motivo });
  };

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
      const origen = { topic, partition, offset: message.offset };
      const raw = message.value ? message.value.toString() : '';
      const key = message.key ? message.key.toString() : null;
      let ev = null;
      let errs;
      try { ev = JSON.parse(raw); errs = validate(topic, ev); } catch { errs = ['JSON no parseable']; }
      if (errs.length) {
        await aDlq(key, origen, errs.join('; '), raw, ev && ev.eventId);
      } else {
        const res = await handler(ev, { topic, partition, offset: message.offset, producer });
        log(res || 'procesado', { eventId: ev.eventId, tipo: ev.tipo, ...origen });
      }
      // Solo después de aplicar el efecto: un fallo antes de este punto reintenta el evento.
      await consumer.commitOffsets([{ topic, partition, offset: (BigInt(message.offset) + 1n).toString() }]);
    },
  });
  log('consumidor activo', { groupId, topics });

  const stop = async () => { await consumer.disconnect().catch(() => {}); await producer.disconnect().catch(() => {}); process.exit(0); };
  process.on('SIGTERM', stop);
  process.on('SIGINT', stop);
  return { consumer, producer };
}

module.exports = { startConsumer };
