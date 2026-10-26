// Muestra el contenido de envios.estados.dlq. Uso: node scripts/dlq.js [maxMensajes]
const { makeKafka } = require('../src/shared/kafka');
const T = require('../src/shared/config').topics;

async function leerTopic(topic, groupId = `lectura-${Date.now()}`) {
  const kafka = makeKafka('lector'); const admin = kafka.admin(); await admin.connect();
  const fin = (await admin.fetchTopicOffsets(topic)).reduce((s, p) => s + Number(p.offset) - Number(p.low), 0);
  await admin.disconnect();
  const msgs = [];
  if (!fin) return msgs;
  const c = kafka.consumer({ groupId }); await c.connect(); await c.subscribe({ topic, fromBeginning: true });
  await new Promise((resolve) => {
    c.run({ eachMessage: async ({ partition, message }) => {
      msgs.push({ partition, offset: message.offset, key: message.key && message.key.toString(), value: JSON.parse(message.value.toString()) });
      if (msgs.length >= fin) resolve();
    } });
    setTimeout(resolve, 20000);
  });
  await c.disconnect();
  return msgs;
}
module.exports = { leerTopic };

if (require.main === module) {
  leerTopic(T.DLQ).then((m) => {
    const max = Number(process.argv[2]) || 20;
    console.log(`DLQ: ${m.length} mensajes`);
    m.slice(-max).forEach((x) => console.log(JSON.stringify({ consumidor: x.value.consumidor, origen: x.value.origen, motivo: x.value.motivo, payload: String(x.value.payload).slice(0, 90) })));
  }).catch((e) => { console.error(e); process.exit(1); });
}
