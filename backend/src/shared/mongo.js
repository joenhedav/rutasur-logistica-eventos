const { MongoClient } = require('mongodb');
const cfg = require('./config');
const log = require('./log')('mongo');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Espera a que MongoDB tenga un primario (replica set de un nodo) antes de seguir.
async function connect(service) {
  for (let i = 0; ; i++) {
    const client = new MongoClient(cfg.mongoUrl, { appName: service, serverSelectionTimeoutMS: 3000 });
    try {
      await client.connect();
      const h = await client.db('admin').command({ hello: 1 });
      if (!h.isWritablePrimary) throw new Error('sin primario todavía');
      const db = client.db(cfg.dbName);
      return { client, db, col: (n) => db.collection(n) };
    } catch (e) {
      await client.close().catch(() => {});
      if (i > 90) throw e;
      log('esperando MongoDB', { error: e.message });
      await sleep(2000);
    }
  }
}

async function ensureIndexes(db) {
  const c = (n) => db.collection(n);
  await Promise.all([
    c('envios').createIndex({ shipmentId: 1 }, { unique: true }),
    c('envios').createIndex({ estado: 1, zonaId: 1 }),
    c('envios').createIndex({ 'promesa.venceEn': 1 }),
    c('envios').createIndex({ ultimoEventoEn: -1 }),
    c('alertas').createIndex({ estado: 1, prioridadN: -1 }),
    c('alertas').createIndex({ shipmentId: 1 }),
    c('alertas').createIndex({ shipmentId: 1, regla: 1 }, { unique: true }),
    c('notificaciones').createIndex({ shipmentId: 1 }),
    c('notificaciones').createIndex({ eventId: 1 }, { unique: true }),
    c('metricas_diarias').createIndex({ fecha: 1, zonaId: 1, franja: 1 }, { unique: true }),
    c('eventos_procesados').createIndex({ consumidor: 1, eventId: 1 }, { unique: true }),
    c('eventos_procesados').createIndex({ creadoEn: 1 }, { expireAfterSeconds: 7 * 24 * 3600 }), // TTL 7 días
    c('sla_estado').createIndex({ shipmentId: 1 }, { unique: true }),
    c('tiempo_hub_estado').createIndex({ shipmentId: 1 }, { unique: true }),
  ]);
}

// Idempotencia: marca el evento como procesado y aplica el efecto en UNA transacción.
// Si el proceso cae a mitad, no queda marca sin efecto ni efecto sin marca.
async function applyOnce(m, consumidor, ev, fn) {
  for (let intento = 0; ; intento++) {
    const session = m.client.startSession();
    try {
      let out = { duplicado: false };
      await session.withTransaction(async () => {
        out = { duplicado: false };
        const r = await m.col('eventos_procesados').updateOne(
          { consumidor, eventId: ev.eventId },
          { $setOnInsert: { consumidor, eventId: ev.eventId, shipmentId: ev.shipmentId, creadoEn: new Date() } },
          { upsert: true, session },
        );
        if (r.upsertedCount === 0) { out.duplicado = true; return; }
        out.resultado = await fn(session);
      });
      return out;
    } catch (e) {
      if (e.code === 11000 && intento < 4) continue; // carrera de upsert entre dos procesos
      throw e;
    } finally {
      await session.endSession();
    }
  }
}

module.exports = { connect, ensureIndexes, applyOnce, sleep };
