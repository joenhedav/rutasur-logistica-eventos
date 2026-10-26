// Reconstrucción de las vistas del proyector. REQUISITO: el servicio "proyector" debe estar detenido.
// Borra envios/alertas/notificaciones, limpia su idempotencia y rebobina sus offsets al inicio del log.
const { connect } = require('../src/shared/mongo');
const { makeKafka } = require('../src/shared/kafka');
const cfg = require('../src/shared/config');

(async () => {
  const m = await connect('rebuild');
  const admin = makeKafka('rebuild').admin(); await admin.connect();
  const grupo = 'proyector';
  for (let i = 0; ; i++) { // espera a que el grupo quede vacío
    const d = (await admin.describeGroups([grupo])).groups[0];
    if (d.members.length === 0) break;
    if (i > 40) { console.error('El grupo proyector sigue activo: ejecutá "docker compose stop proyector" primero.'); process.exit(1); }
    await new Promise((r) => setTimeout(r, 1500));
  }
  for (const c of ['envios', 'alertas', 'notificaciones']) await m.col(c).deleteMany({});
  const r = await m.col('eventos_procesados').deleteMany({ consumidor: grupo });
  for (const topic of cfg.groups[grupo]) await admin.resetOffsets({ groupId: grupo, topic, earliest: true });
  console.log(`Vistas borradas, ${r.deletedCount} marcas de idempotencia eliminadas y offsets de "${grupo}" rebobinados.`);
  await admin.disconnect(); await m.client.close();
})().catch((e) => { console.error(e); process.exit(1); });
