// Huella determinista de las vistas. Uso: node scripts/snapshot.js guardar|comparar <archivo>
const fs = require('fs');
const crypto = require('crypto');
const { connect } = require('../src/shared/mongo');

const COLS = { envios: 'shipmentId', alertas: 'shipmentId', notificaciones: 'eventId' };
const estable = (v) => JSON.stringify(v, (k, x) => (k === '_id' ? undefined : x));

async function huella() {
  const m = await connect('snapshot');
  const out = {};
  for (const [c, k] of Object.entries(COLS)) {
    const docs = await m.col(c).find({}).sort({ [k]: 1, regla: 1 }).toArray();
    out[c] = { n: docs.length, hash: crypto.createHash('sha256').update(estable(docs)).digest('hex'), docs: Object.fromEntries(docs.map((d) => [d[k] + (d.regla ? ':' + d.regla : ''), crypto.createHash('md5').update(estable(d)).digest('hex')])) };
  }
  await m.client.close();
  return out;
}

(async () => {
  const [cmd, file] = process.argv.slice(2);
  const h = await huella();
  if (cmd === 'guardar') {
    fs.writeFileSync(file, JSON.stringify(h));
    console.log('Huella guardada:', Object.entries(h).map(([c, v]) => `${c}=${v.n} docs (${v.hash.slice(0, 10)})`).join(' | '));
  } else if (cmd === 'comparar') {
    const antes = JSON.parse(fs.readFileSync(file));
    let ok = true;
    for (const c of Object.keys(COLS)) {
      const iguales = antes[c].hash === h[c].hash;
      console.log(`${iguales ? '✅' : '❌'} ${c}: antes ${antes[c].n} docs, después ${h[c].n} docs`);
      if (!iguales) {
        ok = false;
        const dif = Object.keys({ ...antes[c].docs, ...h[c].docs }).filter((k) => antes[c].docs[k] !== h[c].docs[k]).slice(0, 5);
        console.log('   primeras diferencias:', dif.join(', '));
      }
    }
    process.exit(ok ? 0 : 1);
  } else { console.log('uso: guardar|comparar <archivo>'); process.exit(2); }
})().catch((e) => { console.error(e); process.exit(1); });
