// Lag de cada grupo de consumidores. Uso: node scripts/lag.js [--esperar [segundos]] [--json]
const { makeKafka } = require('../src/shared/kafka');
const cfg = require('../src/shared/config');

async function lags(admin, grupos = Object.keys(cfg.groups)) {
  const out = [];
  for (const g of grupos) {
    let total = 0; const detalle = [];
    for (const topic of cfg.groups[g]) {
      const hw = await admin.fetchTopicOffsets(topic);
      const com = await admin.fetchOffsets({ groupId: g, topics: [topic] });
      for (const p of hw) {
        const c = com[0].partitions.find((x) => x.partition === p.partition);
        const off = c && Number(c.offset) >= 0 ? Number(c.offset) : Number(p.low);
        const lag = Number(p.offset) - off; total += lag;
        detalle.push({ topic, partition: p.partition, comprometido: off, fin: Number(p.offset), lag });
      }
    }
    out.push({ grupo: g, lag: total, detalle });
  }
  return out;
}

// Espera a que los grupos indicados (por defecto los que ya tienen offsets) lleguen a lag 0
const BASE = Object.keys(cfg.groups).filter((g) => g !== 'analitica-tiempo-hub'); // ese grupo solo existe tras activar el replay
async function esperarLag0(admin, grupos = BASE, timeoutS = 180) {
  const t0 = Date.now();
  for (;;) {
    const l = await lags(admin, grupos);
    if (l.every((x) => x.lag === 0)) return true;
    if (Date.now() - t0 > timeoutS * 1000) return false;
    await new Promise((r) => setTimeout(r, 1000));
  }
}

module.exports = { lags, esperarLag0 };

if (require.main === module) {
  (async () => {
    const admin = makeKafka('lag').admin(); await admin.connect();
    const args = process.argv.slice(2);
    if (args.includes('--esperar')) {
      const ok = await esperarLag0(admin, undefined, Number(args[args.indexOf('--esperar') + 1]) || 180);
      console.log(ok ? 'lag 0 en todos los grupos' : 'TIMEOUT: aún hay lag'); }
    const l = await lags(admin);
    if (args.includes('--json')) console.log(JSON.stringify(l, null, 2));
    else console.table(l.map((x) => ({ grupo: x.grupo, lag: x.lag, topics: cfg.groups[x.grupo].join(', ') })));
    await admin.disconnect();
  })().catch((e) => { console.error(e); process.exit(1); });
}
