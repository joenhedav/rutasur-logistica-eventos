// Mide la latencia evento -> pantalla: publica un CREADO y espera a verlo llegar por el SSE de Next.js.
// Uso: node scripts/latencia.js [muestras]   (WEB_URL por defecto http://web:3000)
const { makeProducer, send } = require('../src/shared/kafka');
const WEB = process.env.WEB_URL || 'http://web:3000';

(async () => {
  const producer = await makeProducer('latencia');
  const n = Number(process.argv[2]) || 10;
  const res = [];
  for (let i = 0; i < n; i++) {
    const id = `LAT-${Date.now()}-${i}`;
    const ac = new AbortController();
    const r = await fetch(`${WEB}/api/stream?vista=envio&id=${id}`, { signal: ac.signal });
    const reader = r.body.getReader(); const dec = new TextDecoder();
    await reader.read(); // primer chunk = conexión establecida
    await new Promise((x) => setTimeout(x, 400));
    const t0 = Date.now();
    const ahora = new Date(Date.parse('2026-03-02T06:00:00Z') + 10 * 24 * 3600e3).toISOString();
    await send(producer, 'envios.estados', { eventId: `${id}-1`, shipmentId: id, tipo: 'CREADO', ocurridoEn: ahora, secuencia: 1, hubId: 'H1', zonaId: 'Z1', repartidorId: null, version: 1, venceEn: ahora });
    let ms = null;
    const limite = setTimeout(() => ac.abort(), 8000);
    try {
      for (;;) { const { value, done } = await reader.read(); if (done) break; if (dec.decode(value).includes(id)) { ms = Date.now() - t0; break; } }
    } catch {}
    clearTimeout(limite); ac.abort();
    res.push(ms);
    console.log(`muestra ${i + 1}: ${ms === null ? 'sin respuesta (8 s)' : ms + ' ms'}`);
  }
  const ok = res.filter((x) => x !== null).sort((a, b) => a - b);
  console.log(`\nmin ${ok[0]} ms | mediana ${ok[Math.floor(ok.length / 2)]} ms | máx ${ok[ok.length - 1]} ms`);
  const bien = ok.length === n && ok[ok.length - 1] < 2000;
  console.log(bien ? '✅ latencia < 2 s en todas las muestras' : '❌ alguna muestra superó 2 s o no llegó');
  await producer.disconnect(); process.exit(bien ? 0 : 1);
})().catch((e) => { console.error(e); process.exit(1); });
