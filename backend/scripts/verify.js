// Verificación automática de los criterios de aceptación.
// Uso:  node scripts/verify.js lote [cantidad]   -> ciclo de vida, entrega fallida y analítica (sobre una corrida con semilla fija)
//       node scripts/verify.js hubtime [cantidad]-> reprocesamiento: cuarto indicador
//       node scripts/verify.js sla               -> las 4 reglas de SLA, cierre y no repetición (tarda ~6 min con SIM_ACCEL=120)
// El "recuento manual" se calcula REGENERANDO la corrida con el planificador determinista (misma semilla),
// es decir, sin usar ninguno de los servicios que se están validando.
const cfg = require('../src/shared/config');
const T = cfg.topics;
const { connect } = require('../src/shared/mongo');
const { makeKafka } = require('../src/shared/kafka');
const { planBatch } = require('../src/shared/planner');
const { lags, esperarLag0 } = require('./lag');
const { leerTopic } = require('./dlq');

const SIM = process.env.SIM_URL || 'http://localhost:4000';
let fallos = 0;
const chk = (nombre, ok, detalle = '') => { if (!ok) fallos++; console.log(`${ok ? '✅' : '❌'} ${nombre}${detalle ? '  — ' + detalle : ''}`); };
const post = async (p) => (await fetch(SIM + p, { method: 'POST' })).json();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const GRUPOS_BASE = ['proyector', 'notificaciones', 'reprogramacion', 'analitica'];

async function esperarEstable(admin, grupos) {
  for (let i = 0; i < 3; i++) { // lag 0 sostenido (el monitor puede seguir emitiendo alertas por el paso del tiempo)
    if (!(await esperarLag0(admin, grupos, 240))) return false;
    await sleep(2500);
  }
  return true;
}
const franja = (iso) => (new Date(iso).getUTCHours() < 13 ? 'MANANA' : 'TARDE');
function siguienteHabil(iso) {
  const d = new Date(iso); d.setUTCHours(0, 0, 0, 0);
  do d.setUTCDate(d.getUTCDate() + 1); while ([0, 6].includes(d.getUTCDay()));
  return d.toISOString().slice(0, 10);
}

async function lote(count) {
  const m = await connect('verify');
  const admin = makeKafka('verify').admin(); await admin.connect();
  const previos = await m.col('envios').countDocuments();
  if (previos > 0) { console.error(`La base ya tiene ${previos} envíos. Para una corrida limpia: docker compose down -v && AUTOSTART=false docker compose up -d --build`); process.exit(2); }
  const st = await (await fetch(SIM + '/status')).json();
  if (!st.pausado) console.warn('⚠️  El flujo continuo está activo; iniciá con AUTOSTART=false para una comparación exacta.');

  const esperado = planBatch({ seed: cfg.seed, count, baseMs: Date.parse(cfg.simStart), p: cfg.inject });
  console.log(`\nLote: ${count} envíos, semilla ${cfg.seed}, ${esperado.items.length} mensajes (duplicados ${esperado.stats.dup}, fuera de orden ${esperado.stats.ooo}, malformados ${esperado.stats.bad})`);
  const r = await post(`/batch?count=${count}&seed=${cfg.seed}`);
  console.log('Simulador publicó:', JSON.stringify(r));
  console.log('Esperando que los consumidores drenen el log...');
  chk('Los consumidores alcanzan lag 0', await esperarEstable(admin, GRUPOS_BASE), JSON.stringify((await lags(admin, GRUPOS_BASE)).map((x) => [x.grupo, x.lag])));

  const ok = (pl) => pl.items.filter((i) => i.kind === 'ok' && i.topic === T.ESTADOS).map((i) => i.ev);

  console.log('\n── Ciclo de vida ──');
  const docs = new Map((await m.col('envios').find({}).toArray()).map((d) => [d.shipmentId, d]));
  chk('Cada envío simulado aparece en la vista envios', docs.size === count, `${docs.size}/${count}`);
  let mal = 0, regresion = 0, repetidos = 0, ajenos = 0;
  for (const pl of esperado.plans) {
    const evs = ok(pl); const d = docs.get(pl.shipmentId);
    const ult = evs[evs.length - 1];
    if (!d || d.estado !== ult.tipo || d.secuenciaAplicada !== ult.secuencia) { mal++; continue; }
    const ids = d.historial.map((h) => h.eventId);
    if (new Set(ids).size !== ids.length) repetidos++;
    if (d.historial.some((h, i) => i && h.secuencia <= d.historial[i - 1].secuencia)) regresion++;
    const validos = new Set(evs.map((e) => e.eventId));
    if (ids.some((x) => !validos.has(x))) ajenos++;
  }
  chk('Estado final = último evento por secuencia (torre y tracking leen este estado)', mal === 0, `${mal} envíos con estado incorrecto`);
  chk(`Duplicados (${esperado.stats.dup} inyectados) no repiten entradas en el historial`, repetidos === 0, `${repetidos} historiales con repetidos`);
  chk(`Eventos fuera de orden (${esperado.stats.ooo} inyectados) no hacen retroceder el estado`, regresion === 0, `${regresion} historiales con retroceso`);
  chk('El historial solo contiene eventos válidos', ajenos === 0);
  const dlq = await leerTopic(T.DLQ);
  const origenes = (g) => new Set(dlq.filter((x) => x.value.consumidor === g).map((x) => `${x.value.origen.topic}/${x.value.origen.partition}/${x.value.origen.offset}`)).size;
  chk(`Eventos malformados (${esperado.stats.bad}) terminan en la DLQ`, origenes('proyector') === esperado.stats.bad, `proyector envió ${origenes('proyector')}; total de entradas DLQ ${dlq.length} (cada uno de los 5 consumidores de envios.estados reporta el suyo)`);
  chk('Toda entrada de la DLQ trae su motivo', dlq.every((x) => x.value.motivo && x.value.motivo.length > 0));

  console.log('\n── Entrega fallida ──');
  const fallas = esperado.plans.flatMap(ok).filter((e) => e.tipo === 'ENTREGA_FALLIDA');
  const notis = await m.col('notificaciones').aggregate([{ $match: { origen: 'ENTREGA_FALLIDA' } }, { $group: { _id: '$origenEventId', n: { $sum: 1 } } }]).toArray();
  const nMap = new Map(notis.map((x) => [x._id, x.n]));
  chk(`Cada ENTREGA_FALLIDA (${fallas.length}) genera exactamente 1 notificación`, notis.length === fallas.length && fallas.every((f) => nMap.get(f.eventId) === 1), `${notis.length} notificaciones de fallo`);
  let reprogMal = 0, reprogFalta = 0; const porMotivo = {};
  for (const f of fallas) {
    const d = docs.get(f.shipmentId) && (await m.col('envios').findOne({ shipmentId: f.shipmentId }));
    const rs = (d.reprogramaciones || []).filter((x) => x.origenEventId === f.eventId);
    if (rs.length !== 1) { reprogFalta++; continue; }
    let tipo, prox = null;
    if (f.intento >= 3) tipo = 'DEVOLUCION';
    else if (f.motivo === 'AUSENTE') { tipo = 'REPROGRAMADO'; prox = { fecha: siguienteHabil(f.ocurridoEn), franja: franja(f.ocurridoEn) === 'MANANA' ? 'TARDE' : 'MANANA' }; }
    else if (f.motivo === 'RECHAZADO') tipo = 'DEVOLUCION';
    else tipo = 'RETENIDO';
    const x = rs[0];
    const igual = x.tipo === tipo && (!prox || (x.proximoIntento && x.proximoIntento.fecha === prox.fecha && x.proximoIntento.franja === prox.franja));
    porMotivo[`${f.intento >= 3 ? 'TERCER_FALLO' : f.motivo}→${tipo}`] = (porMotivo[`${f.intento >= 3 ? 'TERCER_FALLO' : f.motivo}→${tipo}`] || 0) + 1;
    if (!igual) reprogMal++;
  }
  chk('Cada ENTREGA_FALLIDA genera exactamente 1 reprogramación', reprogFalta === 0, `${reprogFalta} sin exactamente una`);
  chk('Cada motivo aplica su regla (AUSENTE: día hábil siguiente y franja opuesta; DIRECCIÓN: retenido; RECHAZADO: devolución)', reprogMal === 0, `${reprogMal} incorrectas. Distribución: ${JSON.stringify(porMotivo)}`);
  const terceros = fallas.filter((f) => f.intento >= 3);
  const devueltos = await m.col('envios').countDocuments({ shipmentId: { $in: terceros.map((t) => t.shipmentId) }, destino: 'DEVOLUCION' });
  chk(`El tercer fallo marca el envío para devolución (${terceros.length} casos)`, devueltos === terceros.length, `${devueltos} marcados`);
  console.log('ℹ️  "Detener un servicio no afecta a los otros dos" se prueba a mano (ver VALIDACION.md, prueba 5).');

  console.log('\n── Analítica (recuento manual vs metricas_diarias) ──');
  const exp = new Map();
  const get = (k) => { if (!exp.has(k)) exp.set(k, { pt: 0, pe: 0, fallos: 0, AUSENTE: 0, DIRECCION_INCORRECTA: 0, RECHAZADO: 0, ZONA_INACCESIBLE: 0, se: 0, st: 0 }); return exp.get(k); };
  for (const e of esperado.plans.flatMap(ok)) {
    if (e.tipo !== 'ENTREGADO' && e.tipo !== 'ENTREGA_FALLIDA') continue;
    const x = get(`${e.ocurridoEn.slice(0, 10)}|${e.zonaId}|${franja(e.ocurridoEn)}`);
    if (e.intento === 1) { x.pt++; if (e.tipo === 'ENTREGADO') x.pe++; }
    if (e.tipo === 'ENTREGA_FALLIDA') { x.fallos++; x[e.motivo]++; }
    if (e.tipo === 'ENTREGADO') { x.se++; if (Date.parse(e.ocurridoEn) > Date.parse(e.venceEn)) x.st++; }
  }
  const real = new Map((await m.col('metricas_diarias').find({}).toArray()).map((d) => [`${d.fecha}|${d.zonaId}|${d.franja}`, d]));
  let difs = 0; const ejemplos = [];
  for (const k of new Set([...exp.keys(), ...real.keys()])) {
    const x = exp.get(k) || { pt: 0, pe: 0, fallos: 0, AUSENTE: 0, DIRECCION_INCORRECTA: 0, RECHAZADO: 0, ZONA_INACCESIBLE: 0, se: 0, st: 0 }; const d = real.get(k) || {};
    const v = [x.pt, d.primerIntento?.total || 0, x.pe, d.primerIntento?.entregados || 0, x.fallos, d.fallos || 0, x.se, d.sla?.entregados || 0, x.st, d.sla?.tarde || 0,
      x.AUSENTE, d.motivos?.AUSENTE || 0, x.DIRECCION_INCORRECTA, d.motivos?.DIRECCION_INCORRECTA || 0, x.RECHAZADO, d.motivos?.RECHAZADO || 0, x.ZONA_INACCESIBLE, d.motivos?.ZONA_INACCESIBLE || 0];
    for (let i = 0; i < v.length; i += 2) if (v[i] !== v[i + 1]) { difs++; if (ejemplos.length < 3) ejemplos.push(`${k} campo#${i / 2}: esperado ${v[i]} real ${v[i + 1]}`); }
  }
  chk(`Los 3 indicadores coinciden con el recuento manual (${exp.size} celdas fecha×zona×franja)`, difs === 0, difs ? ejemplos.join(' | ') : '0 diferencias');
  let mejor = null; const zf = {};
  for (const [k, x] of exp) { const [, z, f] = k.split('|'); const a = (zf[`${z}|${f}`] ||= { pt: 0, pe: 0 }); a.pt += x.pt; a.pe += x.pe; }
  for (const [k, a] of Object.entries(zf)) { const t = 1 - a.pe / a.pt; if (a.pt > 30 && (!mejor || t > mejor.t)) mejor = { k, t }; }
  if (mejor) console.log(`ℹ️  Peor combinación zona|franja en primer intento: ${mejor.k} con ${(mejor.t * 100).toFixed(1)}% de fallo (insight para el paso 7 de la demo)`);
  await admin.disconnect(); await m.client.close();
}

async function hubtime(count) {
  const m = await connect('verify');
  const exp = {};
  const esperado = planBatch({ seed: cfg.seed, count, baseMs: Date.parse(cfg.simStart), p: cfg.inject });
  for (const pl of esperado.plans) {
    const evs = pl.items.filter((i) => i.kind === 'ok' && i.topic === T.ESTADOS).map((i) => i.ev);
    const ing = evs.find((e) => e.tipo === 'INGRESADO_HUB'); const rep = evs.find((e) => e.tipo === 'EN_REPARTO');
    if (ing && rep) { const a = (exp[ing.hubId] ||= { sum: 0, n: 0 }); a.sum += Date.parse(rep.ocurridoEn) - Date.parse(ing.ocurridoEn); a.n++; }
  }
  const real = await m.col('metricas_diarias').aggregate([{ $group: { _id: '$hubId', sum: { $sum: '$tiempoHub.sumMs' }, n: { $sum: '$tiempoHub.n' } } }]).toArray();
  const r = Object.fromEntries(real.map((x) => [x._id, x]));
  for (const h of Object.keys(exp)) {
    const ok = r[h] && r[h].sum === exp[h].sum && r[h].n === exp[h].n;
    chk(`Hub ${h}: tiempo medio ingreso→reparto histórico completo`, ok, `${exp[h].n} envíos, promedio ${(exp[h].sum / exp[h].n / 3600e3).toFixed(2)} h` + (ok ? '' : ` (real: ${JSON.stringify(r[h])})`));
  }
  await m.client.close();
}

async function sla() {
  const m = await connect('verify');
  const st = await (await fetch(SIM + '/status')).json();
  const accel = st.accel;
  const e = {
    sinIngreso: (await post('/scenario/sinIngreso')).shipmentId,
    falloLimite: (await post('/scenario/falloLimite')).shipmentId,
    demorado: (await post('/scenario/demorado')).shipmentId,
    rescatado: (await post('/scenario/rescatado')).shipmentId,
  };
  console.log('Escenarios lanzados:', JSON.stringify(e));
  const esperadas = [
    [e.sinIngreso, 'SIN_INGRESO', 'MEDIA'], [e.sinIngreso, 'SIN_SALIDA', 'ALTA'], [e.sinIngreso, 'VENCIDO', 'CRITICA'],
    [e.falloLimite, 'FALLO_DIA_LIMITE', 'ALTA'], [e.demorado, 'SIN_SALIDA', 'ALTA'], [e.rescatado, 'SIN_SALIDA', 'ALTA'],
  ];
  const limite = Date.now() + (11 * 3600 / accel + 90) * 1000;
  console.log(`Esperando alertas (hasta ~${Math.round((limite - Date.now()) / 1000)} s con SIM_ACCEL=${accel})...`);
  let pendientes = esperadas;
  while (pendientes.length && Date.now() < limite) {
    const ahora = await m.col('alertas').find({ shipmentId: { $in: Object.values(e) } }).toArray();
    pendientes = esperadas.filter(([s, r]) => !ahora.find((a) => a.shipmentId === s && a.regla === r));
    await sleep(2000);
  }
  const alertas = await m.col('alertas').find({ shipmentId: { $in: Object.values(e) } }).toArray();
  for (const [s, r, p] of esperadas) {
    const a = alertas.find((x) => x.shipmentId === s && x.regla === r);
    chk(`Regla ${r} sobre ${s} genera alerta ${p}`, !!a && a.prioridad === p, a ? `prioridad ${a.prioridad}` : 'no apareció');
  }
  const todas = await leerTopic(T.ALERTAS);
  const abiertas = todas.map((x) => x.value).filter((v) => v.tipo === 'ALERTA_ABIERTA' && Object.values(e).includes(v.shipmentId));
  const por = {};
  for (const v of abiertas) (por[`${v.shipmentId}|${v.regla}`] ||= new Set()).add(v.eventId);
  chk('Una misma regla no alerta dos veces sobre el mismo envío (1 eventId por envío+regla en alertas.sla)', Object.values(por).every((s) => s.size === 1));
  // cierre: esperar a que el envío rescatado llegue a ENTREGADO
  for (let i = 0; i < 60; i++) { const a = await m.col('alertas').findOne({ shipmentId: e.rescatado, regla: 'SIN_SALIDA' }); if (a && a.estado === 'CERRADA') break; await sleep(2000); }
  const cerr = await m.col('alertas').findOne({ shipmentId: e.rescatado, regla: 'SIN_SALIDA' });
  const env = await m.col('envios').findOne({ shipmentId: e.rescatado });
  chk('La alerta se cierra cuando el envío se entrega', !!cerr && cerr.estado === 'CERRADA' && env.estado === 'ENTREGADO', `estado alerta ${cerr?.estado}, envío ${env?.estado}`);
  chk('Un envío entregado a tiempo no queda con alertas posteriores (sin VENCIDO)', !alertas.find((a) => a.shipmentId === e.rescatado && a.regla === 'VENCIDO'));
  console.log('ℹ️  "Tras reiniciar el monitor no se pierden ni se repiten alertas" se prueba a mano (ver VALIDACION.md, prueba 4).');
  await m.client.close();
}

(async () => {
  const [suite, n] = process.argv.slice(2);
  if (suite === 'lote') await lote(Number(n) || 2000);
  else if (suite === 'hubtime') await hubtime(Number(n) || 2000);
  else if (suite === 'sla') await sla();
  else { console.log('uso: node scripts/verify.js lote [n] | hubtime [n] | sla'); process.exit(2); }
  console.log(fallos ? `\n❌ ${fallos} verificación(es) fallida(s)` : '\n🎉 Todas las verificaciones pasaron');
  process.exit(fallos ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
