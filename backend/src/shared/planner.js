// Planificador PURO y determinista del simulador: dada una semilla y un índice produce siempre el mismo
// envío con todos sus eventos. Lo usan el simulador y los scripts de verificación (el "recuento manual"
// se calcula así de forma independiente del pipeline).
const { H, DAY, ZONAS, dayStart, franjaDe, decidirReprogramacion } = require('./rules');
const T = require('./config').topics;

function xmur3(str) {
  let h = 1779033703 ^ str.length;
  for (let i = 0; i < str.length; i++) {
    h = Math.imul(h ^ str.charCodeAt(i), 3432918353);
    h = (h << 13) | (h >>> 19);
  }
  h = Math.imul(h ^ (h >>> 16), 2246822507);
  h = Math.imul(h ^ (h >>> 13), 3266489909);
  return (h ^ (h >>> 16)) >>> 0;
}
function mulberry32(a) {
  return function () {
    a |= 0; a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
const rngFor = (seed, index, salt = 0) => mulberry32(xmur3(`${seed}:${index}:${salt}`));

const item = (topic, ev, simAt, kind = 'ok') => ({ simAt, topic, key: ev.shipmentId, value: JSON.stringify(ev), kind, ev });

function elegirMotivo(x) {
  if (x < 0.6) return 'AUSENTE';
  if (x < 0.8) return 'DIRECCION_INCORRECTA';
  if (x < 0.95) return 'RECHAZADO';
  return 'ZONA_INACCESIBLE';
}

// Constructor de eventos de un envío (secuencia creciente, eventId determinista)
function builder({ shipmentId, zona, repartidorId, venceMs }) {
  let seq = 0;
  const items = [];
  const venceEn = new Date(venceMs).toISOString();
  return {
    items, venceEn,
    push(tipo, simAt, extra = {}, conRepartidor = false) {
      seq += 1;
      items.push(item(T.ESTADOS, {
        eventId: `${shipmentId}-${seq}`, shipmentId, tipo,
        ocurridoEn: new Date(simAt).toISOString(), secuencia: seq,
        hubId: zona.hub, zonaId: zona.id, repartidorId: conRepartidor ? repartidorId : null,
        version: 1, venceEn, ...extra,
      }, simAt));
    },
    promesa(simAt) {
      items.unshift(item(T.PROMESAS, {
        eventId: `${shipmentId}-P`, shipmentId, tipo: 'PROMESA', ocurridoEn: new Date(simAt).toISOString(),
        secuencia: 0, hubId: zona.hub, zonaId: zona.id, repartidorId: null, version: 1, venceEn,
      }, simAt));
    },
  };
}

// Vida completa de un envío (flujo continuo / ráfaga / lote)
function planShipment({ shipmentId, index, seed, creadoMs, demoraHub = 0.1 }) {
  const r = rngFor(seed, index, 1);
  const zi = Math.floor(r() * ZONAS.length);
  const zona = ZONAS[zi];
  const repartidorId = 'R' + String(zi * 20 + Math.floor(r() * 20) + 1).padStart(3, '0');
  const b = builder({ shipmentId, zona, repartidorId, venceMs: dayStart(creadoMs) + DAY + 20 * H });

  b.promesa(creadoMs);
  b.push('CREADO', creadoMs);
  let ingreso = creadoMs + H + Math.floor(r() * 3 * H);
  if (r() < demoraHub) ingreso += 22 * H + Math.floor(r() * 12 * H); // demora en hub => riesgo de SLA
  b.push('INGRESADO_HUB', ingreso);

  let t = ingreso + 20 * 60e3 + Math.floor(r() * 50 * 60e3);
  let franja = r() < 0.6 ? 'MANANA' : 'TARDE';
  let intento = 1;
  let proximoMs = 0;
  for (;;) {
    b.push('CLASIFICADO', t, { intento }, true);
    let salida;
    if (intento === 1) {
      salida = dayStart(t) + (franja === 'MANANA' ? 8 : 13) * H + Math.floor(r() * 90 * 60e3);
      if (salida < t + 30 * 60e3) salida += DAY;
    } else {
      salida = proximoMs + (franja === 'MANANA' ? 8 : 13) * H + Math.floor(r() * 60 * 60e3);
    }
    b.push('EN_REPARTO', salida, { intento }, true);
    const resultado = salida + 60 * 60e3 + Math.floor(r() * 90 * 60e3);
    // la mañana falla más que la tarde (insight para la analítica: ofrecer franja vespertina)
    const pFalla = zona.fallo * (franjaDe(resultado) === 'MANANA' ? 1.3 : 0.7);
    if (r() >= pFalla) { b.push('ENTREGADO', resultado, { intento }, true); break; }
    const motivo = elegirMotivo(r());
    b.push('ENTREGA_FALLIDA', resultado, { intento, motivo }, true);
    const dec = decidirReprogramacion({ motivo, intento, ocurridoEn: new Date(resultado).toISOString() });
    if (dec.tipo !== 'REPROGRAMADO') break;
    proximoMs = Date.parse(dec.proximoIntento.fecha + 'T00:00:00Z');
    franja = dec.proximoIntento.franja;
    intento += 1;
    t = proximoMs + 6 * H + Math.floor(r() * 30 * 60e3);
  }
  return { shipmentId, zonaId: zona.id, hubId: zona.hub, venceEn: b.venceEn, items: b.items };
}

const MALFORMADOS = [
  (ev) => '{"eventId":"' + ev.eventId + '","tipo":"CRE',                      // JSON truncado
  (ev) => JSON.stringify({ ...ev, tipo: 'EXPLOTADO' }),                         // tipo desconocido
  (ev) => JSON.stringify({ ...ev, ocurridoEn: 'ayer a la tarde' }),             // fecha inválida
  (ev) => JSON.stringify({ ...ev, eventId: undefined }),                        // sin eventId
  (ev) => JSON.stringify({ ...ev, secuencia: 'dos' }),                          // secuencia no numérica
  (ev) => JSON.stringify({ ...ev, tipo: 'ENTREGA_FALLIDA', intento: 1, motivo: undefined, repartidorId: 'R001' }), // fallo sin motivo
];

// Inyecta duplicados, eventos fuera de orden y malformados (probabilidades configurables, semilla fija)
function inyectar(plan, { seed, index, p }) {
  const r = rngFor(seed, index, 2);
  const est = plan.items.filter((i) => i.topic === T.ESTADOS);
  const stats = { dup: 0, ooo: 0, bad: 0 };
  for (let i = 0; i < est.length - 1; i++) {
    if (r() < p.ooo) { // intercambia los instantes de emisión de dos eventos consecutivos
      const a = est[i].simAt; est[i].simAt = est[i + 1].simAt; est[i + 1].simAt = a;
      stats.ooo++; i++;
    }
  }
  const extra = [];
  for (const it of est) {
    if (r() < p.dup) { extra.push({ ...it, kind: 'dup', simAt: it.simAt + 60e3 + Math.floor(r() * 540e3) }); stats.dup++; }
    if (r() < p.bad) {
      const v = Math.floor(r() * MALFORMADOS.length);
      extra.push({ simAt: it.simAt + 1000, topic: T.ESTADOS, key: it.key, value: MALFORMADOS[v](it.ev), kind: 'bad', ev: null });
      stats.bad++;
    }
  }
  plan.items.push(...extra);
  plan.items.sort((x, y) => x.simAt - y.simAt);
  plan.stats = stats;
  return plan;
}

// Lote reproducible: N envíos repartidos en 3 días simulados, mensajes ordenados por tiempo simulado
function planBatch({ seed, count, baseMs, p }) {
  const plans = [];
  for (let i = 0; i < count; i++) {
    const creadoMs = baseMs + Math.floor((i * 3 * DAY) / count);
    const plan = planShipment({ shipmentId: `RSB-${seed}-${String(i).padStart(5, '0')}`, index: i, seed, creadoMs, demoraHub: p.demoraHub });
    plans.push(inyectar(plan, { seed, index: i, p }));
  }
  const items = plans.flatMap((pl) => pl.items).sort((a, b) => a.simAt - b.simAt);
  const stats = plans.reduce((s, pl) => ({ dup: s.dup + pl.stats.dup, ooo: s.ooo + pl.stats.ooo, bad: s.bad + pl.stats.bad }), { dup: 0, ooo: 0, bad: 0 });
  return { plans, items, stats };
}

module.exports = { rngFor, planShipment, inyectar, planBatch, item, builder };
