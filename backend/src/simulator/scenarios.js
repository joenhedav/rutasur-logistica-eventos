// Escenarios a pedido. Los pasos se definen en SEGUNDOS REALES para que la demo sea ágil; el tiempo
// simulado de cada evento se deriva con el factor de aceleración (los reintentos se comprimen en estos guiones).
const { H, DAY, ZONAS, dayStart } = require('../shared/rules');
const T = require('../shared/config').topics;
const { item } = require('../shared/planner');

function guion({ shipmentId, zi, ahora, accel, venceMs, pasos }) {
  const zona = ZONAS[zi];
  const repartidorId = 'R' + String(zi * 20 + 1).padStart(3, '0');
  const venceEn = new Date(venceMs).toISOString();
  const mk = (tipo, simAt, extra, secuencia, conRep) => ({
    eventId: `${shipmentId}-${secuencia}`, shipmentId, tipo, ocurridoEn: new Date(simAt).toISOString(), secuencia,
    hubId: zona.hub, zonaId: zona.id, repartidorId: conRep ? repartidorId : null, version: 1, venceEn, ...extra,
  });
  const items = [item(T.PROMESAS, { ...mk('PROMESA', ahora, {}, 0, false), eventId: `${shipmentId}-P` }, ahora)];
  pasos.forEach(([tipo, seg, extra = {}], i) => {
    const simAt = ahora + seg * 1000 * accel;
    items.push(item(T.ESTADOS, mk(tipo, simAt, extra, i + 1, tipo !== 'CREADO' && tipo !== 'INGRESADO_HUB'), simAt));
  });
  return items;
}

const I = (n, x = {}) => ({ intento: n, ...x });
const falla = (n) => I(n, { motivo: 'AUSENTE' });

// nombre -> (ctx) => { zi, vence, pasos }
const DEF = {
  feliz: ({ ahora }) => ({ zi: 0, vence: ahora + 2 * DAY, pasos: [['CREADO', 0], ['INGRESADO_HUB', 3], ['CLASIFICADO', 6, I(1)], ['EN_REPARTO', 9, I(1)], ['ENTREGADO', 13, I(1)]] }),
  ausente: ({ ahora }) => ({ zi: 3, vence: ahora + 3 * DAY, pasos: [['CREADO', 0], ['INGRESADO_HUB', 3], ['CLASIFICADO', 6, I(1)], ['EN_REPARTO', 9, I(1)], ['ENTREGA_FALLIDA', 13, falla(1)], ['CLASIFICADO', 22, I(2)], ['EN_REPARTO', 26, I(2)], ['ENTREGADO', 30, I(2)] ] }),
  demorado: ({ ahora, horas }) => ({ zi: 2, vence: ahora + (horas || 5) * H, pasos: [['CREADO', 0], ['INGRESADO_HUB', 3]] }),
  tercerFallo: ({ ahora }) => ({ zi: 4, vence: ahora + 3 * DAY, pasos: [['CREADO', 0], ['INGRESADO_HUB', 3], ['CLASIFICADO', 6, I(1)], ['EN_REPARTO', 9, I(1)], ['ENTREGA_FALLIDA', 13, falla(1)], ['CLASIFICADO', 22, I(2)], ['EN_REPARTO', 26, I(2)], ['ENTREGA_FALLIDA', 30, falla(2)], ['CLASIFICADO', 39, I(3)], ['EN_REPARTO', 43, I(3)], ['ENTREGA_FALLIDA', 47, falla(3)]] }),
  // Para validar las 4 reglas de SLA:
  sinIngreso: ({ ahora, horas }) => ({ zi: 1, vence: ahora + (horas || 11) * H, pasos: [['CREADO', 0]] }), // dispara SIN_INGRESO, SIN_SALIDA y VENCIDO
  falloLimite: ({ ahora, accel }) => ({ zi: 5, vence: dayStart(ahora + 13 * 1000 * accel) + DAY - 60e3, pasos: [['CREADO', 0], ['INGRESADO_HUB', 3], ['CLASIFICADO', 6, I(1)], ['EN_REPARTO', 9, I(1)], ['ENTREGA_FALLIDA', 13, falla(1)]] }), // dispara FALLO_DIA_LIMITE
  rescatado: ({ ahora }) => ({ zi: 0, vence: ahora + 5 * H, pasos: [['CREADO', 0], ['INGRESADO_HUB', 3], ['CLASIFICADO', 6, I(1)], ['EN_REPARTO', 40, I(1)], ['ENTREGADO', 45, I(1)]] }), // alerta SIN_SALIDA y luego cierre
};

function planScenario(nombre, { shipmentId, ahora, accel, horas }) {
  const f = DEF[nombre];
  if (!f) return null;
  const d = f({ ahora, accel, horas });
  return guion({ shipmentId, zi: d.zi, ahora, accel, venceMs: d.vence, pasos: d.pasos });
}

module.exports = { planScenario, nombres: Object.keys(DEF) };
