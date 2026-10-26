// Validación de esquema JSON en cada consumidor (decisión de diseño: JSON + validación, sin Schema Registry).
const { ESTADOS, MOTIVOS } = require('./rules');
const T = require('./config').topics;

const isStr = (v) => typeof v === 'string' && v.length > 0;
const isIso = (v) => typeof v === 'string' && !Number.isNaN(Date.parse(v));
const isInt = (v, min = 0) => Number.isInteger(v) && v >= min;

function comunes(ev) {
  const e = [];
  if (!isStr(ev.eventId)) e.push('eventId ausente o inválido');
  if (!isStr(ev.shipmentId)) e.push('shipmentId ausente o inválido');
  if (!isStr(ev.tipo)) e.push('tipo ausente o inválido');
  if (!isIso(ev.ocurridoEn)) e.push('ocurridoEn no es una fecha ISO');
  if (!isInt(ev.secuencia)) e.push('secuencia debe ser un entero >= 0');
  if (ev.version !== 1) e.push('version no soportada');
  if (!isStr(ev.hubId)) e.push('hubId ausente');
  if (!isStr(ev.zonaId)) e.push('zonaId ausente');
  return e;
}

const validadores = {
  [T.ESTADOS]: (ev) => {
    const e = comunes(ev);
    if (!ESTADOS.includes(ev.tipo)) e.push(`tipo de estado desconocido: ${ev.tipo}`);
    if (!isInt(ev.secuencia, 1)) e.push('secuencia debe ser >= 1');
    if (!isIso(ev.venceEn)) e.push('venceEn no es una fecha ISO');
    if (['CLASIFICADO', 'EN_REPARTO', 'ENTREGADO', 'ENTREGA_FALLIDA'].includes(ev.tipo)) {
      if (!isStr(ev.repartidorId)) e.push('repartidorId requerido');
      if (!isInt(ev.intento, 1)) e.push('intento requerido');
    }
    if (ev.tipo === 'ENTREGA_FALLIDA' && !MOTIVOS.includes(ev.motivo)) e.push('motivo de fallo inválido');
    return e;
  },
  [T.PROMESAS]: (ev) => {
    const e = comunes(ev);
    if (ev.tipo !== 'PROMESA') e.push('tipo debe ser PROMESA');
    if (!isIso(ev.venceEn)) e.push('venceEn no es una fecha ISO');
    return e;
  },
  [T.REPROGRAMACIONES]: (ev) => {
    const e = comunes(ev);
    if (!['REPROGRAMADO', 'RETENIDO', 'DEVOLUCION'].includes(ev.tipo)) e.push('tipo de reprogramación inválido');
    return e;
  },
  [T.ALERTAS]: (ev) => {
    const e = comunes(ev);
    if (!['ALERTA_ABIERTA', 'ALERTA_CERRADA'].includes(ev.tipo)) e.push('tipo de alerta inválido');
    if (ev.tipo === 'ALERTA_ABIERTA' && (!isStr(ev.regla) || !isStr(ev.prioridad))) e.push('regla/prioridad requeridas');
    return e;
  },
  [T.NOTIFICACIONES]: (ev) => {
    const e = comunes(ev);
    if (ev.tipo !== 'NOTIFICACION') e.push('tipo debe ser NOTIFICACION');
    return e;
  },
};

function validate(topic, ev) {
  if (!ev || typeof ev !== 'object' || Array.isArray(ev)) return ['el evento no es un objeto JSON'];
  const v = validadores[topic];
  return v ? v(ev) : [];
}

module.exports = { validate };
