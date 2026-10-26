// Reglas de negocio puras (sin I/O): las comparten simulador, servicios y pruebas.
const H = 3600e3;
const DAY = 24 * H;

// 3 hubs, 2 zonas por hub. Tasa de fallo base por zona entre 6% y 30%.
const ZONAS = [
  { id: 'Z1', hub: 'H1', fallo: 0.06 },
  { id: 'Z2', hub: 'H1', fallo: 0.1 },
  { id: 'Z3', hub: 'H2', fallo: 0.15 },
  { id: 'Z4', hub: 'H2', fallo: 0.2 },
  { id: 'Z5', hub: 'H3', fallo: 0.25 },
  { id: 'Z6', hub: 'H3', fallo: 0.3 },
];
const MOTIVOS = ['AUSENTE', 'DIRECCION_INCORRECTA', 'RECHAZADO', 'ZONA_INACCESIBLE'];
const ESTADOS = ['CREADO', 'INGRESADO_HUB', 'CLASIFICADO', 'EN_REPARTO', 'ENTREGADO', 'ENTREGA_FALLIDA'];
const PRE_REPARTO = ['CREADO', 'INGRESADO_HUB', 'CLASIFICADO'];

const ms = (x) => (typeof x === 'number' ? x : new Date(x).getTime());
const dayStart = (x) => Math.floor(ms(x) / DAY) * DAY;
const diaISO = (x) => new Date(ms(x)).toISOString().slice(0, 10);
// Franja: antes de las 13:00 (hora simulada UTC) = MANANA, después = TARDE
const franjaDe = (x) => (new Date(ms(x)).getUTCHours() < 13 ? 'MANANA' : 'TARDE');
const opuesta = (f) => (f === 'MANANA' ? 'TARDE' : 'MANANA');

function siguienteDiaHabil(x) {
  let d = dayStart(x) + DAY;
  for (;;) {
    const wd = new Date(d).getUTCDay();
    if (wd !== 0 && wd !== 6) return d;
    d += DAY;
  }
}

// CU2: reglas de reprogramación
function decidirReprogramacion({ motivo, intento, ocurridoEn }) {
  if (intento >= 3) return { tipo: 'DEVOLUCION', razon: 'TERCER_FALLO' };
  switch (motivo) {
    case 'AUSENTE':
      return {
        tipo: 'REPROGRAMADO', razon: 'AUSENTE',
        proximoIntento: { fecha: diaISO(siguienteDiaHabil(ocurridoEn)), franja: opuesta(franjaDe(ocurridoEn)) },
      };
    case 'DIRECCION_INCORRECTA': return { tipo: 'RETENIDO', razon: 'DIRECCION_INCORRECTA' };
    case 'RECHAZADO': return { tipo: 'DEVOLUCION', razon: 'RECHAZADO' };
    default: // ZONA_INACCESIBLE: la especificación no define regla; decisión del trabajo: retener
      return { tipo: 'RETENIDO', razon: 'ZONA_INACCESIBLE' };
  }
}

// CU3: reglas de SLA. s = { estado, venceEn(ms), ultimoFalloEn(ms|null) }
const PRIORIDAD_N = { MEDIA: 1, ALTA: 2, CRITICA: 3 };
function reglasActivas(s, ahora) {
  if (!s || s.estado === 'ENTREGADO' || s.venceEn == null) return [];
  const resto = s.venceEn - ahora;
  const out = [];
  if (resto < 10 * H && s.estado === 'CREADO')
    out.push({ regla: 'SIN_INGRESO', prioridad: 'MEDIA', causa: 'Faltan menos de 10 h para el vencimiento y el envío no ingresó al hub' });
  if (resto < 4 * H && PRE_REPARTO.includes(s.estado))
    out.push({ regla: 'SIN_SALIDA', prioridad: 'ALTA', causa: 'Faltan menos de 4 h para el vencimiento y el envío no está en reparto' });
  if (s.estado === 'ENTREGA_FALLIDA' && s.ultimoFalloEn != null && dayStart(s.ultimoFalloEn) === dayStart(s.venceEn))
    out.push({ regla: 'FALLO_DIA_LIMITE', prioridad: 'ALTA', causa: 'Entrega fallida el mismo día en que vence la promesa' });
  if (ahora > s.venceEn)
    out.push({ regla: 'VENCIDO', prioridad: 'CRITICA', causa: 'Pasó el vencimiento de la promesa sin entrega' });
  return out;
}

module.exports = {
  H, DAY, ZONAS, MOTIVOS, ESTADOS, PRE_REPARTO, PRIORIDAD_N,
  ms, dayStart, diaISO, franjaDe, opuesta, siguienteDiaHabil, decidirReprogramacion, reglasActivas,
};
