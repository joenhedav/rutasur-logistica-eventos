export const ORDEN = ['CREADO', 'INGRESADO_HUB', 'CLASIFICADO', 'EN_REPARTO', 'ENTREGADO', 'ENTREGA_FALLIDA'];
export const ETIQUETA = { CREADO: 'Creado', INGRESADO_HUB: 'En el hub', CLASIFICADO: 'Clasificado', EN_REPARTO: 'En reparto', ENTREGADO: 'Entregado', ENTREGA_FALLIDA: 'Entrega fallida' };
export const MOTIVO = { AUSENTE: 'Ausente', DIRECCION_INCORRECTA: 'Dirección incorrecta', RECHAZADO: 'Rechazado', ZONA_INACCESIBLE: 'Zona inaccesible' };
export const REGLA = { SIN_INGRESO: 'Sin ingreso al hub', SIN_SALIDA: 'Sin salida a reparto', FALLO_DIA_LIMITE: 'Fallo en el día límite', VENCIDO: 'Promesa vencida' };
export const PRIORIDAD = { CRITICA: 'Crítica', ALTA: 'Alta', MEDIA: 'Media' };
const MES = ['ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic'];
const p2 = (n) => String(n).padStart(2, '0');

// Todas las horas de la interfaz son horas SIMULADAS (UTC)
export function fmt(d) {
  if (!d) return '—';
  const x = new Date(d);
  if (Number.isNaN(x.getTime())) return '—';
  return `${p2(x.getUTCDate())} ${MES[x.getUTCMonth()]} ${p2(x.getUTCHours())}:${p2(x.getUTCMinutes())}`;
}
export const fmtDia = (iso) => { const [, m, d] = String(iso).split('-'); return `${d} ${MES[Number(m) - 1]}`; };
export const franjaTxt = (f) => (f === 'MANANA' ? 'mañana' : 'tarde');

// Texto de una entrada del feed / línea de tiempo
export function textoItem(it) {
  switch (it.kind) {
    case 'historial':
      return ETIQUETA[it.tipo] + (it.motivo ? ` · ${MOTIVO[it.motivo] || it.motivo}` : '') + (it.intento > 1 ? ` (intento ${it.intento})` : '');
    case 'reprogramaciones':
      if (it.tipo === 'REPROGRAMADO') return `Reprogramado para el ${fmtDia(it.proximoIntento.fecha)}, franja ${franjaTxt(it.proximoIntento.franja)}`;
      if (it.tipo === 'RETENIDO') return 'Retenido hasta corregir el dato del destino';
      return 'Marcado para devolución al comercio';
    case 'notificaciones': return `Aviso al destinatario: ${it.mensaje}`;
    case 'alertas': return `Alerta ${PRIORIDAD[it.prioridad] || ''}: ${REGLA[it.regla] || it.regla}`;
    default: return it.kind;
  }
}

// Une historial, reprogramaciones, avisos y alertas de un envío en una sola línea de tiempo
export function lineaDeTiempo(doc) {
  const t = [];
  for (const h of doc.historial || []) t.push({ ...h, kind: 'historial', t: h.ocurridoEn });
  for (const r of doc.reprogramaciones || []) t.push({ ...r, kind: 'reprogramaciones', t: r.ocurridoEn });
  for (const n of doc.notificaciones || []) t.push({ ...n, kind: 'notificaciones', t: n.ocurridoEn });
  for (const a of doc.alertas || []) t.push({ ...a, kind: 'alertas', t: a.ocurridoEn });
  return t.sort((a, b) => new Date(a.t) - new Date(b.t));
}
