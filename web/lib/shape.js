// Proyecciones del documento de envío según quién lo mira
export const publico = (d) => ({
  shipmentId: d.shipmentId, estado: d.estado || null, venceEn: d.promesa?.venceEn || null,
  destino: d.destino || null, proximoIntento: d.proximoIntento || null, intentos: d.intentos || 0,
});
export const resumenDoc = (d) => ({
  shipmentId: d.shipmentId, estado: d.estado || null, hubId: d.hubId, zonaId: d.zonaId, destino: d.destino || null, ultimoEventoEn: d.ultimoEventoEn,
});
export function feedDe(doc, claves) {
  for (const n of ['historial', 'reprogramaciones', 'notificaciones', 'alertas']) {
    if (claves.some((k) => k === n || k.startsWith(n + '.')) && doc[n]?.length) {
      const e = doc[n][doc[n].length - 1];
      return { shipmentId: doc.shipmentId, zonaId: doc.zonaId, kind: n, ...e, t: e.ocurridoEn };
    }
  }
  return null;
}
