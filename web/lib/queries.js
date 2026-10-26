import { ORDEN } from './fmt';

export async function resumen(db) {
  const [e, d, a] = await Promise.all([
    db.collection('envios').aggregate([{ $match: { estado: { $exists: true } } }, { $group: { _id: '$estado', n: { $sum: 1 } } }]).toArray(),
    db.collection('envios').aggregate([{ $match: { destino: { $in: ['RETENIDO', 'DEVOLUCION'] } } }, { $group: { _id: '$destino', n: { $sum: 1 } } }]).toArray(),
    db.collection('alertas').aggregate([{ $match: { estado: 'ABIERTA' } }, { $group: { _id: '$prioridad', n: { $sum: 1 } } }]).toArray(),
  ]);
  const estados = Object.fromEntries(ORDEN.map((k) => [k, 0]));
  e.forEach((x) => { estados[x._id] = x.n; });
  return {
    estados,
    destinos: { RETENIDO: d.find((x) => x._id === 'RETENIDO')?.n || 0, DEVOLUCION: d.find((x) => x._id === 'DEVOLUCION')?.n || 0 },
    alertas: Object.fromEntries(a.map((x) => [x._id, x.n])),
  };
}
export const alertasAbiertas = (db, n = 40) =>
  db.collection('alertas').find({ estado: 'ABIERTA' }).sort({ prioridadN: -1, abiertaEn: -1 }).limit(n).toArray();

export async function ultimos(db, n = 12) {
  const docs = await db.collection('envios').find({ estado: { $exists: true } }).sort({ ultimoEventoEn: -1 }).limit(n).toArray();
  return docs.map((d) => { const h = d.historial[d.historial.length - 1]; return { shipmentId: d.shipmentId, zonaId: d.zonaId, kind: 'historial', ...h, t: h.ocurridoEn }; });
}
