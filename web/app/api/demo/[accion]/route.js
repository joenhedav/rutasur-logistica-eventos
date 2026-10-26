// Panel de demo: reenvía la orden al plano de control del simulador (no es parte del flujo de negocio).
export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';
const SIM = process.env.SIMULATOR_URL || 'http://localhost:4000';
const RUTAS = {
  feliz: '/scenario/feliz', ausente: '/scenario/ausente', demorado: '/scenario/demorado', tercerFallo: '/scenario/tercerFallo',
  sinIngreso: '/scenario/sinIngreso', falloLimite: '/scenario/falloLimite', rescatado: '/scenario/rescatado',
  rafaga: '/rafaga', malformado: '/malformado', malformadoJson: '/malformado?tipo=json', pausa: '/pausa', reanudar: '/reanudar',
};
export async function POST(_req, { params }) {
  const ruta = RUTAS[params.accion];
  if (!ruta) return Response.json({ error: 'acción desconocida' }, { status: 404 });
  try { const r = await fetch(SIM + ruta, { method: 'POST', cache: 'no-store' }); return Response.json(await r.json(), { status: r.status }); }
  catch (e) { return Response.json({ error: 'No se pudo contactar al simulador: ' + e.message }, { status: 502 }); }
}
export async function GET(_req, { params }) {
  if (params.accion !== 'status') return Response.json({ error: 'usar POST' }, { status: 405 });
  try { const r = await fetch(SIM + '/status', { cache: 'no-store' }); return Response.json(await r.json()); }
  catch { return Response.json({ error: 'simulador no disponible' }, { status: 502 }); }
}
