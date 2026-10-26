import { getDb } from '@/lib/mongo';
import { publico, resumenDoc, feedDe } from '@/lib/shape';
export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

// Server-Sent Events alimentados por los change streams de envios y alertas (la UI nunca toca Kafka).
// vista=torre (todo, resumido) | envio&id=X (documento completo) | publico&id=X (sin datos internos)
export async function GET(req) {
  const q = new URL(req.url).searchParams;
  const vista = q.get('vista') || 'torre';
  const id = q.get('id');
  if (vista !== 'torre' && !id) return new Response('falta id', { status: 400 });
  const db = await getDb();
  const enc = new TextEncoder();
  const cierres = [];
  let cerrado = false;
  const cerrar = () => {
    if (cerrado) return;
    cerrado = true;
    cierres.forEach((f) => { try { f(); } catch {} });
  };

  const stream = new ReadableStream({
    start(controller) {
      const raw = (s) => { if (!cerrado) { try { controller.enqueue(enc.encode(s)); } catch { cerrar(); } } };
      const send = (o) => raw(`data: ${JSON.stringify(o)}\n\n`);
      raw('retry: 2000\n\n');
      send({ type: 'open' });

      const filtro = id ? [{ $match: { 'fullDocument.shipmentId': id } }] : [];
      const opts = { fullDocument: 'updateLookup' };
      const cs1 = db.collection('envios').watch(filtro, opts);
      cs1.on('change', (ch) => {
        const doc = ch.fullDocument;
        if (!doc) return;
        if (vista === 'publico') return send({ type: 'envio', doc: publico(doc) });
        if (vista === 'envio') return send({ type: 'envio', doc });
        const claves = ch.operationType === 'insert' ? ['historial'] : Object.keys(ch.updateDescription?.updatedFields || {});
        send({ type: 'envio', doc: resumenDoc(doc), feed: feedDe(doc, claves) });
      });
      cs1.on('error', cerrar);
      cierres.push(() => cs1.close().catch(() => {}));

      if (vista !== 'publico') {
        const cs2 = db.collection('alertas').watch(filtro, opts);
        cs2.on('change', (ch) => { if (ch.fullDocument) send({ type: 'alerta', alerta: ch.fullDocument }); });
        cs2.on('error', cerrar);
        cierres.push(() => cs2.close().catch(() => {}));
      }
      const ping = setInterval(() => raw(': ping\n\n'), 15000);
      cierres.push(() => clearInterval(ping));
      cierres.push(() => { try { controller.close(); } catch {} });
      req.signal.addEventListener('abort', cerrar);
    },
    cancel: cerrar,
  });
  return new Response(stream, {
    headers: { 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-cache, no-transform', Connection: 'keep-alive', 'X-Accel-Buffering': 'no' },
  });
}
