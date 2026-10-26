'use client';
import Link from 'next/link';
import { useCallback, useRef, useState } from 'react';
import { useStream, Vivo } from './useStream';
import { ORDEN, ETIQUETA, REGLA, PRIORIDAD, fmt, textoItem } from '@/lib/fmt';

const clave = (a) => `${a.shipmentId}:${a.regla}`;
const ordenar = (l) => [...l].sort((a, b) => (b.prioridadN - a.prioridadN) || (new Date(b.abiertaEn) - new Date(a.abiertaEn))).slice(0, 40);

export default function TorreClient({ inicial }) {
  const [res, setRes] = useState(inicial.resumen);
  const [alertas, setAlertas] = useState(inicial.alertas);
  const [feed, setFeed] = useState(inicial.feed);
  const timer = useRef(null);

  // Los contadores se recalculan en el servidor (agregación indexada) con un pequeño debounce
  const refrescar = useCallback(() => {
    if (timer.current) return;
    timer.current = setTimeout(async () => {
      timer.current = null;
      try { setRes(await (await fetch('/api/resumen', { cache: 'no-store' })).json()); } catch {}
    }, 400);
  }, []);

  const vivo = useStream('/api/stream?vista=torre', (m) => {
    if (m.type === 'envio') { if (m.feed) setFeed((f) => [m.feed, ...f].slice(0, 15)); refrescar(); }
    if (m.type === 'alerta') {
      setAlertas((a) => {
        const sin = a.filter((x) => clave(x) !== clave(m.alerta));
        return m.alerta.estado === 'ABIERTA' ? ordenar([m.alerta, ...sin]) : sin;
      });
      refrescar();
    }
  });

  return (
    <>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline' }}>
        <div><h1>Torre de control</h1><p className="sub">Dónde está cada envío ahora y qué necesita atención.</p></div>
        <Vivo on={vivo} />
      </div>
      <section className="panel" aria-label="Envíos por estado">
        <div className="cinta">
          {ORDEN.map((k) => (
            <div key={k} className={`etapa ${k === 'ENTREGADO' ? 'fin' : k === 'ENTREGA_FALLIDA' ? 'mal' : ''}`}>
              <b>{res.estados[k] ?? 0}</b><span>{ETIQUETA[k]}</span>
            </div>
          ))}
        </div>
        <div className="chips">
          <span className="chip">Retenidos: <b>{res.destinos.RETENIDO}</b></span>
          <span className="chip">A devolver al comercio: <b>{res.destinos.DEVOLUCION}</b></span>
          <span className="chip">Alertas críticas: <b>{res.alertas.CRITICA || 0}</b></span>
          <span className="chip">Altas: <b>{res.alertas.ALTA || 0}</b></span>
          <span className="chip">Medias: <b>{res.alertas.MEDIA || 0}</b></span>
        </div>
      </section>
      <div className="cols">
        <section className="panel" aria-label="Cola de alertas">
          <h2>Alertas de SLA abiertas</h2>
          {alertas.length === 0 ? <p className="vacio">No hay alertas abiertas. Si hay envíos en curso, ninguno está en riesgo.</p> : (
            <ul className="lista">
              {alertas.map((a) => (
                <li key={clave(a)} className={a.prioridad}>
                  <span className={`pri ${a.prioridad}`}>{PRIORIDAD[a.prioridad]}</span> · <b>{REGLA[a.regla] || a.regla}</b>{' '}
                  <Link href={`/envio/${a.shipmentId}`}>{a.shipmentId}</Link>
                  <div className="meta">{a.causa}. Zona {a.zonaId} · desde {fmt(a.abiertaEn)}{a.venceEn ? ` · vence ${fmt(a.venceEn)}` : ''}</div>
                </li>
              ))}
            </ul>
          )}
        </section>
        <section className="panel" aria-label="Últimos eventos">
          <h2>Últimos eventos</h2>
          {feed.length === 0 ? <p className="vacio">Todavía no llegaron eventos. Arrancá el flujo desde el Panel de demo.</p> : (
            <ul className="lista">
              {feed.map((f, i) => (
                <li key={`${f.eventId || i}-${i}`}>
                  <Link href={`/envio/${f.shipmentId}`}>{f.shipmentId}</Link> <span className="meta">· zona {f.zonaId} · {fmt(f.t)}</span>
                  <div>{textoItem(f)}</div>
                </li>
              ))}
            </ul>
          )}
        </section>
      </div>
    </>
  );
}
