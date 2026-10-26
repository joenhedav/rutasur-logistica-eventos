'use client';
import Link from 'next/link';
import { useState } from 'react';
import { useStream, Vivo } from './useStream';
import { ETIQUETA, MOTIVO, fmt, fmtDia, franjaTxt, lineaDeTiempo, textoItem } from '@/lib/fmt';

export default function DetalleClient({ inicial, id }) {
  const [doc, setDoc] = useState(inicial);
  const vivo = useStream(`/api/stream?vista=envio&id=${encodeURIComponent(id)}`, (m) => { if (m.type === 'envio') setDoc(m.doc); });
  if (!doc) return (<><h1>{id}</h1><p className="vacio">Este envío todavía no existe. Cuando llegue su primer evento aparecerá acá automáticamente.</p><Vivo on={vivo} /></>);
  const abiertas = (doc.alertas || []).filter((a) => !a.cerradaEn);
  return (
    <>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline' }}>
        <div><h1>{doc.shipmentId}</h1><p className="sub"><span className={`etiqueta ${doc.estado}`}>{ETIQUETA[doc.estado] || 'Sin estado'}</span>{' '}
          <Link href={`/tracking/${doc.shipmentId}`}>Ver como lo ve el destinatario</Link></p></div>
        <Vivo on={vivo} />
      </div>
      <div className="cols">
        <section className="panel" aria-label="Línea de tiempo">
          <h2>Línea de tiempo</h2>
          <ul className="tl">
            {lineaDeTiempo(doc).map((it, i) => (
              <li key={i} className={it.kind}><div>{textoItem(it)}</div><div className="tipo">{fmt(it.t)}{it.kind === 'alertas' && it.cerradaEn ? ` · cerrada ${fmt(it.cerradaEn)}` : ''}</div></li>
            ))}
          </ul>
        </section>
        <section className="panel" aria-label="Datos del envío">
          <h2>Datos</h2>
          <dl className="kv">
            <dt>Hub / zona</dt><dd>{doc.hubId} / {doc.zonaId}</dd>
            <dt>Repartidor</dt><dd>{doc.repartidorId || 'Sin asignar'}</dd>
            <dt>Intentos</dt><dd>{doc.intentos}</dd>
            <dt>Promesa vence</dt><dd>{fmt(doc.promesa?.venceEn)}</dd>
            {doc.ultimoMotivo && <><dt>Último motivo</dt><dd>{MOTIVO[doc.ultimoMotivo]}</dd></>}
            {doc.destino && <><dt>Destino</dt><dd>{doc.destino === 'EN_CIRCUITO' ? 'En circuito' : doc.destino === 'RETENIDO' ? 'Retenido' : 'Devolución al comercio'}</dd></>}
            {doc.proximoIntento && <><dt>Próximo intento</dt><dd>{fmtDia(doc.proximoIntento.fecha)}, {franjaTxt(doc.proximoIntento.franja)}</dd></>}
            <dt>Alertas abiertas</dt><dd>{abiertas.length ? abiertas.map((a) => a.regla).join(', ') : 'Ninguna'}</dd>
            <dt>Avisos enviados</dt><dd>{(doc.notificaciones || []).length}</dd>
          </dl>
        </section>
      </div>
    </>
  );
}
