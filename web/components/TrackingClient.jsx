'use client';
import { useState } from 'react';
import { useStream, Vivo } from './useStream';
import { fmt, fmtDia, franjaTxt } from '@/lib/fmt';

const PASOS = [['CREADO', 'Registrado'], ['INGRESADO_HUB', 'En el centro'], ['CLASIFICADO', 'Preparado'], ['EN_REPARTO', 'En camino'], ['ENTREGADO', 'Entregado']];

function mensaje(d) {
  if (d.destino === 'DEVOLUCION') return ['Vuelve al comercio', 'No pudimos entregarlo. Contactá al comercio para coordinar.'];
  if (d.destino === 'RETENIDO') return ['Necesitamos tu ayuda', 'Hay un problema con el destino. Contactá al comercio para corregir el dato y retomamos la entrega.'];
  if (d.estado === 'ENTREGADO') return ['Entregado', 'Tu paquete ya llegó.'];
  if (d.estado === 'ENTREGA_FALLIDA')
    return ['No pudimos entregarlo', d.proximoIntento ? `Volvemos a intentar el ${fmtDia(d.proximoIntento.fecha)}, en la ${franjaTxt(d.proximoIntento.franja)}.` : 'Estamos definiendo un nuevo intento.'];
  if (d.estado === 'EN_REPARTO') return ['Sale a entregarse hoy', 'El repartidor está en camino.'];
  return ['Lo estamos preparando', 'Te avisamos cuando salga a entregarse.'];
}

export default function TrackingClient({ inicial, id }) {
  const [d, setD] = useState(inicial);
  const vivo = useStream(`/api/stream?vista=publico&id=${encodeURIComponent(id)}`, (m) => { if (m.type === 'envio') setD(m.doc); });
  if (!d || !d.estado) return (<div className="pub panel"><h1>Envío {id}</h1><p className="vacio">No encontramos un envío con ese código. Revisalo e intentá de nuevo.</p></div>);
  const [titulo, detalle] = mensaje(d);
  const idx = PASOS.findIndex(([k]) => k === (d.estado === 'ENTREGA_FALLIDA' ? 'EN_REPARTO' : d.estado));
  return (
    <div className="pub panel">
      <div style={{ display: 'flex', justifyContent: 'space-between' }}><span className="meta">Envío {d.shipmentId}</span><Vivo on={vivo} /></div>
      <div className="grande">{titulo}</div>
      <p>{detalle}</p>
      <div className="pasos" aria-label="Progreso del envío">
        {PASOS.map(([k, t], i) => <div key={k} className={`paso ${i <= idx ? 'hecho' : ''}`}>{t}</div>)}
      </div>
      {d.estado !== 'ENTREGADO' && d.venceEn && <p className="meta">Fecha estimada de entrega: hasta el {fmt(d.venceEn)}</p>}
    </div>
  );
}
