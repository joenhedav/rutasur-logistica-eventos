'use client';
import Link from 'next/link';
import { useEffect, useState } from 'react';

const ACCIONES = [
  ['feliz', 'Envío feliz', 'Un envío recorre todos los estados sin incidentes (~13 s). Seguilo en el tracking público.'],
  ['ausente', 'Ausente', 'Falla por ausencia, se reprograma y se entrega en el segundo intento.'],
  ['demorado', 'Demorado', 'Queda detenido en el hub: aparece una alerta Alta de SLA en ~30 s y una Crítica a los ~2,5 min.'],
  ['tercerFallo', 'Tercer fallo', 'Falla tres veces y pasa a devolución al comercio.'],
  ['rafaga', 'Ráfaga de 500', '500 envíos en pocos segundos para ver acumulación y recuperación.'],
  ['malformado', 'Evento malformado', 'Publica un evento inválido: debe aparecer en la DLQ sin frenar al resto.'],
];
const AVANZADOS = [
  ['sinIngreso', 'Sin ingreso al hub', 'Dispara las reglas Media, Alta y Crítica (≈ 0,5 / 3,5 / 5,5 min).'],
  ['falloLimite', 'Fallo en el día límite', 'Entrega fallida el día que vence la promesa (Alta).'],
  ['rescatado', 'Alerta que se cierra', 'Alerta Alta que se cierra cuando el envío se entrega.'],
  ['malformadoJson', 'JSON roto', 'Publica un mensaje que ni siquiera es JSON.'],
];

export default function DemoPanel() {
  const [log, setLog] = useState([]);
  const [st, setSt] = useState(null);
  const [ocupado, setOcupado] = useState(false);
  const leer = async () => { try { setSt(await (await fetch('/api/demo/status', { cache: 'no-store' })).json()); } catch { setSt(null); } };
  useEffect(() => { leer(); const t = setInterval(leer, 3000); return () => clearInterval(t); }, []);

  async function disparar(a) {
    setOcupado(true);
    try {
      const r = await (await fetch(`/api/demo/${a}`, { method: 'POST' })).json();
      setLog((l) => [{ a, r, t: new Date().toLocaleTimeString() }, ...l].slice(0, 8));
    } finally { setOcupado(false); leer(); }
  }
  const Tarjeta = ([k, t, d]) => (
    <div key={k}><div><h3>{t}</h3><p className="meta" style={{ margin: 0 }}>{d}</p></div><button onClick={() => disparar(k)} disabled={ocupado}>Lanzar escenario</button></div>
  );
  return (
    <>
      <h1>Panel de demo</h1>
      <p className="sub">Disparadores del simulador. {st && !st.error ? <>Reloj simulado: <b>{st.relojSim.slice(0, 16).replace('T', ' ')}</b> · flujo continuo: <b>{st.pausado ? 'en pausa' : 'activo'}</b> · cola pendiente: {st.pendientesEnCola}</> : 'Simulador no disponible.'}</p>
      <div className="botones">{ACCIONES.map(Tarjeta)}</div>
      <div style={{ display: 'flex', gap: 10, margin: '16px 0' }}>
        <button className="sec" onClick={() => disparar('pausa')}>Pausar flujo continuo</button>
        <button className="sec" onClick={() => disparar('reanudar')}>Reanudar flujo continuo</button>
      </div>
      <h2 style={{ marginTop: 24 }}>Escenarios de validación</h2>
      <div className="botones">{AVANZADOS.map(Tarjeta)}</div>
      <section className="panel" style={{ marginTop: 24 }}>
        <h2>Resultado de las últimas acciones</h2>
        {log.length === 0 ? <p className="vacio">Todavía no lanzaste ningún escenario.</p> : log.map((x, i) => (
          <p key={i} style={{ margin: '6px 0' }}><span className="meta">{x.t}</span> · <b>{x.a}</b>{' '}
            {x.r.shipmentId ? <><Link href={`/envio/${x.r.shipmentId}`}>detalle</Link> · <Link href={`/tracking/${x.r.shipmentId}`}>tracking público</Link> · {x.r.shipmentId}</> : <span className="meta">{x.r.error || JSON.stringify(x.r).slice(0, 120)}</span>}</p>))}
      </section>
      <section className="panel" style={{ marginTop: 20 }}>
        <h2>Caídas y reprocesamiento (desde una terminal)</h2>
        <pre>{`docker compose stop notificaciones      # el tracking y las alertas siguen funcionando
docker compose exec simulador node scripts/lag.js   # el lag de "notificaciones" crece
docker compose start notificaciones     # procesa lo pendiente sin duplicar avisos
docker compose --profile replay up -d analitica-tiempo-hub   # cuarto indicador con histórico`}</pre>
      </section>
    </>
  );
}
