'use client';
import React from 'react';
import { useEffect, useRef, useState } from 'react';

// Suscripción SSE. EventSource reconecta solo; "vivo" indica si hay conexión abierta.
export function useStream(url, onMsg) {
  const cb = useRef(onMsg);
  cb.current = onMsg;
  const [vivo, setVivo] = useState(false);
  useEffect(() => {
    const es = new EventSource(url);
    es.onopen = () => setVivo(true);
    es.onerror = () => setVivo(false);
    es.onmessage = (e) => { try { cb.current(JSON.parse(e.data)); } catch {} };
    return () => es.close();
  }, [url]);
  return vivo;
}
export const Vivo = ({ on }) => <span className={`vivo ${on ? 'on' : ''}`} role="status"><i />{on ? 'En vivo' : 'Reconectando…'}</span>;
