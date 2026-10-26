import './globals.css';
import Link from 'next/link';

export const metadata = { title: 'RutaSur · Seguimiento de envíos', description: 'Logística orientada a eventos' };

export default function Layout({ children }) {
  return (
    <html lang="es">
      <body>
        <header className="barra">
          <div className="marca">Ruta<span>Sur</span></div>
          <nav aria-label="Principal">
            <Link href="/">Torre de control</Link>
            <Link href="/tracking">Tracking</Link>
            <Link href="/analitica">Analítica</Link>
            <Link href="/demo">Panel de demo</Link>
          </nav>
          <div className="reloj">Horas en tiempo simulado (UTC)</div>
        </header>
        <main>{children}</main>
      </body>
    </html>
  );
}
