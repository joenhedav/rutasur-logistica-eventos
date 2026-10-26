# RutaSur · logística orientada a eventos

Plataforma de seguimiento de envíos en tiempo real: simulador → **Kafka** → 5 servicios **Node.js** → **MongoDB** (vistas) → **Next.js**.
Todo corre en local con Docker Compose, sin dependencias externas.

```
docker compose up --build        # un solo comando; Next.js queda en http://localhost:3000
```

| Qué | Dónde |
|---|---|
| Torre de control (supervisor) | http://localhost:3000 |
| Detalle de un envío | http://localhost:3000/envio/RS-0000001 |
| Tracking público (destinatario) | http://localhost:3000/tracking |
| Analítica (gerencia) | http://localhost:3000/analitica |
| Panel de demo (escenarios) | http://localhost:3000/demo |
| API de control del simulador | http://localhost:4000/status |
| Kafka (desde el host) / MongoDB | `localhost:29092` / `localhost:27017` |

La **validación paso a paso** de los criterios de aceptación está en [`EJECUCION.md`](EJECUCION.md).

## Estructura

```
docker-compose.yml        Kafka (KRaft, 1 broker), creación de topics, MongoDB replica set, 6 procesos Node, Next.js
backend/
  src/shared/             config, esquema JSON, reglas puras, planificador determinista, consumidor idempotente
  src/simulator/          simulador (flujo continuo + escenarios + lote reproducible) y API HTTP de control
  src/services/           proyector · notificaciones · reprogramacion · monitor-sla · analitica · analitica-tiempo-hub
  scripts/                lag, verify, snapshot, rebuild, dlq, latencia
  test/                   pruebas unitarias (node --test)
web/                      Next.js 14 (App Router): 4 pantallas + panel de demo, SSE sobre change streams
```

## Cómo cumple cada caso de uso

| CU | Implementación |
|---|---|
| **CU1 ciclo de vida** | `proyector` (grupo `proyector`) consume `envios.estados` y mantiene `envios` con historial embebido. Descarta `secuencia <= secuenciaAplicada`; ignora `eventId` repetido (colección `eventos_procesados`, TTL 7 días); lo inválido va a `envios.estados.dlq` con su motivo. |
| **CU2 entrega fallida** | Tres grupos independientes sobre el mismo topic: `notificaciones` (publica aviso), `reprogramacion` (publica decisión) y `analitica` (incrementa `metricas_diarias`). El proyector refleja fallo, aviso y reprogramación en el documento del envío. |
| **CU3 riesgo de SLA** | `monitor-sla`: estado interno en memoria respaldado en `sla_estado`, cruza `envios.estados` + `envios.promesas`, evalúa por evento y cada segundo real (≈ 2 min simulados) con el **reloj simulado** de los eventos. Las 4 reglas, cierre al `ENTREGADO` y sin repetición. |
| **CU4 analítica** | `analitica` (3 indicadores, `$inc` atómico). El 4º indicador (tiempo medio ingreso→reparto por hub) lo calcula `analitica-tiempo-hub`, un grupo **nuevo** que lee desde el offset 0 (`docker compose --profile replay up -d analitica-tiempo-hub`). |

