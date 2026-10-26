# Cómo validar el trabajo contra los criterios de aceptación

Requisitos: Docker con Compose v2. Los comandos son para bash; en PowerShell definí variables con `$env:AUTOSTART="false"` o usá un archivo `.env` (ver `.env.example`).

## 0. Entorno (criterio: un solo comando en máquina limpia)

```bash
docker compose down -v                     # máquina limpia
docker compose up --build                  # un solo comando
```
Esperá ~1 minuto y abrí http://localhost:3000: los contadores de la torre deben moverse. `docker compose ps` debe mostrar kafka y mongo *healthy* y los 6 procesos *running*.

## 1. Validación automática por lote (ciclo de vida, entrega fallida y analítica)

Usa una corrida **reproducible** (semilla 42, 2000 envíos, con duplicados, fuera de orden y malformados inyectados) y compara contra un recuento independiente.

```bash
docker compose down -v
AUTOSTART=false docker compose up -d --build        # sin flujo continuo, base limpia
docker compose exec simulador node scripts/verify.js lote 2000
```

| Criterio | Qué comprueba el script |
|---|---|
| Un envío aparece con su estado correcto | los 2000 envíos existen y su estado/secuencia final = el último evento |
| Duplicado no repite entrada | historiales sin `eventId` repetidos (hay ~220 duplicados inyectados) |
| Fuera de orden no hace retroceder | secuencias estrictamente crecientes en cada historial (~160 inyectados) |
| Malformado termina en DLQ con motivo | cada malformado (~100) aparece en `envios.estados.dlq` y trae `motivo` |
| Un `ENTREGA_FALLIDA` genera 1 notificación y 1 reprogramación | cuenta exacta por `eventId` de origen |
| Cada motivo aplica su regla | AUSENTE → día hábil siguiente y franja opuesta; DIRECCIÓN → retenido; RECHAZADO → devolución (calculado de forma independiente) |
| Tercer fallo → devolución | todos los envíos con 3 fallos tienen `destino = DEVOLUCION` |
| Los 3 indicadores = recuento manual | compara todas las celdas fecha × zona × franja de `metricas_diarias` |

Salida esperada: todo con ✅ y `🎉 Todas las verificaciones pasaron`. Si la base no está limpia, el script te lo indica.

## 2. Reprocesamiento: cuarto indicador (criterio: se completa con histórico sin reiniciar los demás servicios)

Con los datos del paso 1 cargados:
```bash
docker compose ps                                        # anotá los "Up ... minutes" de los demás servicios
docker compose --profile replay up -d analitica-tiempo-hub
docker compose exec simulador node scripts/lag.js --json | grep -A3 tiempo-hub   # el lag baja a 0
docker compose exec simulador node scripts/verify.js hubtime 2000
docker compose ps                                        # los demás servicios NO se reiniciaron
```
Visualmente: `/analitica` pasa de "indicador no activo" a mostrar el tiempo medio por hub y por día con datos anteriores a la creación del grupo.

## 3. Riesgo de SLA: las cuatro reglas (≈ 6 min con SIM_ACCEL=120)

```bash
docker compose exec simulador node scripts/verify.js sla
```
Lanza cuatro escenarios y espera sus alertas: `SIN_INGRESO`→Media, `SIN_SALIDA`→Alta, `FALLO_DIA_LIMITE`→Alta, `VENCIDO`→Crítica; además comprueba que **una regla no alerta dos veces** (un solo `eventId` por envío+regla en `alertas.sla`) y que la alerta **se cierra** cuando el envío se entrega. Para acelerar: `SIM_ACCEL=600 docker compose up -d` (≈ 1 min).
Visual: en la torre de control las alertas entran a la cola ordenadas por prioridad.

## 4. Pruebas manuales (las que no se pueden automatizar de forma honesta)

**4.1 Reinicio del monitor: no se pierden ni se repiten alertas**
1. Panel de demo → *Sin ingreso al hub*. A los ~30 s aparece la alerta Media.
2. `docker compose restart monitor-sla`.
3. A los ~3,5 min aparece la Alta y a los ~5,5 min la Crítica, una sola vez cada una.
4. Comprobá:
```bash
docker compose logs monitor-sla | grep -c '"regla":"SIN_INGRESO"'    # 1 (no se re-emitió tras el reinicio)
docker compose exec mongo mongosh rutasur --quiet --eval 'db.alertas.aggregate([{$group:{_id:{s:"$shipmentId",r:"$regla"},n:{$sum:1}}},{$match:{n:{$gt:1}}}]).toArray()'   # []
```
Variante "no se pierden": `docker compose stop monitor-sla` justo antes de que venza una regla, esperá 1 min y `docker compose start monitor-sla`: la alerta sale al arrancar.

**4.2 Caída de un servicio (criterios "detener un servicio no afecta a los otros" y "recuperación sin duplicar")**
```bash
docker compose stop notificaciones
```
Panel de demo → *Ausente* (varias veces). Verificá que el tracking y la torre siguen actualizándose, que el detalle muestra reprogramación pero **no** aviso, y que crece el lag:
```bash
docker compose exec simulador node scripts/lag.js          # grupo notificaciones con lag > 0
docker compose start notificaciones
docker compose exec simulador node scripts/lag.js --esperar   # vuelve a 0
docker compose exec mongo mongosh rutasur --quiet --eval 'db.notificaciones.aggregate([{$group:{_id:"$origenEventId",n:{$sum:1}}},{$match:{n:{$gt:1}}}]).toArray()'   # []
```

**4.3 Evento malformado en vivo (con el flujo continuo andando)**
Panel de demo → *Evento malformado* y luego `docker compose exec simulador node scripts/dlq.js`. El resto del flujo no se detiene.

**4.4 Reconstrucción: borrar `envios` y reprocesar deja el mismo estado**
```bash
docker compose stop simulador monitor-sla                  # congela el log
docker compose exec analitica node scripts/lag.js --esperar
docker compose exec analitica node scripts/snapshot.js guardar /tmp/antes.json
docker compose stop proyector
docker compose exec analitica node scripts/rebuild.js      # borra envios/alertas/notificaciones y rebobina offsets
docker compose start proyector
docker compose exec analitica node scripts/lag.js --esperar
docker compose exec analitica node scripts/snapshot.js comparar /tmp/antes.json    # ✅ ✅ ✅
```

**4.5 Latencia < 2 s con flujo continuo** (`AUTOSTART=true`)
```bash
docker compose exec simulador env WEB_URL=http://web:3000 node scripts/latencia.js 10
```
Publica un evento y mide hasta verlo llegar por el SSE de la interfaz; exige < 2000 ms en las 10 muestras.

**4.6 Observabilidad y lag**
`docker compose logs proyector` → cada línea trae `eventId`, `partition` y `offset`. `docker compose exec simulador node scripts/lag.js` → lag por grupo. Alternativa nativa: `docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups`.

**4.7 Paralelismo/rebalanceo (opcional, suma en la defensa)**
`docker compose up -d --scale proyector=2` y observá en los logs cómo se reparten las 3 particiones.

## Resumen: criterio → prueba

| Criterio | Prueba |
|---|---|
| Envío visible en torre y tracking con su estado | 1 (automático) + Panel de demo → *Envío feliz* y seguirlo en `/tracking/<id>` |
| Duplicado / fuera de orden / malformado | 1 y 4.3 |
| 1 notificación + 1 reprogramación / reglas / tercer fallo | 1 |
| Detener un servicio no afecta a los otros | 4.2 |
| 4 reglas de SLA, sin repetir, cierre | 3 |
| Sin pérdidas ni repeticiones tras reiniciar el monitor | 4.1 |
| 3 indicadores = recuento manual | 1 |
| Cuarto indicador con histórico sin reiniciar | 2 |
| Borrar y reprocesar deja el mismo estado | 4.4 |
| Un solo comando / latencia < 2 s | 0 y 4.5 |

## Guion de demo
1. `docker compose up --build` → torre con contadores moviéndose. 2. *Envío feliz* + tracking público. 3. *Ausente*: detalle con aviso, reprogramación y analítica a la vez. 4. *Demorado*: alerta Alta en la cola. 5. `docker compose stop notificaciones`, lanzar *Ausente*, mostrar lag, `start`. 6. *Evento malformado* → DLQ. 7. `/analitica`: zona y franja con más fallos. 8. `docker compose --profile replay up -d analitica-tiempo-hub` → el gráfico se completa con histórico.
