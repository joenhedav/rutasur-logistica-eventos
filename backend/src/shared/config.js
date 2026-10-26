// Configuración central. Todo se puede sobreescribir con variables de entorno (ver docker-compose.yml).
const n = (v, d) => (v === undefined || v === '' ? d : Number(v));

const topics = {
  ESTADOS: 'envios.estados',
  PROMESAS: 'envios.promesas',
  REPROGRAMACIONES: 'envios.reprogramaciones',
  ALERTAS: 'alertas.sla',
  NOTIFICACIONES: 'notificaciones.enviadas',
  DLQ: 'envios.estados.dlq',
};

// Grupos de consumidores y topics que lee cada uno (catálogo de eventos de la especificación)
const groups = {
  proyector: [topics.ESTADOS, topics.REPROGRAMACIONES, topics.ALERTAS, topics.NOTIFICACIONES],
  notificaciones: [topics.ESTADOS, topics.ALERTAS],
  reprogramacion: [topics.ESTADOS],
  'monitor-sla': [topics.ESTADOS, topics.PROMESAS],
  analitica: [topics.ESTADOS],
  'analitica-tiempo-hub': [topics.ESTADOS],
};

module.exports = {
  topics,
  groups,
  brokers: (process.env.KAFKA_BROKERS || 'localhost:29092').split(','),
  mongoUrl: process.env.MONGO_URL || 'mongodb://localhost:27017/rutasur?directConnection=true',
  dbName: process.env.MONGO_DB || 'rutasur',
  accel: n(process.env.SIM_ACCEL, 120), // 120 = 1 hora simulada cada 30 s reales
  simStart: process.env.SIM_START || '2026-03-02T06:00:00Z', // un lunes
  seed: n(process.env.SIM_SEED, 42),
  rate: n(process.env.SIM_RATE, 20), // envíos por minuto
  autostart: process.env.AUTOSTART !== 'false',
  port: n(process.env.SIM_PORT, 4000),
  slaEvalMs: n(process.env.SLA_EVAL_MS, 1000), // "cada minuto" simulado (~0,5 s real) -> se evalúa cada 1 s
  inject: {
    demoraHub: n(process.env.P_DEMORA_HUB, 0.1),
    dup: n(process.env.P_DUPLICADO, 0.02),
    ooo: n(process.env.P_FUERA_ORDEN, 0.02),
    bad: n(process.env.P_MALFORMADO, 0.01),
  },
};
