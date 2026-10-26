const test = require('node:test');
const assert = require('node:assert');
const { planBatch, planShipment } = require('../src/shared/planner');
const { decidirReprogramacion, reglasActivas, H } = require('../src/shared/rules');
const { validate } = require('../src/shared/schema');
const P = { demoraHub: 0.1, dup: 0.02, ooo: 0.02, bad: 0.01 };

test('el planificador es determinista con semilla fija', () => {
  const a = planBatch({ seed: 42, count: 200, baseMs: Date.parse('2026-03-02T06:00:00Z'), p: P });
  const b = planBatch({ seed: 42, count: 200, baseMs: Date.parse('2026-03-02T06:00:00Z'), p: P });
  assert.deepStrictEqual(a.items.map((i) => i.value), b.items.map((i) => i.value));
  assert.ok(a.stats.dup > 0 && a.stats.bad > 0 && a.stats.ooo > 0);
});
test('todo evento válido generado cumple el esquema y todo malformado falla', () => {
  const { items } = planBatch({ seed: 7, count: 300, baseMs: Date.parse('2026-03-02T06:00:00Z'), p: P });
  for (const it of items) {
    let ev = null; try { ev = JSON.parse(it.value); } catch {}
    const errs = validate(it.topic, ev);
    if (it.kind === 'bad') assert.ok(errs.length > 0, it.value);
    else assert.deepStrictEqual(errs, [], it.value);
  }
});
test('secuencia y tipos del ciclo de vida', () => {
  const pl = planShipment({ shipmentId: 'X', index: 1, seed: 1, creadoMs: Date.parse('2026-03-02T06:00:00Z') });
  const est = pl.items.filter((i) => i.ev.tipo !== 'PROMESA').map((i) => i.ev);
  est.forEach((e, i) => assert.strictEqual(e.secuencia, i + 1));
  assert.strictEqual(est[0].tipo, 'CREADO');
});
test('reglas de reprogramación', () => {
  const f = '2026-03-06T10:00:00Z'; // viernes por la mañana
  const a = decidirReprogramacion({ motivo: 'AUSENTE', intento: 1, ocurridoEn: f });
  assert.deepStrictEqual(a.proximoIntento, { fecha: '2026-03-09', franja: 'TARDE' }); // lunes, franja opuesta
  assert.strictEqual(decidirReprogramacion({ motivo: 'DIRECCION_INCORRECTA', intento: 1, ocurridoEn: f }).tipo, 'RETENIDO');
  assert.strictEqual(decidirReprogramacion({ motivo: 'RECHAZADO', intento: 1, ocurridoEn: f }).tipo, 'DEVOLUCION');
  assert.strictEqual(decidirReprogramacion({ motivo: 'AUSENTE', intento: 3, ocurridoEn: f }).tipo, 'DEVOLUCION');
});
test('reglas de SLA y prioridades', () => {
  const v = Date.parse('2026-03-03T20:00:00Z');
  const nombres = (s, now) => reglasActivas(s, now).map((r) => r.regla + ':' + r.prioridad);
  assert.deepStrictEqual(nombres({ estado: 'CREADO', venceEn: v }, v - 11 * H), []);
  assert.deepStrictEqual(nombres({ estado: 'CREADO', venceEn: v }, v - 9 * H), ['SIN_INGRESO:MEDIA']);
  assert.deepStrictEqual(nombres({ estado: 'INGRESADO_HUB', venceEn: v }, v - 3 * H), ['SIN_SALIDA:ALTA']);
  assert.deepStrictEqual(nombres({ estado: 'ENTREGA_FALLIDA', venceEn: v, ultimoFalloEn: v - 2 * H }, v - 1 * H), ['FALLO_DIA_LIMITE:ALTA']);
  assert.ok(nombres({ estado: 'EN_REPARTO', venceEn: v }, v + 1).includes('VENCIDO:CRITICA'));
  assert.deepStrictEqual(nombres({ estado: 'ENTREGADO', venceEn: v }, v + 5 * H), []);
});

test('escenarios: secuencias válidas y esquema correcto', () => {
  const { planScenario, nombres } = require('../src/simulator/scenarios');
  for (const n of nombres) {
    const items = planScenario(n, { shipmentId: 'DEMO-' + n, ahora: Date.parse('2026-03-02T06:00:00Z'), accel: 120 });
    for (const it of items) assert.deepStrictEqual(validate(it.topic, it.ev), [], `${n}: ${it.value}`);
  }
  const tf = planScenario('tercerFallo', { shipmentId: 'T', ahora: 0 + Date.parse('2026-03-02T06:00:00Z'), accel: 120 });
  assert.strictEqual(tf.filter((i) => i.ev.tipo === 'ENTREGA_FALLIDA').length, 3);
});
