const { Kafka, logLevel } = require('kafkajs');
const cfg = require('./config');

const makeKafka = (clientId) =>
  new Kafka({ clientId, brokers: cfg.brokers, logLevel: logLevel.WARN, retry: { initialRetryTime: 300, retries: 30 } });

async function makeProducer(clientId) {
  const p = makeKafka(clientId).producer({ allowAutoTopicCreation: false });
  await p.connect();
  return p;
}

const send = (producer, topic, ev) =>
  producer.send({ topic, messages: [{ key: ev.shipmentId, value: JSON.stringify(ev) }] });

module.exports = { makeKafka, makeProducer, send };
