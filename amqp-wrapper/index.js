const uuid = require('uuid');
const amqp = require('amqplib');
const S = require('string');
const callerId = require('caller-id');
const R = require('ramda');

const config = {
  url: process.env.AMQP_URL || 'amqp://admin:admin@localhost:5672',
  exchange: 'events_exchange',
};

const mqConn = amqp.connect(config.url);
const mqChannel = mqConn.then(conn => conn.createChannel());

const queues = {};

const recordCallback = (queueName, eventType, cb) => {
  if (!queues[queueName]) queues[queueName] = {};
  queues[queueName][eventType] = cb;
};

const connect = () => amqp.connect(config.url)
    .then(connection => connection.createChannel())
    .then(channel => channel.assertExchange(config.exchange, 'topic', { durable: true }));

const buildEvent = (eventType, body) => {
  return {
    id: uuid(),
    eventType,
    eventDate: new Date(),
    body,
  };
};

const emit = (eventType, body, cb) => {
  Promise.all([buildEvent(eventType, body), mqChannel])
      .then(R.zipObj(['event', 'channel']))
      .then(({event, channel}) => {
        channel.publish(
            config.exchange,
            eventType,
            new Buffer(JSON.stringify(event)),
            { persistent: true, mandatory: true });
      })
      .then(() => {
        if (cb) return cb();
        return Promise.resolve();
      });
};

const buildQueueName = (caller) => {
  return `amqp.topic.wrapper.${S(S(caller).splitRight('/', 1)[1]).strip(".js")}`;
};

const on = (eventType, cb) => {
  const queueName = buildQueueName(callerId.getData().filePath);

  let channel;
  Promise.all([queueName, mqChannel])
      .then(R.zipObj(['queueName', '_channel']))
      .then(({queueName, _channel}) => {
        channel = _channel;
        return channel.assertQueue(queueName, {
          durable: true,
          exclusive: false,
        });
      })
      .then(() => channel.bindQueue(queueName, config.exchange, eventType))
      .then(() => recordCallback(queueName, eventType, cb))
      .then(() => channel.consume(queueName, (msg) => {
        try {
          const obj = JSON.parse(msg.content);
          const callback = queues[queueName][obj.eventType];
          if (callback) {
            callback(obj.body);
            channel.ack(msg);
          } else {
            channel.nack(msg);
          }
          return Promise.resolve();
        } catch (error) {
          channel.nack(msg);
        }
      }));
};

module.exports = {
  on,
  emit,
  connect,
};
