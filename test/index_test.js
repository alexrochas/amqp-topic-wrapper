const {before, describe, it} = require('mocha');
const {emit, on, connect} = require('../amqp-wrapper/index');
const expect = require('chai').expect;

describe('AMQP Topic Wrapper', () => {
  before(() => {
    return connect();
  });

  it('should bind and then emit event', (done) => {
    on('event.test', (msg) => {
      expect(msg.data).to.be.eql('test');
      done()
    });
    emit('event.test', { data: 'test' }, () => {});
  });

  it('should emit and then bind event', (done) => {
    emit('event.secondTest', { data: 'test2' }, () => {});
    on('event.secondTest', (msg) => {
      expect(msg.data).to.be.eql('test2');
      done()
    });
  })

});
