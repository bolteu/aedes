const { test } = require('tap')
const { connect, setup, subscribe } = require('./helper')
const aedes = require('../aedes')

test('subscribe a single shared topic QoS 0', function (t) {
  t.plan(4)

  const broker = new aedes.Server({ sharedTopicsEnabled: true })
  const s = connect(setup(broker))
  t.teardown(s.broker.close.bind(s.broker))

  const expected = {
    cmd: 'publish',
    topic: 'topic',
    payload: Buffer.from('world'),
    dup: false,
    length: 12,
    qos: 0,
    retain: false
  }

  subscribe(t, s, '$share/someGroup/topic', 0, function () {
    s.outStream.once('data', function (packet) {
      t.same(packet, expected, 'packet matches')
    })

    s.broker.publish({
      cmd: 'publish',
      topic: 'topic',
      payload: 'world'
    })
  })
})

test('subscribe 2 clients to shared topic QoS 0', function (t) {
  t.plan(8)

  const broker = new aedes.Server({ sharedTopicsEnabled: true })
  const c1 = connect(setup(broker))
  t.teardown(c1.broker.close.bind(c1.broker))

  const expected = {
    cmd: 'publish',
    topic: 'hello',
    payload: Buffer.from('world'),
    dup: false,
    length: 12,
    qos: 0,
    retain: false
  }

  subscribe(t, c1, '$share/group1/hello', 0, function () {
    c1.outStream.once('data', function (packet) {
      t.same(packet, expected, 'packet matches c1')
    })

    const c2 = connect(setup(broker))
    t.teardown(c2.broker.close.bind(c2.broker))
    subscribe(t, c2, '$share/group2/hello', 0, function () {
      c2.outStream.once('data', function (packet) {
        t.same(packet, expected, 'packet matches c2')
      })

      c1.broker.publish({
        cmd: 'publish',
        topic: 'hello',
        payload: 'world'
      })
    })
  })
})

test('subscribe to shared wildcard topic QoS 0', function (t) {
  t.plan(5)

  const broker = new aedes.Server({ sharedTopicsEnabled: true })
  const s = connect(setup(broker))
  t.teardown(s.broker.close.bind(s.broker))

  const expected = {
    cmd: 'publish',
    topic: 'topic/123',
    payload: Buffer.from('world'),
    dup: false,
    length: 16,
    qos: 0,
    retain: false
  }

  subscribe(t, s, '$share/someGroup/topic/#', 0, function () {
    s.outStream.once('data', function (packet) {
      t.same(packet, expected, 'packet matches')
      const expected2 = {
        cmd: 'publish',
        topic: 'topic/321',
        payload: Buffer.from('world'),
        dup: false,
        length: 16,
        qos: 0,
        retain: false
      }
      s.outStream.once('data', function (packet) {
        t.same(packet, expected2, 'packet matches')
      })

      s.broker.publish({
        cmd: 'publish',
        topic: 'topic/321',
        payload: 'world'
      })
    })

    s.broker.publish({
      cmd: 'publish',
      topic: 'topic/123',
      payload: 'world'
    })
  })
})

test('unsubscribe', function (t) {
  t.plan(5)

  const broker = new aedes.Server({ sharedTopicsEnabled: true })
  const s = connect(setup(broker))
  t.teardown(s.broker.close.bind(s.broker))

  subscribe(t, s, '$share/someGroup/topic', 0, function () {
    s.inStream.write({
      cmd: 'unsubscribe',
      messageId: 43,
      unsubscriptions: ['$share/someGroup/topic']
    })

    s.outStream.once('data', function (packet) {
      t.same(packet, {
        cmd: 'unsuback',
        messageId: 43,
        dup: false,
        length: 2,
        qos: 0,
        retain: false
      }, 'packet matches')

      s.outStream.on('data', function (packet) {
        t.fail('packet received')
      })

      s.broker.publish({
        cmd: 'publish',
        topic: 'topic',
        payload: 'world'
      }, function () {
        t.pass('publish finished')
      })
    })
  })
})
