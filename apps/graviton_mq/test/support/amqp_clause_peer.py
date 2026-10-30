"""Independent, pinned Proton cases for active-name admission and durability."""
import json
import os
import socket
import struct
from urllib.parse import urlsplit
import subprocess
import sys
import time

import proton
from proton import Array, Data, Delivery, Message, Terminus, UNDESCRIBED, symbol
from proton.handlers import MessagingHandler
from proton.reactor import AtLeastOnce, Container, SenderOption
from proton.utils import BlockingConnection, BlockingSender, LinkDetached

assert proton.VERSION == (0, 40, 0), proton.VERSION


def report(**fields):
    print(json.dumps(fields), flush=True)


def connect(url, container='same-client'):
    return BlockingConnection(url, container=Container(container_id=container), timeout=5, sasl_enabled=False)


def sender(connection, name, session=None):
    session = session or connection.conn.session()
    session.open()
    return BlockingSender(connection, connection.container.create_sender(
        session, 'orders', name=name, options=AtLeastOnce()))


def refused(connection, name, session=None):
    try:
        sender(connection, name, session)
    except LinkDetached as error:
        assert error.condition == 'amqp:resource-locked', error
        assert error.link.remote_target.type == Terminus.UNSPECIFIED
        assert error.link.remote_source.type == Terminus.UNSPECIFIED
    else:
        raise AssertionError('Duplicate active name was granted')


def receive(receiver, body):
    message = receiver.receive(timeout=5)
    assert bytes(message.body) == body
    receiver.accept()


def names(left_url, right_url):
    first = connect(left_url)
    second = connect(left_url)
    other = connect(left_url, 'different-client')
    right = connect(right_url)
    try:
        original = sender(first, 'locked')
        refused(first, 'locked')  # A second Session on the original Connection.
        refused(second, 'locked')  # A second Connection, same container pair.
        opposite = second.create_receiver('orders', name='locked', credit=1, options=AtLeastOnce())
        original.send(Message(body=b'first-survives'), timeout=5)
        receive(opposite, b'first-survives')
        reader = second.create_receiver('orders', name='reader', credit=4, options=AtLeastOnce())
        different = sender(other, 'locked')
        different.send(Message(body=b'different-container'), timeout=5)
        receive(reader, b'different-container')
        independent = sender(right, 'locked')
        independent.send(Message(body=b'different-instance'), timeout=5)
        right_reader = right.create_receiver('orders', credit=1, options=AtLeastOnce())
        receive(right_reader, b'different-instance')
        original.close()
        reused = sender(second, 'locked')
        reused.send(Message(body=b'after-detach'), timeout=5)
        receive(reader, b'after-detach')
        child = subprocess.Popen([sys.executable, __file__, 'hold', left_url],
                                 stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        try:
            assert json.loads(child.stdout.readline()) == {'holding': True}
            refused(second, 'down-owned')
            child.communicate('exit\n', timeout=5)
            assert child.returncode == 0
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=5)
        deadline = time.monotonic() + 5
        while True:
            try:
                down_reused = sender(second, 'down-owned')
                break
            except LinkDetached as error:
                assert error.condition == 'amqp:resource-locked', error
                assert time.monotonic() < deadline
                time.sleep(0.01)
        down_reused.send(Message(body=b'after-owner-down'), timeout=5)
        receive(reader, b'after-owner-down')
        report(cross_session=True, cross_connection=True, opposite_direction=True,
               different_container=True, instance_isolation=True, reuse=True, owner_down=True)
    finally:
        for connection in [first, second, other, right]:
            connection.close()


class AcceptedOnly(SenderOption):
    def apply(self, link):
        link.source.address = 'publisher'
        link.source.outcomes.put_object(Array(UNDESCRIBED, Data.SYMBOL, symbol('amqp:accepted:list')))


def durable(url):
    connection = connect(url, 'durable-client')
    try:
        link = connection.create_sender('orders', name='durable-publication', options=[AtLeastOnce(), AcceptedOnly()])
        try:
            link.send(Message(body=b'durable-must-not-be-accepted', durable=True), timeout=5)
        except LinkDetached as error:
            assert error.condition == 'amqp:precondition-failed', error
        else:
            raise AssertionError('Memory broker accepted durable content')
        healthy = connection.create_sender('orders', name='healthy-publication', options=AtLeastOnce())
        healthy.send(Message(body=b'healthy-after-rejection'), timeout=5)
        receiver = connection.create_receiver('orders', credit=1, options=AtLeastOnce())
        receive(receiver, b'healthy-after-rejection')
        report(durable_condition=True, healthy_link=True)
    finally:
        connection.close()


def retiring(url):
    connection = connect(url)
    try:
        link = connection.create_sender('orders', name='retiring', options=[AtLeastOnce(), AcceptedOnly()])
        report(ready=True)
        assert sys.stdin.readline().strip() == 'publish'
        try:
            link.send(Message(body=b'retirement', durable=True), timeout=10)
        except LinkDetached as error:
            assert error.condition == 'amqp:precondition-failed', error
            report(detached=True)
        else:
            raise AssertionError('Expected durable Detach')
    finally:
        connection.close()


def retirement_probe(url, expected):
    connection = connect(url)
    try:
        if expected == 'refuse':
            refused(connection, 'retiring')
            report(refused=True)
        else:
            link = sender(connection, 'retiring')
            link.send(Message(body=b'reused-retirement'), timeout=5)
            receiver = connection.create_receiver('orders', credit=1, options=AtLeastOnce())
            receive(receiver, b'reused-retirement')
            report(reused=True)
    finally:
        connection.close()


class RawPeer:
    """Proton's independent value codec plus explicit malformed-peer framing."""
    def __init__(self, url):
        address = urlsplit(url)
        self.socket = socket.create_connection((address.hostname, address.port), timeout=5)
        self.socket.settimeout(5)
        self.socket.sendall(b'AMQP\x00\x01\x00\x00')
        assert self.read(8) == b'AMQP\x00\x01\x00\x00'
        self.send(0x10, ['raw-peer', None, proton.uint(65536), proton.ushort(255)], channel=0)
        self.until(0x10)
        self.send(0x11, [None, proton.uint(0), proton.uint(16), proton.uint(16), proton.uint(31)])
        _, begin = self.until(0x11)
        assert begin.value[0] == 3
        self.local_channel = 3

    def read(self, size):
        result = b''
        while len(result) < size:
            part = self.socket.recv(size - len(result))
            assert part, 'Unexpected transport closure'
            result += part
        return result

    @staticmethod
    def value(code, fields):
        return proton.Described(proton.ulong(code), fields)

    @staticmethod
    def encode(value):
        data = Data()
        data.put_object(value)
        return data.encode()

    def send(self, code, fields, payload=b'', channel=3):
        body = self.encode(self.value(code, fields)) + payload
        self.socket.sendall(struct.pack('!IBBH', 8 + len(body), 2, 0, channel) + body)

    def until(self, descriptor):
        for _ in range(40):
            size, doff, frame_type, channel = struct.unpack('!IBBH', self.read(8))
            raw = self.read(size - 8)
            if not raw:
                continue
            data = Data()
            data.decode(raw[(doff - 2) * 4:])
            data.rewind()
            data.next()
            value = data.get_object()
            if int(value.descriptor) == descriptor:
                return channel, value
            assert int(value.descriptor) not in [0x17, 0x18], value
        raise AssertionError('Bounded expected frame count exceeded')

    def sender(self, handle=0):
        source = self.value(0x28, ['publisher', None, None, None, None, None, None,
                                 None, None, Array(UNDESCRIBED, Data.SYMBOL, symbol('amqp:accepted:list'))])
        return ['errored', proton.uint(handle), False, proton.ubyte(0), proton.ubyte(0),
                source, self.value(0x29, ['orders']), None, None, proton.uint(0)]

    def close(self):
        self.socket.close()


def error_boundaries(url):
    handles = []
    for performative in ['attach', 'flow', 'transfer', 'detach']:
        peer = RawPeer(url)
        try:
            if performative == 'attach':
                peer.send(0x12, peer.sender(256))  # Broker advertises max_handle255.
            elif performative == 'flow':
                peer.send(0x13, [proton.uint(0), proton.uint(16), proton.uint(0), proton.uint(16), proton.uint(256)])
            elif performative == 'transfer':
                peer.send(0x14, [proton.uint(256), proton.uint(0), b'over-limit', proton.uint(0)])
            else:
                peer.send(0x16, [proton.uint(256), True])
            channel, close = peer.until(0x18)
            assert channel == 0 and close.value[0].value[0] == symbol('amqp:connection:framing-error'), close
            peer.send(0x18, [], channel=0)
            handles.append(performative)
        finally:
            peer.close()

    checked = []
    for violation in ['flow', 'transfer', 'attach-same-handle', 'attach-new-handle', 'disposition']:
        peer = RawPeer(url)
        try:
            peer.send(0x12, peer.sender())
            peer.until(0x12)
            peer.until(0x13)
            raw = peer.encode(peer.value(0x70, [True])) + peer.encode(peer.value(0x75, b'durable'))
            peer.send(0x14, [proton.uint(0), proton.uint(0), b'first', proton.uint(0)], raw)
            _, detach = peer.until(0x16)
            assert detach.value[2].value[0] == symbol('amqp:precondition-failed'), detach
            if violation == 'flow':
                peer.send(0x13, [proton.uint(0), proton.uint(16), proton.uint(1), proton.uint(16),
                                 proton.uint(0), proton.uint(1), proton.uint(0)])
            elif violation == 'transfer':
                peer.send(0x14, [proton.uint(0), proton.uint(1), b'late', proton.uint(0)])
            elif violation.startswith('attach'):
                peer.send(0x12, peer.sender(0 if violation == 'attach-same-handle' else 1))
            else:
                peer.send(0x15, [False, proton.uint(0), None, True])
            _, end = peer.until(0x17)
            assert end.value[0].value[0] == symbol('amqp:session:errant-link'), (violation, end)
            peer.send(0x17, [])
            peer.send(0x18, [], channel=0)
            peer.until(0x18)
            checked.append(violation)
        finally:
            peer.close()
    peer = RawPeer(url)
    try:
        peer.send(0x11, [None, proton.uint(0), proton.uint(16), proton.uint(16)], channel=4)
        _, begin = peer.until(0x11)
        assert begin.value[0] == 4
        peer.send(0x13, [proton.uint(0), proton.uint(16), proton.uint(0), proton.uint(16), proton.uint(0)])
        _, end = peer.until(0x17)
        assert end.value[0].value[0] == symbol('amqp:session:unattached-handle')
        # Session-level bodies must be ignored while its End remains unacknowledged.
        peer.send(0x11, [None, proton.uint(0), proton.uint(16), proton.uint(16)])
        for raw in [b'\xff', b'\x00\x53\x12\x40', b'\x00\x53\x17\x40']:
            peer.socket.sendall(struct.pack('!IBBH', 8 + len(raw), 2, 0, 3) + raw)
        peer.send(0x14, [proton.uint(0xffffffff)])
        peer.send(0x13, [proton.uint(0), proton.uint(16), proton.uint(0), proton.uint(16),
                         None, None, None, None, False, True], channel=4)
        channel, _ = peer.until(0x13)
        assert channel == 1
        peer.send(0x17, [])
        peer.send(0x18, [], channel=3)  # Connection Close remains valid on that channel.
        channel, _ = peer.until(0x18)
        assert channel == 0
    finally:
        peer.close()

    peer = RawPeer(url)
    try:
        peer.send(0x17, [])
        peer.until(0x17)
        raw = b'\x00\x53\x18\x40'  # Connection-level malformed Close stays fatal.
        peer.socket.sendall(struct.pack('!IBBH', 8 + len(raw), 2, 0, 3) + raw)
        _, close = peer.until(0x18)
        assert close.value[0].value[0] == symbol('amqp:connection:framing-error')
        peer.send(0x18, [], channel=0)
    finally:
        peer.close()
    report(handle_limit=True, handle_performatives=handles, errant_inputs=checked,
           ending_session_discard=True, healthy_session=True, connection_validation=True)


class ClientServer(MessagingHandler):
    def __init__(self, url, container):
        super().__init__(prefetch=0, auto_accept=False, auto_settle=False)
        self.url = url
        self.container = container
        self.connections = set()

    def on_start(self, event):
        self.acceptor = event.container.listen(self.url)
        event.container.schedule(20, Deadline())
        report(ready=True)

    def on_connection_opening(self, event):
        event.connection.container = self.container
        self.connections.add(event.connection)

    def on_link_opening(self, event):
        link = event.link
        link.source.copy(link.remote_source)
        link.target.copy(link.remote_target)
        link.source.type = Terminus.SOURCE
        link.source.address = 'orders'
        link.snd_settle_mode = 0
        link.rcv_settle_mode = 0
        if link.is_receiver:
            link.flow(16)
        report(attached=link.name, receiver=link.is_receiver)

    def on_message(self, event):
        event.delivery.update(Delivery.ACCEPTED)
        event.delivery.settle()
        report(received=event.receiver.name)

    def on_sendable(self, event):
        if not getattr(event.sender, 'sent_once', False):
            event.sender.sent_once = True
            event.sender.send(Message(body=b'opposite-direction'))

    def on_connection_closed(self, event):
        self.connection_gone(event)

    def on_transport_closed(self, event):
        self.connection_gone(event)

    def connection_gone(self, event):
        self.connections.discard(event.connection)
        if not self.connections:
            self.acceptor.close()
            event.container.stop()
            report(closed=True)


class Deadline:
    def on_timer_task(self, event):
        event.container.stop()
        raise RuntimeError('Finite independent peer deadline exceeded')


mode = sys.argv[1]
if mode == 'names':
    names(sys.argv[2], sys.argv[3])
elif mode == 'durable':
    durable(sys.argv[2])
elif mode == 'error-boundaries':
    error_boundaries(sys.argv[2])
elif mode == 'retiring':
    retiring(sys.argv[2])
elif mode == 'retirement-probe':
    retirement_probe(sys.argv[2], sys.argv[3])
elif mode == 'hold':
    connection = connect(sys.argv[2])
    link = sender(connection, 'down-owned')
    report(holding=True)
    assert sys.stdin.readline().strip() == 'exit'
    os._exit(0)  # Actual process/socket loss without AMQP Close/Detach.
else:
    Container(ClientServer(sys.argv[2], sys.argv[3])).run()
