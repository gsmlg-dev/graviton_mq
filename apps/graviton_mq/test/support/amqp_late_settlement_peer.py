"""Pinned independent Proton peers for initially unsettled sender settlement."""
import json
import sys

import proton
from proton import Delivery, Message, Terminus
from proton.handlers import MessagingHandler
from proton.reactor import AtLeastOnce, Container
from proton.utils import BlockingConnection, BlockingSender, LinkDetached

assert proton.VERSION == (0, 40, 0), proton.VERSION


def report(**fields):
    print(json.dumps(fields), flush=True)


class Tick:
    done = False

    def on_timer_task(self, event):
        self.done = True


def pump(connection):
    tick = Tick()
    connection.container.schedule(0.05, tick)
    connection.wait(lambda: tick.done, timeout=5)


def broker_peer(url):
    connection = BlockingConnection(url, timeout=5, sasl_enabled=False)
    try:
        first = connection.create_sender('orders', name='first', options=AtLeastOnce())
        # Explicitly reuse the same Session, so a mistaken Session End also
        # terminates the unrelated second Link checked below.
        second = BlockingSender(connection, connection.container.create_sender(
            first.link.session, 'orders', name='second', options=AtLeastOnce()))
        receiver = connection.create_receiver('orders', credit=2, options=AtLeastOnce())
        connection.wait(lambda: first.link.credit > 0 and second.link.credit > 0, timeout=5)
        report(ready=True)
        assert sys.stdin.readline().strip() == 'send'
        delivery = first.link.send(Message(body=b'first-host-responsibility'))
        pump(connection)  # The initial Transfer goes on the wire unsettled.
        delivery.settle()
        pump(connection)  # Flush the later sender Disposition before gating.
        report(sender_settled=True)
        assert sys.stdin.readline().strip() == 'continue'
        second.send(Message(body=b'second-link-still-active'), timeout=5)
        bodies = []
        for _ in range(2):
            message = receiver.receive(timeout=5)
            bodies.append(bytes(message.body))
            receiver.accept()
        assert bodies == [b'first-host-responsibility', b'second-link-still-active'], bodies
        report(host_responsibility_preserved=True, second_link=True)
    finally:
        connection.close()


def broker_receiver_peer(url):
    connection = BlockingConnection(url, timeout=5, sasl_enabled=False)
    try:
        publisher = connection.create_sender('orders', options=AtLeastOnce())
        publisher.send(Message(body=b'unknown-must-redeliver'), timeout=5)
        first = connection.create_receiver('orders', credit=1, options=AtLeastOnce())
        second = BlockingSender(connection, connection.container.create_sender(
            first.link.session, 'orders', name='unrelated-sender', options=AtLeastOnce()))
        message = first.receive(timeout=5)
        assert bytes(message.body) == b'unknown-must-redeliver'
        first.settle()  # Legal receiver settlement with no delivery-state.
        try:
            pump(connection)
        except LinkDetached as error:
            # This profile abandons only the uncertain receiver Link after
            # returning its claim. Its Session and unrelated Link must survive.
            assert error.link == first.link, error
            assert error.condition == 'amqp:resource-limit-exceeded', error
        second.send(Message(body=b'unrelated-session-traffic'), timeout=5)
        receiver = connection.create_receiver('orders', credit=2, options=AtLeastOnce())
        bodies = []
        for _ in range(2):
            message = receiver.receive(timeout=5)
            bodies.append(bytes(message.body))
            receiver.accept()
        assert bodies == [b'unknown-must-redeliver', b'unrelated-session-traffic'], bodies
        report(receiver_settled=True, responsibility_requeued=True, second_link=True)
    finally:
        connection.close()


class SettleLater:
    def __init__(self, owner, delivery):
        self.owner = owner
        self.delivery = delivery

    def on_timer_task(self, event):
        self.delivery.settle()
        event.reactor.schedule(0.05, Settled(self.owner))


class Settled:
    def __init__(self, owner):
        self.owner = owner

    def on_timer_task(self, event):
        report(sender_settled=True)


class ClientPeer(MessagingHandler):
    def __init__(self, url, receive=False):
        super().__init__(prefetch=0, auto_accept=False, auto_settle=False)
        self.url = url
        self.sent = set()
        self.failed = False
        self.receive = receive

    def on_start(self, event):
        self.acceptor = event.container.listen(self.url)
        event.container.schedule(10, Deadline(self))
        report(ready=True)

    def on_connection_opening(self, event):
        event.connection.container = 'independent-proton-late-settlement'

    def on_link_opening(self, event):
        event.link.source.copy(event.link.remote_source)
        event.link.target.copy(event.link.remote_target)
        event.link.snd_settle_mode = 0
        event.link.rcv_settle_mode = 0
        if self.receive and event.link.is_receiver:
            # Proton omits a copied address-less Source. Supply an explicit
            # supported source rather than accidentally refusing the Attach.
            event.link.source.type = Terminus.SOURCE
            event.link.source.address = 'q'
            event.link.flow(1)

    def on_message(self, event):
        assert self.receive
        name = event.receiver.name
        assert bytes(event.message.body) == name.encode('ascii')
        self.sent.add(name)
        if name == 'first':
            event.delivery.settle()
            report(receiver_settled=True)
        else:
            event.delivery.update(Delivery.ACCEPTED)
            event.delivery.settle()

    def on_sendable(self, event):
        name = event.sender.name
        if name not in self.sent:
            self.sent.add(name)
            delivery = event.sender.send(Message(body=name.encode('ascii')))
            if name == 'first':
                event.reactor.schedule(0.05, SettleLater(self, delivery))

    def on_connection_closed(self, event):
        self.acceptor.close()
        event.container.stop()
        report(second_link='second' in self.sent)

    def on_transport_error(self, event):
        self.failed = True
        raise RuntimeError(str(event.transport.condition))


class Deadline:
    def __init__(self, owner):
        self.owner = owner

    def on_timer_task(self, event):
        self.owner.acceptor.close()
        event.container.stop()
        raise RuntimeError('Peer lifecycle exceeded finite deadline')


if sys.argv[1] == 'broker':
    broker_peer(sys.argv[2])
elif sys.argv[1] == 'broker-receiver':
    broker_receiver_peer(sys.argv[2])
else:
    Container(ClientPeer(sys.argv[2], receive=sys.argv[1] == 'client-sender')).run()
