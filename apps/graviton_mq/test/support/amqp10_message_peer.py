"""Pinned independent Proton message forwarding checks; test infrastructure."""
import argparse
import gzip
import hashlib
import json
import struct
import uuid
from collections import deque

import proton
from proton import (Array, Data, Delivery, Described, Handler, Message,
                    UNDESCRIBED, byte, int32, symbol, timestamp, ubyte, uint, ulong)
from proton.reactor import AtLeastOnce
from proton.utils import BlockingConnection


def first_acquirer(connection):
    sender = connection.create_sender("messages", options=AtLeastOnce())
    sender.send(Message(body=b"acquired-by-another-link", first_acquirer=True), timeout=5)
    first = connection.create_receiver("messages", name="first", credit=1, options=AtLeastOnce())
    original = first.receive(timeout=5)
    assert bytes(original.body) == b"acquired-by-another-link"
    first.release()
    first.close()
    second = connection.create_receiver("messages", name="second", credit=1, options=AtLeastOnce())
    reacquired = second.receive(timeout=5)
    assert bytes(reacquired.body) == b"acquired-by-another-link"
    assert not reacquired.first_acquirer, "first-acquirer true survived acquisition by another Link"
    second.accept()
    second.close()
    return {"reacquired_first_acquirer": reacquired.first_acquirer}


def section(code, value):
    data = Data()
    data.put_object(Described(ulong(code), value))
    encoded = bytes(data.encode())
    assert encoded[:3] == bytes([0, 0x53, code]), encoded.hex()
    # The specification also permits a full ulong descriptor. Retain this
    # nonminimal form so a decode/re-encode intermediary cannot pass unnoticed.
    return b"\x00\x80" + struct.pack("!Q", code) + encoded[3:]


def sections(raw):
    result = []
    while raw:
        data = Data()
        consumed = data.decode(raw)
        assert consumed > 0
        data.rewind()
        assert data.next()
        value = data.get_object()
        assert isinstance(value, Described)
        result.append((int(value.descriptor), owned(value.value)))
        raw = raw[consumed:]
    return result


def owned(value):
    # Proton's decoded binary memoryviews borrow the Data allocation. Copy
    # them before that allocation is freed or reused for the next section.
    if isinstance(value, memoryview):
        return bytes(value)
    if isinstance(value, list):
        return [owned(item) for item in value]
    if isinstance(value, dict):
        return {owned(key): owned(item) for key, item in value.items()}
    if isinstance(value, Array):
        return ("array", owned(value.descriptor), value.type,
                tuple(owned(item) for item in value.elements))
    if isinstance(value, Described):
        return Described(owned(value.descriptor), owned(value.value))
    return value


class RawReceiver(Handler):
    """Read bytes through Proton without Message.decode/re-encode."""
    def __init__(self, connection):
        self.connection = connection
        self.partial, self.incoming = {}, deque()

    def on_delivery(self, event):
        delivery = event.delivery
        if not delivery.readable:
            return
        assert not delivery.aborted
        raw = self.partial.pop(delivery.tag, b"")
        if delivery.pending:
            raw += bytes(event.receiver.recv(delivery.pending))
        if delivery.partial:
            self.partial[delivery.tag] = raw
            return
        event.receiver.advance()
        self.incoming.append((raw, delivery))
        self.connection.container.yield_()


def fixtures(durable=False):
    identifiers = [ulong(123), uuid.UUID("01234567-89ab-cdef-8123-456789abcdef"),
                   b"\x00\xff\x80-id", "message-id-雪"]
    application = {"null": None, "boolean": True, "binary": b"\x00\xff\x80",
                   "byte": byte(-128), "int": int32(-129), "ulong": ulong(1 << 40),
                   "text": "application-雪"}
    annotations = {symbol("x-opt-sequence"): uint(17), symbol("x-opt-null"): None,
                   symbol("x-opt-array"): Array(UNDESCRIBED, Data.UINT, uint(0), uint(256))}
    header = section(0x70, [durable, ubyte(7), None, False, uint(3)])
    hop = section(0x71, {symbol("x-opt-hop"): b"remove-at-next-hop"})
    infrastructure = section(0x72, annotations)
    for kind in ["data", "sequence", "value"]:
        if kind == "data":
            payload = b"".join(hashlib.sha256(str(index).encode()).digest() for index in range(128))
            compressed = gzip.compress(payload, mtime=0)
            midpoint = len(compressed) // 2
            body = section(0x75, compressed[:midpoint]) + section(0x75, compressed[midpoint:])
            assert gzip.decompress(b"".join(value for _code, value in sections(body))) == payload
        elif kind == "sequence":
            body = section(0x76, [uint(1), "sequence-雪", [None, False]])
            body += section(0x76, [b"\x00\xff", ulong(1 << 40)])
        else:
            body = section(0x77, {None: "generic-null-map-key", "nested": [
                uint(1), symbol("distinct-symbol"), b"distinct-binary", "distinct-string",
                Array(UNDESCRIBED, Data.UINT, uint(0), uint(256))]})
        footer = section(0x78, {symbol("x-opt-sha256"): hashlib.sha256(body).digest()})
        for index, message_id in enumerate(identifiers):
            fields = [message_id, b"\x00\xff-user", "unchanged-to", "subject-雪",
                      "reply-to", identifiers[(index + 1) % len(identifiers)],
                      symbol("application/x-graviton"),
                      symbol("gzip") if kind == "data" else None, None,
                      timestamp(123456789), "group-id", uint(9), "reply-group-id"]
            bare = section(0x73, fields) + section(0x74, application) + body + footer
            original = header + hop + infrastructure + bare
            expected = header + infrastructure + bare
            yield kind + "-" + str(index), original, expected


def forwarding(connection):
    handler = RawReceiver(connection)
    receiver = connection.create_receiver("messages", handler=handler, credit=1,
                                          options=AtLeastOnce())
    sender = connection.create_sender("messages", options=AtLeastOnce())
    records = []
    for name, original, expected in fixtures():
        connection.wait(lambda: sender.link.credit > 0, timeout=5)
        sent = sender.link.delivery("opaque-" + name)
        assert sender.link.stream(original) == len(original)
        assert sender.link.advance()
        connection.wait(lambda: sent.settled, timeout=5)
        assert sent.remote_state == Delivery.ACCEPTED, (name, sent.remote_state)
        sent.settle()
        connection.wait(lambda: bool(handler.incoming), timeout=5)
        actual, received = handler.incoming.popleft()
        assert actual == expected, (name, hashlib.sha256(actual).hexdigest(),
                                    hashlib.sha256(expected).hexdigest())
        decoded = sections(actual)
        assert [code for code, _value in decoded] == [
            0x70, 0x72, 0x73, 0x74,
            *([0x75, 0x75] if name.startswith("data") else
              [0x76, 0x76] if name.startswith("sequence") else [0x77]), 0x78]
        assert decoded[2][1] == sections(original)[3][1], name
        received.update(Delivery.ACCEPTED)
        received.settle()
        records.append({"name": name, "bytes": len(actual),
                        "sha256": hashlib.sha256(actual).hexdigest()})
        receiver.link.flow(1)
    receiver.close()
    sender.close()
    return {"fixtures": len(records), "body_kinds": ["data", "sequence", "value"],
            "message_id_types": ["ulong", "uuid", "binary", "string"],
            "exact_forwarding": records}


def send_corpus(connection, address, durable):
    sender = connection.create_sender(address, options=AtLeastOnce())
    count = 0
    for name, original, _expected in fixtures(durable):
        connection.wait(lambda: sender.link.credit > 0, timeout=5)
        delivery = sender.link.delivery("corpus-" + name)
        assert sender.link.stream(original) == len(original)
        assert sender.link.advance()
        connection.wait(lambda: delivery.settled, timeout=5)
        assert delivery.remote_state == Delivery.ACCEPTED, (name, delivery.remote_state)
        delivery.settle()
        count += 1
    sender.close()
    return {"sent_fixtures": count}


def receive_corpus(connection, address, durable):
    handler = RawReceiver(connection)
    receiver = connection.create_receiver(address, handler=handler, credit=1,
                                          options=AtLeastOnce())
    print("READY corpus", flush=True)
    records = []
    for name, _original, expected in fixtures(durable):
        connection.wait(lambda: bool(handler.incoming), timeout=5)
        actual, delivery = handler.incoming.popleft()
        assert actual == expected, (name, hashlib.sha256(actual).hexdigest(),
                                    hashlib.sha256(expected).hexdigest())
        delivery.update(Delivery.ACCEPTED)
        delivery.settle()
        receiver.link.flow(1)
        records.append({"name": name, "bytes": len(actual),
                        "sha256": hashlib.sha256(actual).hexdigest()})
    receiver.close()
    return {"received_fixtures": len(records), "records": records}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("url")
    parser.add_argument("mode", choices=["first-acquirer", "forwarding", "send-corpus", "receive-corpus"])
    parser.add_argument("--address", default="messages")
    parser.add_argument("--durable", action="store_true")
    arguments = parser.parse_args()
    assert proton.VERSION == (0, 40, 0), proton.VERSION
    connection = BlockingConnection(arguments.url, timeout=5, sasl_enabled=False)
    try:
        if arguments.mode == "first-acquirer":
            result = first_acquirer(connection)
        elif arguments.mode == "forwarding":
            result = forwarding(connection)
        elif arguments.mode == "send-corpus":
            result = send_corpus(connection, arguments.address, arguments.durable)
        else:
            result = receive_corpus(connection, arguments.address, arguments.durable)
        print("PASS " + json.dumps(result), flush=True)
    finally:
        connection.close()
