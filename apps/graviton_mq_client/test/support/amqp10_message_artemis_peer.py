"""Pinned Proton raw-message corpus for the independent client/Artemis path.

Only test fixture builders are shared with the broker corpus. Transfers use
Proton's raw link stream, without Message.decode/re-encode on either path.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import sys

import proton
from proton import Data, Delivery, Described
from proton.reactor import AtLeastOnce
from proton.utils import BlockingConnection

sys.dont_write_bytecode = True
FIXTURES = Path(__file__).resolve().parents[3] / "graviton_mq/test/support/amqp10_message_peer.py"
spec = importlib.util.spec_from_file_location("graviton_independent_message_fixtures", FIXTURES)
corpus = importlib.util.module_from_spec(spec)
spec.loader.exec_module(corpus)


def bare(raw):
    remaining = raw
    while remaining:
        data = Data()
        consumed = data.decode(remaining)
        data.rewind()
        assert consumed > 0 and data.next()
        section = data.get_object()
        assert isinstance(section, Described)
        if int(section.descriptor) == 0x73:
            return remaining
        assert int(section.descriptor) in (0x70, 0x71, 0x72)
        remaining = remaining[consumed:]
    raise AssertionError("missing properties/bare message")


def check_message(name, actual, original):
    actual_bare, expected_bare = bare(actual), bare(original)
    assert actual_bare == expected_bare, {
        "name": name, "actual_bytes": len(actual_bare), "expected_bytes": len(expected_bare),
        "actual_sha256": hashlib.sha256(actual_bare).hexdigest(),
        "expected_sha256": hashlib.sha256(expected_bare).hexdigest(),
    }
    # Header, delivery annotations and message annotations are mutable. Optional
    # original message annotations still have to reach this independent peer.
    expected = dict(corpus.sections(original))[0x72]
    annotations = dict(corpus.sections(actual))[0x72]
    assert all(key in annotations and annotations[key] == value for key, value in expected.items()), name
    return {"name": name, "bare_bytes": len(actual_bare),
            "bare_sha256": hashlib.sha256(actual_bare).hexdigest()}


def generate(directory):
    directory.mkdir(parents=True, exist_ok=True)
    names = []
    for name, original, _expected in corpus.fixtures():
        (directory / (name + ".amqp")).write_bytes(original)
        names.append(name)
    assert len(names) == 12
    (directory / "manifest").write_text("\n".join(names) + "\n")
    return {"generated": len(names)}


def transfer(connection, mode, address, directory):
    records = []
    fixtures = list(corpus.fixtures())
    assert len(fixtures) == 12
    if mode == "receive":
        handler = corpus.RawReceiver(connection)
        receiver = connection.create_receiver(address, handler=handler, credit=1,
                                              options=AtLeastOnce())
        print("READY", flush=True)
        for name, original, _expected in fixtures:
            connection.wait(lambda: bool(handler.incoming), timeout=10)
            actual, delivery = handler.incoming.popleft()
            records.append(check_message(name, actual, original))
            (directory / (name + ".received")).write_bytes(actual)
            delivery.update(Delivery.ACCEPTED)
            delivery.settle()
            receiver.link.flow(1)
        receiver.close()
    else:
        sender = connection.create_sender(address, options=AtLeastOnce())
        for name, original, _expected in fixtures:
            connection.wait(lambda: sender.link.credit > 0, timeout=10)
            delivery = sender.link.delivery("artemis-corpus-" + name)
            assert sender.link.stream(original) == len(original)
            assert sender.link.advance()
            connection.wait(lambda: delivery.settled, timeout=10)
            assert delivery.remote_state == Delivery.ACCEPTED, (name, delivery.remote_state)
            delivery.settle()
            records.append({"name": name, "sent_bytes": len(original),
                            "bare_sha256": hashlib.sha256(bare(original)).hexdigest()})
        sender.close()
    return {"mode": mode, "fixtures": len(records), "records": records}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["fixtures", "send", "receive"])
    parser.add_argument("directory", type=Path)
    parser.add_argument("--url")
    parser.add_argument("--address")
    arguments = parser.parse_args()
    assert proton.VERSION == (0, 40, 0), proton.VERSION
    if arguments.mode == "fixtures":
        result = generate(arguments.directory)
    else:
        assert arguments.url and arguments.address
        connection = BlockingConnection(arguments.url, timeout=10, sasl_enabled=False)
        try:
            result = transfer(connection, arguments.mode, arguments.address, arguments.directory)
        finally:
            connection.close()
    print("PASS " + json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
