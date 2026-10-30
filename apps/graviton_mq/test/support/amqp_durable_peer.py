"""Independent pinned Qpid Proton peer for durable broker restart qualification."""
import argparse
import hashlib
import json
import sys
from pathlib import Path

import proton
from proton import Delivery, Message, symbol
from proton.reactor import AtLeastOnce
from proton.utils import BlockingConnection

parser = argparse.ArgumentParser()
parser.add_argument("operation", choices=["send", "receive", "hold", "modify", "reject"])
parser.add_argument("url")
parser.add_argument("address")
parser.add_argument("path")
parser.add_argument("--modified", action="store_true")
args = parser.parse_args()
assert proton.VERSION == (0, 40, 0), proton.VERSION
expected = Path(args.path).read_bytes()
connection = BlockingConnection(args.url, timeout=8, sasl_enabled=False)
try:
    if args.operation == "send":
        sender = connection.create_sender(args.address, options=AtLeastOnce())
        sender.send(Message(body=expected, durable=True,
                            annotations={symbol("x-opt-keep"): 3}), timeout=8)
        print(json.dumps({"accepted": True}), flush=True)
    else:
        receiver = connection.create_receiver(args.address, credit=1, options=AtLeastOnce())
        print(json.dumps({"ready": True}), flush=True)
        message = receiver.receive(timeout=8)
        assert bytes(message.body) == expected, "authoritative body changed"
        assert message.durable, "durable header lost"
        if args.modified:
            assert message.delivery_count == 1, message.delivery_count
            assert message.annotations == {symbol("x-opt-keep"): 3, symbol("x-opt-recovered"): "yes"}, message.annotations
        if args.operation == "hold":
            print(json.dumps({"held": True}), flush=True)
            if sys.stdin.readline().strip() == "accept":
                receiver.accept()
        elif args.operation == "modify":
            delivery = receiver.fetcher.unsettled[0]
            delivery.local.failed = True
            delivery.local.annotations = {symbol("x-opt-recovered"): "yes"}
            receiver.settle(Delivery.MODIFIED)
            print(json.dumps({"modified": True}), flush=True)
            # BlockingReceiver.settle queues local state. Closing pumps and
            # flushes its disposition before this independent peer exits.
            connection.close()
        elif args.operation == "reject":
            receiver.reject()
        else:
            receiver.accept()
        print(json.dumps({"sha256": hashlib.sha256(expected).hexdigest()}), flush=True)
finally:
    connection.close()
