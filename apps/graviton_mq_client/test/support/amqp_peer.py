"""Independent Qpid Proton 0.40.0 client used against the external test broker.

The Graviton client is never imported here. Commands terminate with a finite
timeout and report content digests so large test bodies need not be printed.
"""
import argparse
import hashlib
import json
import sys
from pathlib import Path

import proton
from proton import Message, Delivery, SSLDomain, symbol
from proton.utils import BlockingConnection
from proton.reactor import AtLeastOnce


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=["send", "receive", "release", "modified", "reject", "receive-hold"])
    parser.add_argument("url")
    parser.add_argument("address")
    parser.add_argument("path")
    parser.add_argument("--gate", action="store_true")
    parser.add_argument("--durable", action="store_true")
    parser.add_argument("--annotated", action="store_true")
    parser.add_argument("--cafile")
    arguments = parser.parse_args()
    version = ".".join(str(value) for value in proton.VERSION)
    if version != "0.40.0":
        raise RuntimeError(f"Expected Qpid Proton 0.40.0, got {version}")
    ssl_domain = None
    if arguments.cafile:
        ssl_domain = SSLDomain(SSLDomain.MODE_CLIENT)
        ssl_domain.set_trusted_ca_db(arguments.cafile)
        ssl_domain.set_peer_authentication(SSLDomain.VERIFY_PEER_NAME)
    connection = BlockingConnection(arguments.url, timeout=10, sasl_enabled=False,
                                    ssl_domain=ssl_domain)
    try:
        if arguments.operation == "send":
            body = Path(arguments.path).read_bytes()
            sender = connection.create_sender(arguments.address, options=AtLeastOnce())
            annotations = {symbol("x-opt-keep"): 3, symbol("x-opt-replace"): "old"} if arguments.annotated else None
            sender.send(Message(body=body, durable=arguments.durable, annotations=annotations), timeout=10)
            print(json.dumps({"sent_bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}))
        else:
            receiver = connection.create_receiver(arguments.address, credit=0 if arguments.gate else 1,
                                                  options=AtLeastOnce())
            print(json.dumps({"ready": True}), flush=True)
            if arguments.gate:
                if not sys.stdin.readline():
                    raise RuntimeError("No host credit gate command")
            message = receiver.receive(timeout=10)
            body = bytes(message.body)
            if arguments.operation in ("release", "modified"):
                if arguments.operation == "modified":
                    delivery = receiver.fetcher.unsettled[0]
                    delivery.local.failed = True
                    delivery.local.annotations = {symbol("x-opt-replace"): "new", symbol("x-opt-added"): True}
                    receiver.settle(Delivery.MODIFIED)
                else:
                    receiver.release(delivered=False)
                redelivered = receiver.receive(timeout=10)
                if bytes(redelivered.body) != body:
                    raise RuntimeError("Redelivery body changed")
                if arguments.operation == "modified":
                    if redelivered.delivery_count != 1 or redelivered.annotations != {
                            symbol("x-opt-keep"): 3, symbol("x-opt-replace"): "new", symbol("x-opt-added"): True}:
                        raise RuntimeError("Modified flags or annotations were not honored")
                receiver.accept()
            elif arguments.operation == "reject":
                receiver.reject()
            elif arguments.operation == "receive-hold":
                print(json.dumps({"unsettled_bytes": len(body)}), flush=True)
                command = sys.stdin.readline()
                if not command:
                    raise RuntimeError("No disconnect gate command")
                if command.strip() == "accept":
                    receiver.accept()
            else:
                receiver.accept()
            print(json.dumps({"received_bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}), flush=True)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
