"""Independent Qpid Proton 0.40.0 SASL client used against the Graviton broker.

The Graviton client is never imported here. Commands terminate with a finite
timeout, print JSON without credentials and report content digests.
"""
import argparse
import hashlib
import inspect
import json
import sys
from pathlib import Path

import proton
import proton._reactor
from proton import Message, SSLDomain
from proton.reactor import AtLeastOnce
from proton.utils import BlockingConnection

# In Proton 0.40.0 `proton.reactor` only re-exports `proton._reactor`, whose
# `Container` reads these connect options as `kwargs.get('<name>', ...)`.
KEYWORDS = ("sasl_enabled", "allowed_mechs", "allow_insecure_mechs", "user", "password")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=["send", "receive"])
    parser.add_argument("url")
    parser.add_argument("address")
    parser.add_argument("path")
    parser.add_argument("--no-sasl", action="store_true")
    parser.add_argument("--mechs")
    parser.add_argument("--user")
    parser.add_argument("--password")
    parser.add_argument("--insecure", action="store_true")
    parser.add_argument("--cafile")
    arguments = parser.parse_args()
    version = ".".join(str(value) for value in proton.VERSION)
    if version != "0.40.0":
        raise RuntimeError(f"Expected Qpid Proton 0.40.0, got {version}")
    source = inspect.getsource(proton._reactor.Container)
    missing = [keyword for keyword in KEYWORDS if f"kwargs.get('{keyword}'" not in source]
    if missing:
        raise RuntimeError(f"Proton connect keywords unavailable: {missing}")
    options = {"sasl_enabled": not arguments.no_sasl}
    if arguments.mechs:
        options["allowed_mechs"] = arguments.mechs
    if arguments.user is not None:
        options["user"] = arguments.user
    if arguments.password is not None:
        options["password"] = arguments.password
    # Proton's Container already defaults allow_insecure_mechs to True; the
    # flag records the opt-in explicitly rather than enforcing it.
    if arguments.insecure:
        options["allow_insecure_mechs"] = True
    ssl_domain = None
    if arguments.cafile:
        ssl_domain = SSLDomain(SSLDomain.MODE_CLIENT)
        ssl_domain.set_trusted_ca_db(arguments.cafile)
        ssl_domain.set_peer_authentication(SSLDomain.VERIFY_PEER_NAME)
    try:
        connection = BlockingConnection(arguments.url, timeout=10, ssl_domain=ssl_domain, **options)
    except Exception as error:  # refused authentication surfaces here
        print(json.dumps({"connected": False, "error": type(error).__name__}), flush=True)
        return 3
    try:
        if arguments.operation == "send":
            body = Path(arguments.path).read_bytes()
            sender = connection.create_sender(arguments.address, options=AtLeastOnce())
            sender.send(Message(body=body), timeout=10)
            print(json.dumps({"sent_bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}), flush=True)
        else:
            receiver = connection.create_receiver(arguments.address, credit=1, options=AtLeastOnce())
            print(json.dumps({"ready": True}), flush=True)
            message = receiver.receive(timeout=10)
            body = bytes(message.body)
            receiver.accept()
            print(json.dumps({"received_bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}), flush=True)
    except Exception as error:
        print(json.dumps({"failed": type(error).__name__}), flush=True)
        return 4
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
