"""Fixed-version independent MQTT5 TLS peer (Paho 2.1.0); never imported by production code."""

import argparse
import ssl
import time
from threading import Event

import paho.mqtt.client as mqtt


def context(ca, cert=None, key=None, tls12_only=False):
    tls = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca)
    if tls12_only:
        tls.maximum_version = ssl.TLSVersion.TLSv1_2
    if cert:
        tls.load_cert_chain(cert, key)
    return tls


def round_trip(port, ca, cert, key, hold):
    connected, received = Event(), Event()
    results = {}
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="paho-tls", protocol=mqtt.MQTTv5)
    client.tls_set_context(context(ca, cert, key))

    def on_connect(_client, _userdata, _flags, reason, _properties):
        results["connect"] = reason.value
        connected.set()

    def on_message(_client, _userdata, message):
        results["payload"] = message.payload
        received.set()

    client.on_connect = on_connect
    client.on_message = on_message
    client.connect("localhost", port, keepalive=5)
    client.loop_start()
    try:
        assert connected.wait(5), "missing CONNACK"
        assert results["connect"] == 0, results
        client.subscribe("tls/paho/#", qos=1)
        time.sleep(0.2)
        client.publish("tls/paho/value", b"over-tls", qos=1).wait_for_publish(5)
        assert received.wait(5), "missing round-trip PUBLISH"
        assert results["payload"] == b"over-tls", results
        print(f"Paho TLS round trip PASS cipher={client.socket().cipher()} openssl={ssl.OPENSSL_VERSION}", flush=True)
        time.sleep(hold)
    finally:
        client.disconnect()
        client.loop_stop()


def tls12_only(port, ca):
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="paho-tls12", protocol=mqtt.MQTTv5,
                         reconnect_on_failure=False)
    client.tls_set_context(context(ca, tls12_only=True))
    try:
        client.connect("localhost", port, keepalive=5)
    except ssl.SSLError as error:
        print(f"TLS1.2-only refused: {error.reason} openssl={ssl.OPENSSL_VERSION}", flush=True)
        return
    raise AssertionError("a TLS1.2-only Paho client connected to a TLS1.3-only broker")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["round-trip", "tls12-only"])
    parser.add_argument("port", type=int)
    parser.add_argument("ca")
    parser.add_argument("--cert")
    parser.add_argument("--key")
    parser.add_argument("--hold", type=float, default=1.0)
    arguments = parser.parse_args()
    if arguments.mode == "round-trip":
        round_trip(arguments.port, arguments.ca, arguments.cert, arguments.key, arguments.hold)
    else:
        tls12_only(arguments.port, arguments.ca)
