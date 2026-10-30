"""Pinned independent Paho MQTT5 peer for broker persistence qualification."""
import argparse
import json
import time
from importlib.metadata import version

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties


def run(port, mode):
    assert version("paho-mqtt") == "2.1.0"
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                         client_id="g6-offline", protocol=mqtt.MQTTv5)
    state = {"ready": False, "done": False, "messages": [], "present": None}
    seed = mode.startswith("seed")
    expected = ([["qos2/value", "once", 2]] if mode.endswith("qos2") else
                [["persist/one", "q1", 1], ["persist/two", "q2", 2]])

    def connected(peer, _data, flags, reason, _properties):
        assert reason.value == 0, reason
        state["present"] = flags.session_present
        assert flags.session_present == (not seed), (mode, flags)
        if seed:
            topic = "qos2/#" if mode.endswith("qos2") else "persist/#"
            assert peer.subscribe(topic, qos=2)[0] == mqtt.MQTT_ERR_SUCCESS
        else:
            state["ready"] = True

    def subscribed(peer, _data, _mid, reasons, _properties):
        assert [r.value for r in reasons] == [2], reasons
        state["ready"] = True
        peer.disconnect()

    def message(_peer, _data, publication):
        state["messages"].append([publication.topic, publication.payload.decode(), publication.qos])
        assert len(state["messages"]) <= len(expected), state

    def disconnected(_peer, _data, _flags, reason, _properties):
        assert reason.value == 0, reason
        state["done"] = True

    client.on_connect, client.on_subscribe = connected, subscribed
    client.on_message, client.on_disconnect = message, disconnected
    properties = Properties(PacketTypes.CONNECT)
    properties.SessionExpiryInterval = 60
    client.connect("127.0.0.1", port, keepalive=10, clean_start=seed, properties=properties)
    deadline = time.monotonic() + 8
    try:
        while time.monotonic() < deadline and not state["done"]:
            result = client.loop(timeout=0.02)
            assert result in (mqtt.MQTT_ERR_SUCCESS, mqtt.MQTT_ERR_NO_CONN), result
            if not seed and len(state["messages"]) == len(expected):
                # Complete both wire handshakes before an orderly disconnect.
                for _ in range(5):
                    assert client.loop(timeout=0.02) == mqtt.MQTT_ERR_SUCCESS
                assert sorted(state["messages"]) == expected, state
                client.disconnect()
        assert state["ready"] and state["done"], state
        print("PASS " + json.dumps(state, sort_keys=True), flush=True)
    finally:
        client._sock_close()


def expiry(port, mode):
    assert version("paho-mqtt") == "2.1.0"
    seed = mode == "seed-expiry"
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                         client_id="g6-expiry" if seed else "g6-check", protocol=mqtt.MQTTv5)
    state = {"published": False, "messages": [], "subscribed": False}
    if seed:
        will = Properties(PacketTypes.WILLMESSAGE)
        will.WillDelayInterval = 1
        will.MessageExpiryInterval = 10
        client.will_set("will/state", b"gone", qos=1, retain=True, properties=will)

    def connected(peer, _data, _flags, reason, _properties):
        assert reason.value == 0, reason
        if seed:
            publication = Properties(PacketTypes.PUBLISH)
            publication.MessageExpiryInterval = 1
            peer.publish("expire/state", b"short", qos=1, retain=True, properties=publication)
        else:
            peer.subscribe([("expire/#", 1), ("will/#", 1)])

    def published(_peer, _data, _mid, reason, _properties):
        assert reason.value == 0, reason
        state["published"] = True

    def subscribed(_peer, _data, _mid, reasons, _properties):
        assert [r.value for r in reasons] == [1, 1], reasons
        state["subscribed"] = True

    def message(_peer, _data, publication):
        state["messages"].append([publication.topic, publication.payload.decode(), publication.retain])

    client.on_connect, client.on_publish = connected, published
    client.on_subscribe, client.on_message = subscribed, message
    properties = Properties(PacketTypes.CONNECT)
    properties.SessionExpiryInterval = 2 if seed else 0
    client.connect("127.0.0.1", port, keepalive=10, clean_start=True, properties=properties)
    deadline = time.monotonic() + 8
    try:
        while time.monotonic() < deadline:
            assert client.loop(timeout=0.02) == mqtt.MQTT_ERR_SUCCESS
            if seed and state["published"]:
                client._sock_close()  # No DISCONNECT: retain the scheduled Will.
                print("PASS expiry seeded", flush=True)
                return
            if not seed and state["subscribed"] and state["messages"]:
                for _ in range(10):
                    assert client.loop(timeout=0.02) == mqtt.MQTT_ERR_SUCCESS
                assert state["messages"] == [["will/state", "gone", True]], state
                client.disconnect()
                print("PASS " + json.dumps(state, sort_keys=True), flush=True)
                return
        raise AssertionError(("expiry peer timed out", state))
    finally:
        client._sock_close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["seed", "receive", "seed-qos2", "receive-qos2", "seed-expiry", "check-expiry"])
    parser.add_argument("port", type=int)
    args = parser.parse_args()
    if args.mode.endswith("expiry"):
        expiry(args.port, args.mode)
    else:
        run(args.port, args.mode)
