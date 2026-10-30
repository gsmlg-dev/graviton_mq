"""Fixed-version independent MQTT5 peer; never imported by production code."""

import argparse
from importlib.metadata import version
from threading import Event

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from paho.mqtt.reasoncodes import ReasonCode


def normal_will(port):
    connected, disconnected = Event(), Event()
    results = {}
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                         client_id="paho-normal-will", protocol=mqtt.MQTTv5)

    def on_connect(_client, _userdata, _flags, reason, _properties):
        results["connect"] = reason.value
        connected.set()

    def on_disconnect(_client, _userdata, _flags, reason, _properties):
        results["disconnect"] = reason.value
        disconnected.set()

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    will = Properties(PacketTypes.WILLMESSAGE)
    will.WillDelayInterval = 1
    client.will_set("will/normal", b"must-not-publish", qos=1, properties=will)
    properties = Properties(PacketTypes.CONNECT)
    properties.SessionExpiryInterval = 10
    client.connect("127.0.0.1", port, keepalive=5, properties=properties)
    client.loop_start()
    try:
        assert connected.wait(3), "missing CONNACK"
        assert results["connect"] == 0, results
        publication = client.publish("irrelevant/value", b"normal", qos=1)
        publication.wait_for_publish(3)
        assert publication.is_published(), "missing PUBACK"
        assert client.disconnect() == mqtt.MQTT_ERR_SUCCESS
        assert disconnected.wait(3), "DISCONNECT was not transmitted"
        assert results["disconnect"] == 0, results
        print("Paho 2.1.0: CONNACK, PUBACK, DISCONNECT completed", flush=True)
    finally:
        client.disconnect()
        client.loop_stop()


def nonzero_will(port, reason):
    connected, disconnected = Event(), Event()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                         client_id=f"paho-error-will-{reason}", protocol=mqtt.MQTTv5,
                         reconnect_on_failure=False)
    client.on_connect = lambda _c, _u, _f, code, _p: connected.set() if code.value == 0 else None
    client.on_disconnect = lambda *_args: disconnected.set()
    will = Properties(PacketTypes.WILLMESSAGE)
    will.WillDelayInterval = 1
    client.will_set(f"will/nonzero/{reason}", b"delayed", qos=1, properties=will)
    properties = Properties(PacketTypes.CONNECT)
    properties.SessionExpiryInterval = 10
    client.connect("127.0.0.1", port, keepalive=5, properties=properties)
    client.loop_start()
    try:
        assert connected.wait(3), "missing successful CONNACK"
        assert client.disconnect(ReasonCode(PacketTypes.DISCONNECT, identifier=reason)) == mqtt.MQTT_ERR_SUCCESS
        assert disconnected.wait(3), "DISCONNECT was not transmitted"
        print(f"Paho DISCONNECT {reason} transmitted", flush=True)
    finally:
        client.disconnect()
        client.loop_stop()


def shared_unavailable(port):
    disconnected = Event()
    results = {}

    class ObservedClient(mqtt.Client):
        def _handle_disconnect(self):
            # Paho 2.1.0 parses the reason only for Remaining Length > 2.
            # Observe the complete peer-received bytes as well as its callback;
            # a legal compact DISCONNECT must not be mistaken for reason zero.
            results["wire_disconnect"] = bytes(self._in_packet["packet"]).hex()
            return super()._handle_disconnect()

    client = ObservedClient(mqtt.CallbackAPIVersion.VERSION2,
                            client_id="paho-shared-off", protocol=mqtt.MQTTv5,
                            reconnect_on_failure=False)

    def on_connect(peer, _userdata, _flags, reason, properties):
        results["connect"] = reason.value
        results["available"] = properties.SharedSubscriptionAvailable
        results["subscribe"] = peer.subscribe("$share/g/a", qos=1)[0]

    def on_disconnect(_peer, _userdata, flags, reason, _properties):
        results["disconnect"] = reason.value
        results["from_server"] = flags.is_disconnect_packet_from_server
        disconnected.set()

    client.on_connect = on_connect
    client.on_subscribe = lambda *_args: results.update(suback=True)
    client.on_disconnect = on_disconnect
    client.connect("127.0.0.1", port, keepalive=5)
    client.loop_start()
    try:
        assert disconnected.wait(3), "server did not disconnect forbidden shared subscription"
        assert results == {"connect": 0, "available": 0, "subscribe": 0,
                           "wire_disconnect": "9e", "disconnect": 0,
                           "from_server": True}, results
        print("Paho shared subscription rejected by DISCONNECT 0x9E", flush=True)
    finally:
        client.disconnect()
        client.loop_stop()


if __name__ == "__main__":
    assert version("paho-mqtt") == "2.1.0", "peer requires paho-mqtt==2.1.0"
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["normal-will", "will-4", "will-128", "shared-off"])
    parser.add_argument("port", type=int)
    arguments = parser.parse_args()
    if arguments.mode == "normal-will":
        normal_will(arguments.port)
    elif arguments.mode == "shared-off":
        shared_unavailable(arguments.port)
    else:
        nonzero_will(arguments.port, int(arguments.mode.split("-")[1]))
