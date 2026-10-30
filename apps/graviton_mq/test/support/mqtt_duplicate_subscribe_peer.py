"""Pinned independent MQTT5 peer exercising ordered duplicate SUBSCRIBE filters."""

import argparse
from importlib.metadata import version
from queue import Queue
from threading import Event

import paho.mqtt.client as mqtt
from paho.mqtt.subscribeoptions import SubscribeOptions


def check(port, first, second):
    connected, subscribed, disconnected = Event(), Event(), Event()
    messages = Queue()
    results = {}
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                         client_id=f"paho-duplicate-{first}-{second}",
                         protocol=mqtt.MQTTv5, reconnect_on_failure=False)

    def on_connect(_peer, _userdata, _flags, reason, _properties):
        results["connect"] = reason.value
        connected.set()

    def on_subscribe(_peer, _userdata, _mid, reasons, _properties):
        results["grants"] = [reason.value for reason in reasons]
        subscribed.set()

    def on_message(_peer, _userdata, message):
        messages.put((message.payload, message.qos, message.retain))

    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    client.on_disconnect = lambda *_args: disconnected.set()
    topic = f"duplicate/{first}/{second}"
    client.connect("127.0.0.1", port, keepalive=5)
    client.loop_start()
    try:
        assert connected.wait(3) and results["connect"] == 0, results
        publication = client.publish(topic, b"retained", qos=1, retain=True)
        publication.wait_for_publish(3)
        assert publication.is_published(), "missing retained PUBACK"
        result, _mid = client.subscribe([
            (topic, SubscribeOptions(qos=1, retainHandling=first)),
            (topic, SubscribeOptions(qos=0, retainHandling=second)),
        ])
        assert result == mqtt.MQTT_ERR_SUCCESS
        assert subscribed.wait(3) and results["grants"] == [1, 0], results
        # The live publication is an ordered barrier after any retained replay.
        # It also proves the final duplicate's QoS0 options govern later traffic.
        barrier = client.publish(topic, b"barrier", qos=1)
        barrier.wait_for_publish(3)
        assert barrier.is_published(), "missing barrier PUBACK"
        received = []
        while not received or received[-1][0] != b"barrier":
            received.append(messages.get(timeout=3))
        expected = [(b"barrier", 0, False)]
        if first == 1:
            expected.insert(0, (b"retained", 1, True))
        assert received == expected, (received, expected)
        assert messages.empty(), list(messages.queue)
        assert client.disconnect() == mqtt.MQTT_ERR_SUCCESS
        assert disconnected.wait(3), "DISCONNECT was not transmitted"
        print(f"PASS Paho2.1.0 ordered RH{first},RH{second}: {received}", flush=True)
    finally:
        client.disconnect()
        client.loop_stop()


if __name__ == "__main__":
    assert version("paho-mqtt") == "2.1.0", "peer requires paho-mqtt==2.1.0"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("port", type=int)
    parser.add_argument("first", type=int, choices=[1, 2])
    parser.add_argument("second", type=int, choices=[1, 2])
    arguments = parser.parse_args()
    assert arguments.first != arguments.second
    check(arguments.port, arguments.first, arguments.second)
