"""Pinned Paho MQTT5 metadata publisher/receiver; test infrastructure only."""

import argparse
import json
import time
from importlib.metadata import version

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties


def properties():
    result = Properties(PacketTypes.PUBLISH)
    result.PayloadFormatIndicator = 1
    result.ContentType = "application/x-graviton; charset=utf-8"
    result.ResponseTopic = "metadata/replies/雪"
    result.CorrelationData = bytes([0, 255, 128, 0, 42, 126])
    result.UserProperty = [("repeat", "first"), ("other", "雪"),
                           ("repeat", "second"), ("repeat", "first")]
    result.MessageExpiryInterval = 60
    return result


def validate(message, prefix, retained, expiry_upper):
    qos = int(message.topic.rsplit("/", 1)[1][1:])
    assert qos in (1, 2), message.topic
    assert message.topic == f"{prefix}/q{qos}", message.topic
    assert message.qos == qos, (message.qos, qos)
    assert bytes(message.payload) == f"metadata-{qos}-雪".encode(), message.payload
    assert message.retain is retained, (message.retain, retained)
    actual = message.properties
    assert actual.PayloadFormatIndicator == 1, actual
    assert actual.ContentType == "application/x-graviton; charset=utf-8", actual
    assert actual.ResponseTopic == "metadata/replies/雪", actual
    assert actual.CorrelationData == bytes([0, 255, 128, 0, 42, 126]), actual
    assert list(actual.UserProperty) == [
        ("repeat", "first"), ("other", "雪"),
        ("repeat", "second"), ("repeat", "first")], actual
    assert 1 <= actual.MessageExpiryInterval <= expiry_upper, actual
    return {"topic": message.topic, "qos": qos, "retain": message.retain,
            "expiry": actual.MessageExpiryInterval,
            "correlation_hex": actual.CorrelationData.hex(),
            "user_properties": actual.UserProperty}


def run(arguments):
    assert version("paho-mqtt") == "2.1.0"
    peer = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                       client_id=f"metadata-{arguments.mode}-{time.monotonic_ns()}",
                       protocol=mqtt.MQTTv5, reconnect_on_failure=False)
    state = {"connected": False, "published": [], "received": []}

    def connected(client, _data, flags, reason, _properties):
        assert reason.value == 0 and not flags.session_present, (flags, reason)
        state["connected"] = True
        if arguments.mode == "publish":
            for qos in (1, 2):
                info = client.publish(f"{arguments.prefix}/q{qos}",
                                      f"metadata-{qos}-雪".encode(), qos=qos,
                                      retain=arguments.retained, properties=properties())
                assert info.rc == mqtt.MQTT_ERR_SUCCESS, info.rc
        else:
            assert client.subscribe(arguments.prefix + "/#", qos=2)[0] == mqtt.MQTT_ERR_SUCCESS

    def subscribed(_client, _data, _mid, reasons, _properties):
        assert [reason.value for reason in reasons] == [2], reasons
        print("READY", flush=True)

    def published(_client, _data, mid, reason, _properties):
        assert reason.value == 0, reason
        state["published"].append(mid)

    def received(_client, _data, message):
        entry = validate(message, arguments.prefix, arguments.retained, arguments.expiry_upper)
        assert entry["topic"] not in [item["topic"] for item in state["received"]], state
        state["received"].append(entry)

    peer.on_connect, peer.on_subscribe = connected, subscribed
    peer.on_publish, peer.on_message = published, received
    peer.connect("127.0.0.1", arguments.port, keepalive=10)
    deadline = time.monotonic() + 8
    try:
        while time.monotonic() < deadline:
            assert peer.loop(timeout=0.02) == mqtt.MQTT_ERR_SUCCESS
            result = state["published"] if arguments.mode == "publish" else state["received"]
            if len(result) == 2:
                # Flush the real QoS2 PUBCOMP before ending the connection.
                for _ in range(5):
                    assert peer.loop(timeout=0.02) == mqtt.MQTT_ERR_SUCCESS
                assert len(result) == 2, state
                print("PASS " + json.dumps(state, sort_keys=True), flush=True)
                return
        raise AssertionError(("metadata peer deadline", state))
    finally:
        peer.disconnect()
        peer.loop(timeout=0.02)
        peer._sock_close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["publish", "receive"])
    parser.add_argument("port", type=int)
    parser.add_argument("prefix")
    parser.add_argument("--retained", action="store_true")
    parser.add_argument("--expiry-upper", type=int, default=60)
    run(parser.parse_args())
