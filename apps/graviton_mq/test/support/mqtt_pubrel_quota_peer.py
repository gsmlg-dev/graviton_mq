"""Pinned Paho peer: resumed PUBREL must progress behind unacknowledged QoS1.

Paho performs the MQTT handshakes. Manual host ACK deliberately retains two
QoS1 deliveries and one received QoS2 message before a transport cut. There is
no reordered PUBREC or invalid peer packet in this scenario.
"""
import argparse
import json
import struct
import time
from importlib.metadata import version

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties


class Peer(mqtt.Client):
    def __init__(self):
        super().__init__(mqtt.CallbackAPIVersion.VERSION2, client_id="paho-pubrel-quota",
                         protocol=mqtt.MQTTv5, manual_ack=True)
        self.connections, self.releases = [], []
        self.messages, self.acked = {}, set()
        self.cut = False
        self.qos2_id = None
        self.on_connect = self.connected
        self.on_subscribe = self.subscribed
        self.on_message = self.message

    def connected(self, _client, _data, flags, reason, _properties):
        assert reason.value == 0, reason
        self.connections.append(flags.session_present)
        assert self.connections == [False] or self.connections == [False, True], self.connections
        if len(self.connections) == 1:
            assert self.subscribe("qos2/quota/#", qos=2)[0] == mqtt.MQTT_ERR_SUCCESS

    def subscribed(self, _client, _data, _mid, reasons, _properties):
        assert [reason.value for reason in reasons] == [2], reasons
        print("SUBSCRIBED", flush=True)

    def message(self, _client, _data, message):
        expected = {"qos2/quota/one": (b"one", 1), "qos2/quota/two": (b"two", 1),
                    "qos2/quota/three": (b"three", 2)}
        assert (bytes(message.payload), message.qos) == expected[message.topic]
        if len(self.connections) == 1:
            assert message.mid not in self.messages, message.mid
            self.messages[message.mid] = (message.topic, bytes(message.payload), message.qos)
            if message.qos == 2:
                self.qos2_id = message.mid
        else:
            assert message.qos == 1 and message.dup, message
            assert self.messages[message.mid] == (message.topic, bytes(message.payload), message.qos)
            # No QoS1 ACK is allowed to unlock quota before the resumed PUBREL.
            assert self.releases == [(1, self.qos2_id), (2, self.qos2_id)], self.releases
            assert self.ack(message.mid, message.qos) == mqtt.MQTT_ERR_SUCCESS
            self.acked.add(message.mid)

    def _handle_pubrel(self):
        mid, = struct.unpack("!H", self._in_packet["packet"][:2])
        self.releases.append((len(self.connections), mid))
        result = super()._handle_pubrel()
        assert result == mqtt.MQTT_ERR_SUCCESS, result
        if len(self.connections) == 1:
            assert len(self.messages) == 3 and mid == self.qos2_id, self.messages
            self._sock_close()
            self.cut = True
            return mqtt.MQTT_ERR_CONN_LOST
        assert mid == self.qos2_id, (mid, self.qos2_id)
        return self.ack(mid, 2)


def run(port):
    assert version("paho-mqtt") == "2.1.0"
    peer = Peer()
    properties = Properties(PacketTypes.CONNECT)
    properties.SessionExpiryInterval = 60
    properties.ReceiveMaximum = 3
    peer.connect("127.0.0.1", port, keepalive=5,
                 clean_start=mqtt.MQTT_CLEAN_START_FIRST_ONLY, properties=properties)
    deadline = time.monotonic() + 10
    try:
        while time.monotonic() < deadline:
            result = peer.loop(timeout=0.02)
            if peer.cut:
                peer.cut = False
                properties.ReceiveMaximum = 1
                time.sleep(0.05)
                assert peer.reconnect() == mqtt.MQTT_ERR_SUCCESS
            else:
                assert result == mqtt.MQTT_ERR_SUCCESS, result
            if len(peer.acked) == 2:
                for _ in range(5):
                    assert peer.loop(timeout=0.02) == mqtt.MQTT_ERR_SUCCESS
                assert len(peer.messages) == 3, peer.messages
                print("PASS " + json.dumps({"connections": peer.connections,
                      "releases": peer.releases, "qos1_acks": len(peer.acked),
                      "unique_messages": len(peer.messages), "resumed_receive_maximum": 1}), flush=True)
                return
        raise AssertionError(("resumed PUBREL stranded behind QoS1 quota", peer.releases, peer.acked))
    finally:
        peer.disconnect()
        peer.loop(timeout=0.02)
        peer._sock_close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("port", type=int)
    run(parser.parse_args().port)
