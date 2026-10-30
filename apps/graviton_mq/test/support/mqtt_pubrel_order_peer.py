"""Paho 2.1.0: two unfinished PUBRELs retain order across a transport cut.

Paho sends PUBREC in received PUBLISH order, as MQTT-4.6.0-3 requires. The
first connection deliberately stops before processing either PUBREL, retaining
Paho's incoming messages. The resumed connection reduces Receive Maximum to
one; both earlier PUBRELs must still progress, in order, without a new PUBLISH.
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
        super().__init__(mqtt.CallbackAPIVersion.VERSION2,
                         client_id="paho-pubrel-order", protocol=mqtt.MQTTv5,
                         manual_ack=True)
        self.connections, self.publications = [], []
        self.releases, self.deliveries, self.acked = [[], []], [], []
        self.cut = False
        self.on_connect = self.connected
        self.on_subscribe = self.subscribed
        self.on_message = self.message

    def connected(self, _client, _data, flags, reason, _properties):
        assert reason.value == 0, reason
        self.connections.append(flags.session_present)
        assert self.connections in ([False], [False, True]), self.connections
        if len(self.connections) == 1:
            assert self.subscribe("qos2/order", qos=2)[0] == mqtt.MQTT_ERR_SUCCESS

    def subscribed(self, _client, _data, _mid, reasons, _properties):
        assert [reason.value for reason in reasons] == [2], reasons
        print("SUBSCRIBED", flush=True)

    def _send_pubrec(self, mid):
        assert self.connections == [False], "PUBLISH repeated after positive PUBREC"
        assert mid not in self.publications and len(self.publications) < 2
        self.publications.append(mid)
        return super()._send_pubrec(mid)

    def _handle_pubrel(self):
        mid, = struct.unpack("!H", self._in_packet["packet"][:2])
        connection = len(self.connections) - 1
        self.releases[connection].append(mid)
        assert self.releases[connection] == self.publications[:len(self.releases[connection])], (
            self.publications, self.releases)
        if connection == 0:
            # Do not pop Paho's message or send PUBCOMP before the cut.
            if len(self.releases[0]) == 2:
                self._sock_close()
                self.cut = True
                return mqtt.MQTT_ERR_CONN_LOST
            return mqtt.MQTT_ERR_SUCCESS
        result = super()._handle_pubrel()
        assert result == mqtt.MQTT_ERR_SUCCESS, result
        self.acked.append(mid)
        return self.ack(mid, 2)

    def message(self, _client, _data, message):
        assert self.connections == [False, True], self.connections
        assert message.topic == "qos2/order" and message.qos == 2
        assert message.mid == self.publications[len(self.deliveries)]
        self.deliveries.append(bytes(message.payload).decode("ascii"))
        assert self.deliveries == ["first", "second"][:len(self.deliveries)]


def run(port):
    assert version("paho-mqtt") == "2.1.0"
    peer = Peer()
    properties = Properties(PacketTypes.CONNECT)
    properties.SessionExpiryInterval = 60
    properties.ReceiveMaximum = 2
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
                assert peer.releases == [peer.publications, peer.publications]
                assert peer.deliveries == ["first", "second"]
                print("PASS " + json.dumps({"connections": peer.connections,
                      "releases": peer.releases, "packet_ids": peer.publications,
                      "delivered": peer.deliveries, "pubcomp": len(peer.acked),
                      "resumed_receive_maximum": 1}), flush=True)
                return
        raise AssertionError(("ordered PUBREL resume deadline", peer.releases, peer.acked))
    finally:
        peer.disconnect()
        peer.loop(timeout=0.02)
        peer._sock_close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("port", type=int)
    run(parser.parse_args().port)
