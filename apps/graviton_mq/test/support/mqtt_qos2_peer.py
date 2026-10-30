"""Paho 2.1.0 MQTT5 peer with one deliberate transport loss, test-only."""
import argparse
import json
import time
from importlib.metadata import version

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties


class Peer(mqtt.Client):
    def __init__(self, mode):
        super().__init__(mqtt.CallbackAPIVersion.VERSION2,
                         client_id="paho-qos2-" + mode, protocol=mqtt.MQTTv5)
        self.mode, self.lost, self.losses = mode, False, 0
        self.connections, self.received, self.sent, self.completed = [], [], [], False
        self.published = None
        self.reason = None
        self.on_connect = self.connected
        self.on_subscribe = self.subscribed
        self.on_message = self.message
        self.on_publish = self.finished
        self.on_log = self.log

    def cut(self, phase):
        self.losses += 1
        self.lost = True
        print("CUT " + phase, flush=True)
        self._sock_close()
        return mqtt.MQTT_ERR_CONN_LOST

    def connected(self, _client, _data, flags, reason, _properties):
        assert reason.value == 0, reason
        self.connections.append(flags.session_present)
        assert flags.session_present == (len(self.connections) > 1), self.connections
        print("CONNECTED " + json.dumps(self.connections), flush=True)
        if len(self.connections) == 1:
            if self.mode.startswith("send-"):
                self.published = self.publish("qos2/value", b"once", qos=2)
            else:
                assert self.subscribe("qos2/value", qos=2)[0] == mqtt.MQTT_ERR_SUCCESS

    def subscribed(self, _client, _data, _mid, reasons, _properties):
        assert [reason.value for reason in reasons] == [2], reasons
        print("SUBSCRIBED", flush=True)

    def message(self, _client, _data, message):
        self.received.append((message.mid, bytes(message.payload), message.qos))
        assert len(self.received) == 1, self.received
        assert message.payload == b"once" and message.qos == 2
        print("DELIVERED " + str(message.mid), flush=True)

    def finished(self, _client, _data, _mid, reason, _properties):
        expected = 0x92 if self.mode == "send-before-pubcomp" else 0
        assert reason.value == expected, (reason, expected)
        self.reason = reason.value
        self.completed = True

    def log(self, _client, _data, _level, message):
        if "PUBLISH" in message or "PUBREC" in message or "PUBREL" in message or "PUBCOMP" in message:
            print(message, flush=True)

    def _handle_pubrec(self):
        if self.mode == "send-before-pubrec" and not self.losses:
            return self.cut("before-pubrec")
        return super()._handle_pubrec()

    def _send_pubrel(self, mid):
        if self.mode == "send-before-pubrel" and not self.losses:
            return self.cut("before-pubrel")
        self.sent.append(("pubrel", mid))
        return super()._send_pubrel(mid)

    def _handle_pubackcomp(self, command):
        if command == "PUBCOMP" and self.mode == "send-before-pubcomp" and not self.losses:
            return self.cut("before-pubcomp")
        return super()._handle_pubackcomp(command)

    def _send_pubrec(self, mid):
        if self.mode == "receive-before-pubrec" and not self.losses:
            return self.cut("before-pubrec")
        return super()._send_pubrec(mid)

    def _send_pubcomp(self, mid):
        if self.mode == "receive-before-pubcomp" and not self.losses:
            return self.cut("before-pubcomp")
        self.sent.append(("pubcomp", mid))
        return super()._send_pubcomp(mid)


def run(port, mode):
    assert version("paho-mqtt") == "2.1.0"
    peer = Peer(mode)
    properties = Properties(PacketTypes.CONNECT)
    properties.SessionExpiryInterval = 60
    properties.ReceiveMaximum = 1
    peer.connect("127.0.0.1", port, keepalive=5,
                 clean_start=mqtt.MQTT_CLEAN_START_FIRST_ONLY, properties=properties)
    deadline = time.monotonic() + 10
    try:
        while time.monotonic() < deadline:
            rc = peer.loop(timeout=0.02)
            if peer.lost:
                peer.lost = False
                time.sleep(0.05)
                assert peer.reconnect() == mqtt.MQTT_ERR_SUCCESS
            elif rc != mqtt.MQTT_ERR_SUCCESS:
                raise AssertionError(("unexpected peer failure", rc))
            done = peer.completed if mode.startswith("send-") else peer.received and any(item[0] == "pubcomp" for item in peer.sent)
            if len(peer.connections) == 2 and done:
                for _ in range(5):
                    assert peer.loop(timeout=0.02) == mqtt.MQTT_ERR_SUCCESS
                print("PASS " + json.dumps({"mode": mode, "connections": peer.connections,
                      "losses": peer.losses, "received": len(peer.received), "reason": peer.reason,
                      "acknowledgements": peer.sent}), flush=True)
                return
        raise AssertionError("QoS2 recovery deadline exceeded")
    finally:
        peer.disconnect()
        peer.loop(timeout=0.02)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["send-before-pubrec", "send-before-pubrel", "send-before-pubcomp", "receive-before-pubrec", "receive-before-pubcomp"])
    parser.add_argument("port", type=int)
    args = parser.parse_args()
    run(args.port, args.mode)
