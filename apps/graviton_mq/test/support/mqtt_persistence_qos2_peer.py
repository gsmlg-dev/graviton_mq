"""The independent Paho peer keeps its state across a real broker-owner crash."""
import argparse
import json
import time
from importlib.metadata import version
from pathlib import Path

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from mqtt_qos2_peer import Peer


class CrashPeer(Peer):
    def __init__(self, mode, control):
        super().__init__(mode)
        self.control = Path(control)

    def cut(self, phase):
        self.losses += 1
        self.lost = True
        self._sock_close()
        print("CRASH_POINT " + phase, flush=True)
        deadline = time.monotonic() + 12
        while not self.control.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert self.control.exists(), "broker restart control timed out"
        self._port = int(self.control.read_text())
        return mqtt.MQTT_ERR_CONN_LOST


def run(port, mode, control):
    assert version("paho-mqtt") == "2.1.0"
    peer = CrashPeer(mode, control)
    properties = Properties(PacketTypes.CONNECT)
    properties.SessionExpiryInterval = 60
    properties.ReceiveMaximum = 1
    peer.connect("127.0.0.1", port, keepalive=5,
                 clean_start=mqtt.MQTT_CLEAN_START_FIRST_ONLY, properties=properties)
    deadline = time.monotonic() + 18
    try:
        while time.monotonic() < deadline:
            result = peer.loop(timeout=0.02)
            if peer.lost:
                peer.lost = False
                assert peer.reconnect() == mqtt.MQTT_ERR_SUCCESS
            else:
                assert result == mqtt.MQTT_ERR_SUCCESS, result
            done = peer.completed if mode.startswith("send-") else peer.received and any(item[0] == "pubcomp" for item in peer.sent)
            if done:
                for _ in range(5):
                    assert peer.loop(timeout=0.02) == mqtt.MQTT_ERR_SUCCESS
                assert peer.connections == [False, True], peer.connections
                assert peer.losses == 1
                peer.disconnect()
                print("PASS " + json.dumps({"connections": peer.connections,
                      "received": len(peer.received), "reason": peer.reason}), flush=True)
                return
        raise AssertionError("QoS2 persistence handshake timed out")
    finally:
        peer._sock_close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode")
    parser.add_argument("port", type=int)
    parser.add_argument("control")
    args = parser.parse_args()
    run(args.port, args.mode, args.control)
