"""Fixed-version independent MQTT5 peer for S4 features; never imported by production code."""

import argparse
import time
from importlib.metadata import version
from threading import Event, Lock

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from paho.mqtt.subscribeoptions import SubscribeOptions


class ObservedClient(mqtt.Client):
    """Paho 2.1.0 parses a DISCONNECT reason only for Remaining Length > 2; keep the raw body."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.wire_disconnect = None
        self.disconnected = Event()

    def _handle_disconnect(self):
        self.wire_disconnect = bytes(self._in_packet["packet"]).hex()
        self.disconnected.set()
        return super()._handle_disconnect()


class Inbox:
    def __init__(self, peer, auto_ack=False):
        self.lock = Lock()
        self.messages = []
        self.peer = peer
        self.auto_ack = auto_ack
        peer.on_message = self.on_message

    def on_message(self, _client, _userdata, message):
        properties = message.properties
        with self.lock:
            self.messages.append({
                "topic": message.topic,
                "payload": message.payload.decode(),
                "alias": getattr(properties, "TopicAlias", None) if properties else None,
                "ids": sorted(getattr(properties, "SubscriptionIdentifier", []) if properties else []),
                "retain": message.retain,
                "mid": message.mid,
                "qos": message.qos,
            })
        if self.auto_ack:
            self.peer.ack(message.mid, message.qos)

    def wait(self, count, timeout=5):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self.lock:
                if len(self.messages) >= count:
                    return list(self.messages)
            time.sleep(0.02)
        raise AssertionError(f"expected {count} messages, got {self.messages}")


def client(client_id, observed=False, manual_ack=False):
    kind = ObservedClient if observed else mqtt.Client
    return kind(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id, protocol=mqtt.MQTTv5,
                reconnect_on_failure=False, manual_ack=manual_ack)


def connect(peer, port, clean_start=True, **properties):
    connected, result = Event(), {}

    def on_connect(_client, _userdata, _flags, reason, props):
        result["reason"], result["properties"] = reason.value, props
        connected.set()

    peer.on_connect = on_connect
    props = Properties(PacketTypes.CONNECT)
    for name, value in properties.items():
        setattr(props, name, value)
    peer.connect("127.0.0.1", port, keepalive=5, clean_start=clean_start, properties=props)
    peer.loop_start()
    assert connected.wait(3), "missing CONNACK"
    assert result["reason"] == 0, result
    return result["properties"]


def subscribe(peer, topic, qos=1, identifier=None):
    done, granted = Event(), {}

    def on_subscribe(_client, _userdata, _mid, codes, _props):
        granted["codes"] = [code.value for code in codes]
        done.set()

    peer.on_subscribe = on_subscribe
    props = None
    if identifier is not None:
        props = Properties(PacketTypes.SUBSCRIBE)
        props.SubscriptionIdentifier = identifier
    peer.subscribe(topic, options=SubscribeOptions(qos=qos), properties=props)
    assert done.wait(3), "missing SUBACK"
    return granted["codes"]


def publish(peer, topic, payload, qos=1, alias=None, retain=False):
    props = Properties(PacketTypes.PUBLISH)
    if alias is not None:
        props.TopicAlias = alias
    info = peer.publish(topic, payload.encode(), qos=qos, retain=retain, properties=props)
    info.wait_for_publish(3)
    assert info.is_published(), f"publication {payload} not acknowledged"


def expect_disconnect(peer, code):
    assert peer.disconnected.wait(3), "server did not disconnect"
    assert peer.wire_disconnect == f"{code:02x}", peer.wire_disconnect


def stop(*peers):
    for peer in peers:
        peer.disconnect()
        peer.loop_stop()


def probe(_port):
    publisher = client("paho-probe")
    props = Properties(PacketTypes.PUBLISH)
    props.TopicAlias = 1
    publisher.publish("", b"x", qos=0, properties=props)
    subscribe_props = Properties(PacketTypes.SUBSCRIBE)
    subscribe_props.SubscriptionIdentifier = 7
    print("Paho 2.1.0 capability probe passed: manual TopicAlias with empty topic, SubscriptionIdentifier", flush=True)


def alias_inbound(port):
    subscriber = client("paho-alias-sub")
    connect(subscriber, port)
    inbox = Inbox(subscriber)
    subscribe(subscriber, "alias/#", qos=2)
    publisher = client("paho-alias-pub")
    assert connect(publisher, port).TopicAliasMaximum == 10
    for index, qos in enumerate([1, 0, 1, 2]):
        publish(publisher, "alias/t" if index == 0 else "", f"m{index}", qos=qos, alias=1)
    received = inbox.wait(4)
    assert sorted((m["topic"], m["payload"]) for m in received) == [("alias/t", f"m{i}") for i in range(4)], received
    assert all(m["alias"] is None for m in received), received
    stop(subscriber, publisher)
    print("Paho inbound Topic Alias resolved for QoS0/1/2", flush=True)


def alias_invalid(port):
    publisher = client("paho-alias-invalid", observed=True)
    connect(publisher, port)
    props = Properties(PacketTypes.PUBLISH)
    props.TopicAlias = 11
    publisher.publish("alias/x", b"x", qos=0, properties=props)
    expect_disconnect(publisher, 0x94)
    publisher.loop_stop()
    print("Paho alias above Topic Alias Maximum rejected by DISCONNECT 0x94", flush=True)


def alias_unknown(port):
    publisher = client("paho-alias-unknown", observed=True)
    connect(publisher, port)
    props = Properties(PacketTypes.PUBLISH)
    props.TopicAlias = 3
    publisher.publish("", b"x", qos=0, properties=props)
    expect_disconnect(publisher, 0x82)
    publisher.loop_stop()
    print("Paho unknown alias rejected by DISCONNECT 0x82", flush=True)


def alias_reconnect(port):
    first = client("paho-alias-reconnect")
    connect(first, port)
    publish(first, "alias/r", "set", qos=1, alias=1)
    stop(first)
    second = client("paho-alias-reconnect", observed=True)
    connect(second, port)
    props = Properties(PacketTypes.PUBLISH)
    props.TopicAlias = 1
    second.publish("", b"stale", qos=0, properties=props)
    expect_disconnect(second, 0x82)
    second.loop_stop()
    print("Paho alias mapping not carried across Network Connections", flush=True)


def alias_outbound(port, maximum):
    subscriber = client("paho-alias-out-sub")
    connect(subscriber, port, SessionExpiryInterval=60, TopicAliasMaximum=maximum)
    inbox = Inbox(subscriber)
    subscribe(subscriber, "out/#", qos=1)
    publisher = client("paho-alias-out-pub")
    connect(publisher, port)
    topics = [f"out/{index}" for index in range(maximum + 1)]
    for topic in topics:
        publish(publisher, topic, topic)
        publish(publisher, topic, topic)
    received = inbox.wait(2 * len(topics))
    table = {}
    resolved = []
    for message in received:
        if message["alias"] is not None:
            assert 1 <= message["alias"] <= maximum, message
            if message["topic"]:
                table[message["alias"]] = message["topic"]
        resolved.append(message["topic"] or table[message["alias"]])
    assert resolved == [topic for topic in topics for _ in range(2)], resolved
    assert [m["topic"] for m in received[1:2 * maximum:2]] == [""] * maximum, received
    assert all(m["alias"] is None for m in received[-2:]), received
    subscriber.loop_stop()
    subscriber._sock_close()
    again = client("paho-alias-out-sub")
    connect(again, port, clean_start=False, SessionExpiryInterval=60, TopicAliasMaximum=maximum)
    inbox = Inbox(again)
    publish(publisher, topics[0], "after-reconnect")
    message = inbox.wait(1)[0]
    assert message["topic"] == topics[0], message
    stop(again, publisher)
    print(f"Paho outbound aliases bounded by {maximum} and reset after reconnect", flush=True)


def subscription_ids(port):
    subscriber = client("paho-sid-sub")
    connect(subscriber, port)
    inbox = Inbox(subscriber)
    subscribe(subscriber, "sub/#", identifier=7)
    subscribe(subscriber, "sub/+/x", identifier=9)
    publisher = client("paho-sid-pub")
    connect(publisher, port)
    publish(publisher, "sub/a/x", "both")
    assert inbox.wait(1)[0]["ids"] == [7, 9], inbox.messages
    subscribe(subscriber, "sub/+/x")
    publish(publisher, "sub/a/x", "one")
    assert inbox.wait(2)[1]["ids"] == [7], inbox.messages
    publish(publisher, "sub/r/x", "kept", retain=True)
    inbox.wait(3)
    late = client("paho-sid-late")
    connect(late, port)
    late_inbox = Inbox(late)
    subscribe(late, "sub/+/x", identifier=11)
    retained = late_inbox.wait(1)[0]
    assert retained["retain"] and retained["ids"] == [11] and retained["payload"] == "kept", retained
    stop(subscriber, publisher, late)
    print("Paho Subscription Identifiers: overlap, removal and retained", flush=True)


def shared(port, count):
    publisher = client("paho-share-pub")
    connect(publisher, port)
    publish(publisher, "t/r", "retained", retain=True)
    members, inboxes = [], []
    for name in ["paho-share-m1", "paho-share-m2"]:
        member = client(name)
        connect(member, port)
        inboxes.append(Inbox(member))
        assert subscribe(member, "$share/g/t/#") == [1]
        members.append(member)
    time.sleep(0.5)
    assert all(not inbox.messages for inbox in inboxes), "retained message sent on shared subscribe"
    for index in range(count):
        publish(publisher, f"t/{index}", str(index))
    deadline = time.monotonic() + 5
    while sum(len(inbox.messages) for inbox in inboxes) < count and time.monotonic() < deadline:
        time.sleep(0.02)
    payloads = [m["payload"] for inbox in inboxes for m in inbox.messages]
    assert sorted(payloads, key=int) == [str(i) for i in range(count)], payloads
    assert all(inbox.messages for inbox in inboxes), "a member received nothing"
    stop(publisher, *members)
    print(f"Paho shared group delivered {count} messages exactly once across two members", flush=True)


def shared_redistribute(port):
    m1 = client("paho-share-r1", manual_ack=True)
    connect(m1, port, SessionExpiryInterval=60, ReceiveMaximum=1)
    inbox1 = Inbox(m1)
    subscribe(m1, "$share/r/w")
    m2 = client("paho-share-r2", manual_ack=True)
    connect(m2, port, SessionExpiryInterval=60, ReceiveMaximum=1)
    inbox2 = Inbox(m2)
    subscribe(m2, "$share/r/w")
    publisher = client("paho-share-rpub")
    connect(publisher, port)
    for payload in ["1", "2", "3", "4"]:
        publish(publisher, "w", payload)
    first = inbox1.wait(1)[0]
    inbox2.wait(1)
    m1.loop_stop()
    m1._sock_close()
    inbox2.auto_ack = True
    for message in list(inbox2.messages):
        m2.ack(message["mid"], message["qos"])
    received2 = inbox2.wait(3)
    again = client("paho-share-r1", manual_ack=True)
    # Install the inbox before CONNECT: the broker resends the in-flight entry
    # right after CONNACK.
    inbox_again = Inbox(again, auto_ack=True)
    connect(again, port, clean_start=False, SessionExpiryInterval=60, ReceiveMaximum=1)
    resumed = inbox_again.wait(1)[0]
    assert resumed["payload"] == first["payload"], (first, resumed)
    union = sorted({m["payload"] for m in received2} | {resumed["payload"]})
    assert union == ["1", "2", "3", "4"], (received2, resumed)
    stop(again, m2, publisher)
    print("Paho shared QoS1 unsent entry moved to the connected member; sent entry resumed", flush=True)


def shared_no_local(port):
    member = client("paho-share-nolocal", observed=True)
    connect(member, port)
    member.subscribe("$share/g/t", options=SubscribeOptions(qos=1, noLocal=True))
    expect_disconnect(member, 0x82)
    member.loop_stop()
    print("Paho No Local on a shared subscription rejected by DISCONNECT 0x82", flush=True)


MODES = {
    "probe": probe,
    "alias-inbound": alias_inbound,
    "alias-invalid": alias_invalid,
    "alias-unknown": alias_unknown,
    "alias-reconnect": alias_reconnect,
    "subid": subscription_ids,
    "shared-redistribute": shared_redistribute,
    "shared-nolocal": shared_no_local,
}

if __name__ == "__main__":
    assert version("paho-mqtt") == "2.1.0", "peer requires paho-mqtt==2.1.0"
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=sorted(MODES) + ["alias-outbound", "shared"])
    parser.add_argument("port", type=int)
    parser.add_argument("count", type=int, nargs="?", default=0)
    arguments = parser.parse_args()
    if arguments.mode == "alias-outbound":
        alias_outbound(arguments.port, arguments.count)
    elif arguments.mode == "shared":
        shared(arguments.port, arguments.count)
    else:
        MODES[arguments.mode](arguments.port)
