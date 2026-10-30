"""Pinned independent peer for soak.exs; one bounded in-flight round trip."""
import argparse
import hashlib
import json
import os
import queue
import resource
import sys
import threading
import time
from importlib.metadata import version
from pathlib import Path


def emit(event):
    print(json.dumps(event, sort_keys=True), flush=True)


def resources():
    rss = next(line.split()[1] for line in Path('/proc/self/status').read_text().splitlines()
               if line.startswith('VmRSS:'))
    result = {'rss_bytes': int(rss) * 1024, 'fds': len(os.listdir('/proc/self/fd')),
              'cpu_seconds': resource.getrusage(resource.RUSAGE_SELF).ru_utime
                             + resource.getrusage(resource.RUSAGE_SELF).ru_stime}
    if result['rss_bytes'] > 134_217_728 or result['fds'] > 32:
        raise RuntimeError(f'independent peer resource bound exceeded: {result}')
    return result


def payload(iteration, size):
    return iteration.to_bytes(8, 'big') + (bytes(range(256)) * ((size + 255) // 256))[:size - 8]


def start_gate(mode, stopped):
    emit({'event': 'ready', 'role': mode, 'os_pid': os.getpid(), 'resources': resources()})
    if sys.stdin.readline().strip() != 'GO':
        raise RuntimeError('soak coordinator did not grant start')

    def control():
        for line in sys.stdin:
            if line.strip() == 'STOP':
                break
        stopped.set()
    threading.Thread(target=control, daemon=True).start()


def exercise(args, roundtrip, stopped):
    start = time.monotonic()
    deadline = start + args.duration
    count, maximum_us, qos1, qos2 = 0, 0, 0, 0
    digest = hashlib.sha256()
    next_sample = start
    while time.monotonic() < deadline and not stopped.is_set():
        before = time.monotonic()
        body = payload(count + 1, args.payload_bytes)
        qos = 1 + count % 2
        roundtrip(body, qos)
        count += 1
        qos1 += qos == 1
        qos2 += qos == 2
        digest.update(body)
        maximum_us = max(maximum_us, int((time.monotonic() - before) * 1_000_000))
        if time.monotonic() >= next_sample:
            emit({'event': 'progress', 'iterations': count, 'resources': resources()})
            next_sample = time.monotonic() + 1
        stopped.wait(max(0, 1 / args.rate - (time.monotonic() - before)))
    if stopped.is_set():
        raise RuntimeError('soak coordinator stopped role before completion')
    if count < max(2, args.duration * args.rate // 4):
        raise RuntimeError(f'insufficient sustained progress: {count} round trips')
    return {'event': 'done', 'iterations': count, 'sent_bytes': count * args.payload_bytes,
            'received_bytes': count * args.payload_bytes, 'sha256': digest.hexdigest(),
            'duration_ms': int((time.monotonic() - start) * 1000), 'maximum_latency_us': maximum_us,
            'qos1': qos1 if args.role == 'mqtt' else 0, 'qos2': qos2 if args.role == 'mqtt' else 0,
            'resources': resources()}


def mqtt_role(args):
    import paho.mqtt.client as mqtt
    if version('paho-mqtt') != '2.1.0':
        raise RuntimeError('expected Paho 2.1.0')
    connected, subscribed, stopped = threading.Event(), threading.Event(), threading.Event()
    messages, failures = queue.Queue(maxsize=4), queue.Queue(maxsize=4)
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f'g7-soak-{os.getpid()}',
                         protocol=mqtt.MQTTv5, manual_ack=True)
    closing = False

    def fail(reason):
        try:
            failures.put_nowait(str(reason))
        except queue.Full:
            pass  # The first explicit failure is sufficient; never treat it as success.

    def check():
        if not failures.empty():
            raise RuntimeError(failures.get_nowait())

    def on_connect(_client, _userdata, _flags, reason, _properties):
        if reason.is_failure:
            fail(f'CONNACK {reason}')
        connected.set()

    def on_subscribe(_client, _userdata, _mid, reasons, _properties):
        if [int(reason.value) for reason in reasons] != [2]:
            fail(f'SUBACK {reasons}')
        subscribed.set()

    def on_message(_client, _userdata, message):
        try:
            messages.put_nowait(message)
        except queue.Full:
            fail('unexpected additional MQTT deliveries exceeded finite queue')

    def on_disconnect(_client, _userdata, _flags, reason, _properties):
        if not closing:
            fail(f'unexpected DISCONNECT {reason}')

    client.on_connect, client.on_subscribe = on_connect, on_subscribe
    client.on_message, client.on_disconnect = on_message, on_disconnect
    client.max_inflight_messages_set(1)
    client.max_queued_messages_set(4)
    try:
        client.connect('127.0.0.1', args.port, keepalive=10)
        client.loop_start()
        if not connected.wait(5):
            raise TimeoutError('Paho CONNECT timeout')
        check()
        result, _mid = client.subscribe(args.address, qos=2)
        if result != mqtt.MQTT_ERR_SUCCESS or not subscribed.wait(5):
            raise TimeoutError('Paho SUBSCRIBE timeout')
        check()
        start_gate('mqtt', stopped)

        def roundtrip(body, qos):
            check()
            publication = client.publish(args.address, body, qos=qos, retain=False)
            publication.wait_for_publish(timeout=5)
            if not publication.is_published() or publication.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError('Paho publication did not complete its QoS handshake')
            message = messages.get(timeout=5)
            if message.topic != args.address or bytes(message.payload) != body or message.qos != qos:
                raise RuntimeError('Paho payload, order, topic or QoS changed')
            if client.ack(message.mid, message.qos) != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError('Paho manual acknowledgement failed')
            check()
        result = exercise(args, roundtrip, stopped)
        if not messages.empty():
            raise RuntimeError('extra MQTT delivery after counted round trips')
        check()
    finally:
        closing = True
        client.disconnect()
        client.loop_stop()
    emit(result)


def amqp_role(args):
    import proton
    from proton import Delivery, Message
    from proton.reactor import AtLeastOnce
    from proton.utils import BlockingConnection
    if proton.VERSION != (0, 40, 0):
        raise RuntimeError('expected Qpid Proton 0.40.0')
    connection = BlockingConnection(f'amqp://127.0.0.1:{args.port}', timeout=5, sasl_enabled=False)
    stopped = threading.Event()
    try:
        receiver = connection.create_receiver(args.address, credit=1, options=AtLeastOnce())
        sender = connection.create_sender(args.address, options=AtLeastOnce())
        start_gate('amqp', stopped)

        def roundtrip(body, _qos):
            delivery = sender.send(Message(body=body), timeout=5)
            if delivery.remote_state != Delivery.ACCEPTED:
                raise RuntimeError(f'Proton publication outcome {delivery.remote_state}')
            message = receiver.receive(timeout=5)
            if bytes(message.body) != body:
                raise RuntimeError('Proton body bytes or message order changed')
            receiver.accept()
        result = exercise(args, roundtrip, stopped)
    finally:
        connection.close()
    emit(result)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('role', choices=['mqtt', 'amqp'])
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--address', required=True)
    parser.add_argument('--duration', type=int, required=True)
    parser.add_argument('--rate', type=int, required=True)
    parser.add_argument('--payload-bytes', type=int, required=True)
    args = parser.parse_args()
    if not (1 <= args.port <= 65535 and 5 <= args.duration <= 3600 and 1 <= args.rate <= 50
            and 16 <= args.payload_bytes <= 16384):
        parser.error('soak bounds: duration5..3600, rate1..50, payload16..16384, valid port')
    (mqtt_role if args.role == 'mqtt' else amqp_role)(args)


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        emit({'event': 'error', 'error': f'{type(error).__name__}: {error}'})
        raise SystemExit(1)
