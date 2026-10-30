"""Run a command with all four independent-peer paths enabled, then clean up."""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import socket
import ssl
import subprocess
import tempfile
import time

HEADER = b"AMQP\x00\x01\x00\x00"


def tls_material(directory, log):
    """One-day test credentials, confined to the temporary peer instance."""
    ca, key = directory / "ca.pem", directory / "ca.key"
    certificate, private_key = directory / "localhost.pem", directory / "localhost.key"
    untrusted = directory / "untrusted.pem"
    extensions = directory / "localhost.ext"
    extensions.write_text("basicConstraints=critical,CA:FALSE\nsubjectAltName=DNS:localhost\n"
                          "keyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n")
    commands = [
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-subj", "/CN=Graviton Interop CA",
         "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign", "-keyout", key, "-out", ca],
        ["openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=localhost", "-keyout", private_key,
         "-out", directory / "localhost.csr"],
        ["openssl", "x509", "-req", "-in", directory / "localhost.csr", "-CA", ca, "-CAkey", key, "-set_serial", "1",
         "-days", "1", "-extfile", extensions, "-out", certificate],
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-subj", "/CN=Untrusted Interop CA",
         "-addext", "basicConstraints=critical,CA:TRUE", "-keyout", directory / "untrusted.key", "-out", untrusted],
        ["openssl", "pkcs12", "-export", "-inkey", private_key, "-in", certificate, "-certfile", ca,
         "-name", "localhost", "-passout", "pass:ephemeral-test-only", "-out", directory / "server.p12"]
    ]
    for command in commands:
        subprocess.run([str(arg) for arg in command], stdout=log, stderr=subprocess.STDOUT, check=True, timeout=30)
    for secret in [key, private_key, directory / "untrusted.key", directory / "server.p12"]:
        secret.chmod(0o600)
    return ca, untrusted


def probe(port, context=None):
    with socket.create_connection(("127.0.0.1", port), timeout=5) as connection:
        peer = context.wrap_socket(connection, server_hostname="localhost") if context else connection
        try:
            peer.sendall(HEADER)
            response = bytearray()
            while len(response) < len(HEADER):
                part = peer.recv(len(HEADER) - len(response))
                if not part:
                    raise RuntimeError("Artemis closed AMQP negotiation")
                response.extend(part)
            if response != HEADER:
                raise ValueError("Artemis AMQP version mismatch")
        finally:
            if context:
                peer.close()


def listeners(pid):
    inodes = set()
    for fd in Path(f"/proc/{pid}/fd").iterdir():
        try:
            target = os.readlink(fd)
            if target.startswith("socket:["):
                inodes.add(target[8:-1])
        except FileNotFoundError:
            pass
    result = []
    for name in ["tcp", "tcp6"]:
        for row in Path(f"/proc/{pid}/net/{name}").read_text().splitlines()[1:]:
            fields = row.split()
            if fields[3] == "0A" and fields[9] in inodes:
                result.append((fields[1].split(":")[0], int(fields[1].split(":")[1], 16)))
    return result


def stop(process):
    if process is not None and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--logs", type=Path, required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    env = dict(os.environ, **json.loads((args.directory / "environment.json").read_text()))
    args.logs.mkdir(parents=True, exist_ok=True)
    artemis = Path(env["GRAVITON_ARTEMIS_HOME"]) / "bin/artemis"
    instance = Path(tempfile.mkdtemp(prefix="graviton-interop-"))
    amqp, mqtt = None, None
    evidence = {"artemis": "2.41.0", "mosquitto": "2.1.2", "paho": "2.1.0", "proton": "0.40.0", "command": command}
    env["JAVA_ARGS"] = "-Xms64m -Xmx256m -XX:ActiveProcessorCount=2 -Djava.net.preferIPv4Stack=true -Dlog4j2.disableJmx=true"
    try:
        version = subprocess.check_output([str(artemis), "version"], env=env, text=True, timeout=30)
        if "2.41.0" not in version:
            raise ValueError("unexpected Artemis version")
        with (args.logs / "artemis-create.log").open("w") as log:
            subprocess.run([str(artemis), "create", "--silent", "--force", "--allow-anonymous", "--disable-persistence", "--nio", "--no-fsync",
                            "--no-autotune", "--no-web", "--no-amqp-acceptor", "--no-mqtt-acceptor", "--no-stomp-acceptor", "--no-hornetq-acceptor",
                            "--host", "127.0.0.1", "--default-port", "0", "--name", "graviton-interop-peer", "--user", "graviton-test",
                            "--password", "ephemeral-test-only", str(instance / "artemis")], env=env, stdout=log, stderr=subprocess.STDOUT,
                           check=True, timeout=30)
        config = instance / "artemis/etc/broker.xml"
        with (args.logs / "artemis-tls-create.log").open("w") as log:
            ca, untrusted = tls_material(instance, log)
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            tls_port = reservation.getsockname()[1]
        acceptors = (f'<acceptors>\n<acceptor name="amqp">tcp://127.0.0.1:0?protocols=AMQP;useEpoll=false;amqpCredits=100;amqpLowCredits=30</acceptor>\n'
                     f'<acceptor name="amqp-tls">tcp://127.0.0.1:{tls_port}?protocols=AMQP;useEpoll=false;sslEnabled=true;'
                     f'keyStorePath={instance}/server.p12;keyStoreType=PKCS12;keyStorePassword=ephemeral-test-only;'
                     'amqpCredits=100;amqpLowCredits=30</acceptor>\n</acceptors>')
        contents, count = re.subn(r"<acceptors>.*?</acceptors>", lambda _: acceptors, config.read_text(), flags=re.S)
        if count != 1:
            raise ValueError("Artemis acceptor template changed")
        contents = contents.replace("<persistence-enabled>false</persistence-enabled>", "<persistence-enabled>false</persistence-enabled>\n<security-enabled>false</security-enabled>\n<jmx-management-enabled>false</jmx-management-enabled>")
        config.write_text(contents)
        (args.logs / "artemis-config.xml").write_text(contents)
        with (args.logs / "artemis.log").open("w") as log:
            amqp = subprocess.Popen([str(instance / "artemis/bin/artemis"), "run"], env=env, stdout=log, stderr=subprocess.STDOUT)
            deadline, port = time.monotonic() + 30, None
            while time.monotonic() < deadline:
                if amqp.poll() is not None:
                    raise RuntimeError("Artemis exited; inspect artemis.log")
                found = listeners(amqp.pid)
                if len(found) >= 2:
                    if len(found) != 2 or any(host != "0100007F" for host, _ in found):
                        raise ValueError(f"unexpected Artemis listeners: {found}")
                    plain = [value for _, value in found if value != tls_port]
                    if len(plain) != 1:
                        raise ValueError(f"missing Artemis TLS listener: {found}")
                    port = plain[0]
                    break
                time.sleep(0.05)
            if port is None:
                raise TimeoutError("Artemis listener did not start")
            probe(port)
            probe(tls_port, ssl.create_default_context(cafile=str(ca)))
            env.update(ARTEMIS_HOST="127.0.0.1", ARTEMIS_PORT=str(port), ARTEMIS_INSTANCE=str(instance / "artemis"),
                       ARTEMIS_TLS_PORT=str(tls_port), ARTEMIS_TLS_CA=str(ca), ARTEMIS_TLS_UNTRUSTED_CA=str(untrusted))
            # Certificates are public test evidence; private keys remain temporary.
            shutil.copyfile(ca, args.logs / "artemis-ca.pem")
            shutil.copyfile(instance / "localhost.pem", args.logs / "artemis-server.pem")
            with socket.socket() as reservation:
                reservation.bind(("127.0.0.1", 0))
                mqtt_port = reservation.getsockname()[1]
            mqtt_config = instance / "mosquitto.conf"
            mqtt_config.write_text(f"listener {mqtt_port} 127.0.0.1\nallow_anonymous true\npersistence false\nmax_qos 2\n")
            with (args.logs / "mosquitto.log").open("w") as mqtt_log:
                mqtt = subprocess.Popen([env["GRAVITON_MOSQUITTO_BIN"], "-c", str(mqtt_config)], env=env, stdout=mqtt_log, stderr=subprocess.STDOUT)
                deadline = time.monotonic() + 5
                while True:
                    if mqtt.poll() is not None:
                        raise RuntimeError("Mosquitto exited; inspect mosquitto.log")
                    try:
                        with socket.create_connection(("127.0.0.1", mqtt_port), timeout=0.1):
                            break
                    except OSError:
                        if time.monotonic() >= deadline:
                            raise TimeoutError("Mosquitto listener did not start")
                        time.sleep(0.05)
                env["GRAVITON_MQTT_TEST_PORT"] = str(mqtt_port)
                evidence.update(amqp_port=port, amqp_tls_port=tls_port, mqtt_port=mqtt_port, amqp_pid=amqp.pid, mqtt_pid=mqtt.pid,
                                amqp_tls_verified=True)
                print(json.dumps(evidence, sort_keys=True), flush=True)
                if command:
                    result = subprocess.run(command, env=env)
                    evidence["command_exit_code"] = result.returncode
                    return result.returncode
                return 0
    finally:
        stop(mqtt)
        stop(amqp)
        shutil.rmtree(instance)
        evidence.update(peers_stopped=all(process is None or process.poll() is not None for process in [mqtt, amqp]), instance_removed=not instance.exists())
        (args.logs / "evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
        print("Independent peers stopped and temporary instance removed.", flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
