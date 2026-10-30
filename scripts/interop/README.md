# Independent peer qualification

These peers are test tools. They are never production dependencies or substitutes
for a GravitonMQ client or broker. Use the Elixir/OTP pair in `.tool-versions`,
Python 3.12+, Java 21, CMake, a C/C++ compiler, pkg-config, OpenSSL, libffi and
cJSON development headers. The local durability profile requires Linux.

`prepare.py` downloads checksum-pinned Mosquitto 2.1.2 and Artemis 2.41.0,
builds Mosquitto with TLS and installs hash-pinned Paho 2.1.0 and Qpid Proton
0.40.0 in a separate virtual environment. Proton must have TLS support.
Its CFFI build prerequisites are installed before its source build.

```bash
python scripts/interop/prepare.py --directory /tmp/graviton-peers
python scripts/interop/run.py --directory /tmp/graviton-peers \
  --logs /tmp/graviton-tests -- mix test
elixir scripts/stage_hex.exs /tmp/graviton-hex
python scripts/interop/run.py --directory /tmp/graviton-peers \
  --logs /tmp/graviton-packages -- elixir scripts/build_hex.exs /tmp/graviton-hex
python scripts/interop/run.py --directory /tmp/graviton-peers \
  --logs /tmp/graviton-soak-peers -- mix run --no-start scripts/interop/soak.exs \
  --duration 300 --rate 10 --payload-bytes 2048 --logs /tmp/graviton-soak
```

Use a fresh staging directory. The package check builds seven archives and
three consumers from their unpacked contents. It covers default standalone
startup, host-supervised embedding, and client-only installation without
broker/runtime/storage packages. Additional installed checks verify MQTT
durable-through and retained recovery, AMQP durable Proton enqueue/replay/
retirement, and client-owned QoS2 delivery recovery through Mosquitto.

The bounded soak runs all four paths concurrently, one outstanding round trip
per role, alternating MQTT QoS1/2 and explicitly settling AMQP deliveries.
It checks exact payload order/bytes, counted progress, VM/peer resources and
drained queues/connections before cleanup. Duration is 5–3600 seconds, rate
1–50 per role per second and payload16–16384 bytes. This exercises memory/TCP
profiles; it does not establish durability, TLS or unbounded-load behavior.
The CI duration is five minutes; longer local results retain their exact
duration and limits in the qualification record.

Omitting the command from `run.py` performs the startup probe. It validates
the real AMQP header over TCP and verified TLS, restricts peer listeners to loopback, chooses temporary
ports and records configuration, versions, command exit and cleanup in
`evidence.json`. Both temporary brokers are stopped and the generated Artemis
instance is removed on command success or failure. The explicit peer cache
and evidence directory remain available for inspection.

Artemis uses a temporary PKCS12 server key and a one-day test CA. The wrapper
exports `ARTEMIS_TLS_PORT`, `ARTEMIS_TLS_CA`, and `ARTEMIS_TLS_UNTRUSTED_CA` for
the client TLS tests. Those tests exchange real traffic in both directions
with independent Proton peers and reject an unrelated CA and wrong hostname.
Only public certificates are retained as evidence; private keys are deleted
with the temporary instance.

`prepare.py --provided` validates explicitly supplied `GRAVITON_MOSQUITTO_BIN`,
`GRAVITON_PAHO_PYTHON`, `GRAVITON_PROTON_PYTHON`, `GRAVITON_ARTEMIS_HOME` and
`JAVA_HOME` paths. It still checks versions and Proton TLS support. Nix hosts
may need their compiler/pkg-config/library paths configured; the Ubuntu CI job
installs the corresponding development packages.

The workflow definition is a runnable qualification path. Local success does
not establish a hosted Actions result or publication. See
[the support matrix](../../docs/CAPABILITIES.md) for the checks actually run.
