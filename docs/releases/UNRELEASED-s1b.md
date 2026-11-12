# Unreleased: S1b broker TLS on `hell`/`ex_ssl`

Upgrade note for the next release that contains S1b. No version is chosen and
no release is authorized; a release still requires an explicit owner request
(see [active scope](../ACTIVE_SCOPE.md)). S1b was qualified in hosted CI run
43223448120 and Tests run 43223447801 for `375f677`; see
[ADR 0009](../adr/0009-owned-network-stack.md) and the
[capability matrix](../CAPABILITIES.md).

## Upgrade notes (breaking)

- **OTP `:ssl` is no longer used or started by the broker.** MQTT and AMQP
  TLS listeners accept through `hell` with `ex_ssl`. The runtime declares
  `extra_applications: [:crypto]` instead of `[:ssl]`, so a host no longer
  gets OTP `:ssl` started by the runtime. Hosts that use OTP `:ssl`
  themselves must start it in their own application list.
- **Broker TLS is TLS 1.3 only.** TLS 1.2 and older clients are refused during
  the handshake. The client library is unchanged (it still offers TLS 1.3 and
  1.2 by default).
- **TLS options are allowlisted.** Options outside the documented list fail
  start with `{:unsupported_tls_option, key}`. Previously MQTT accepted
  `secure_renegotiate`, `reuse_sessions` and `honor_cipher_order`, and AMQP
  passed any OTP `:ssl` option (for example `verify_fun`, `partial_chain`,
  `crl_check`, TLS 1.2 `versions`, `depth: 0`). A malformed value now fails
  with `{:invalid_tls_option, key}` instead of the generic error. In-memory
  private keys (`key`) are accepted. `active_chunk_size` is set by the runtime
  (16,384 bytes) and rejected in user `tls_options`. See
  [MQTT_BROKER.md](../MQTT_BROKER.md) and [AMQP10_BROKER.md](../AMQP10_BROKER.md).
- **No session resumption:** every connection performs a full handshake.
- **No client-certificate revocation checking.**
- **A TLS listener failure now closes that listener's live connections**, as
  for TCP listeners since S1a. Previously TLS connections survived it. The
  protocol owner, queues, sessions and retained messages still survive.
- **TLS handshake failures are no longer reported to the protocol owner.**
  They close only that socket; no Connection is started.
- **Handshake bound.** `connect_timeout` bounds the TLS handshake, and the
  CONNECT/protocol-header deadline starts after it, so a client gets at most
  2 × `connect_timeout` for TLS plus CONNECT/header. Concurrent TLS
  connections per listener, handshakes included, are bounded at
  `max_connections + 3`.
- **Per-connection TLS input:** unchanged input limits. The MQTT
  `max_packet_size + 16 KiB` and AMQP `max_input_bytes` bounds keep their
  plain-TCP semantics, because each TLS delivery is at most 16 KiB.
