defmodule GravitonMQ.MQTTClient do
  @moduledoc """
  Host-supervised MQTT 5.0 network client, independent of a local broker.

  `publish/4`, `subscribe/3` and `unsubscribe/2` return local admission
  `{:ok, reference}`. The calling process receives
  `{:graviton_mq, client, {:operation, reference, result}}` when the socket
  accepts QoS0 (`{:ok, :sent}`), the peer acknowledges QoS1 (`{:ok, :puback}`),
  the peer completes QoS2 (`{:ok, :pubcomp}`), or subscription acknowledgements
  arrive (`{:ok, reason_codes}`). Negative PUBREC/PUBCOMP preserve their reason
  as `{:error, {:pubrec, code}}` or `{:error, {:pubcomp, code}}`. A possible
  send followed by timeout or lost session reports `{:error, :unknown_outcome}`.
  No result promises subscriber processing. With the default memory profile,
  no result promises disk durability.

  The configured `:observer` receives connection events and publications.
  `request/2` grants finite delivery demand. Each delivery is
  `{:graviton_mq, client, {:delivery, delivery_ref, publish_packet}}`;
  `ack/2` releases local ownership and sends PUBACK for QoS1 or PUBREC for
  QoS2. The QoS2 session ledger remains until the PUBREL/PUBCOMP exchange;
  duplicate packets do not create another local delivery. QoS2 is an MQTT
  protocol guarantee, not business exactly-once processing. The client
  stops when its observer dies. Session state survives transport reconnects
  while this owner lives.

  Opt in to local Linux durability with `persistence: [directory: absolute_path,
  max_bytes: 8_388_608]` and a nonempty stable `:client_id`. The parent directory
  must exist. One client or broker owner holds the shared directory inode lock;
  the two roles must use distinct directories. Every retained protocol
  transition is synchronized before its wire or host acceptance effects. Store
  failures close the connection and stop the owner with an explicit
  `{:storage_error, {:failed | :unknown, reason}}` observer event.

  Persistent admission also sends the caller `{:operation_identity, reference,
  {store_uuid, integer}}`. After owner/VM recovery the new observer receives
  `{:recovered_operation, stable_id, kind}`, terminal
  `{:recovered_operation_result, stable_id, result}`, and previously owned
  `{:recovered_delivery, {new_client_pid, delivery_id}, packet, qos2_phase}`
  events. Ordinary delivery events are not repeated for previously delivered
  ownership. These explicit recovery events can repeat after another crash;
  they do not promise business exactly-once processing. Credentials and caller
  PIDs/references are supplied afresh and never enter checkpoints. Elapsed
  deadlines include wall-clock downtime; clock rollback explicitly rejects recovery.
  """

  alias GravitonMQ.Client.MQTT

  @doc false
  def start_link(options) when is_function(options, 0), do: start_link(options.())

  def start_link(options) when is_list(options) do
    if Keyword.keyword?(options),
      do: MQTT.start_link(Keyword.put_new(options, :observer, self())),
      else: {:error, :invalid_options}
  end

  def start_link(_options), do: {:error, :invalid_options}

  def child_spec(options) do
    identity =
      Keyword.get(options, :name) ||
        {Keyword.get(options, :host, "localhost"), Keyword.get(options, :port, 1883),
         Keyword.get(options, :client_id, "")}

    # OTP supervisor crash reports include start arguments. Keep credentials
    # inside an opaque function rather than exposing the options in its MFA.
    %{
      id: Keyword.get(options, :id, {__MODULE__, identity}),
      start: {__MODULE__, :start_link, [fn -> options end]},
      restart: :transient,
      type: :worker
    }
  end

  def publish(client, topic, payload, options \\ []),
    do: GenServer.call(client, {:publish, topic, payload, options})

  def subscribe(client, filter, options \\ []),
    do: GenServer.call(client, {:subscribe, filter, options})

  def unsubscribe(client, filter), do: GenServer.call(client, {:unsubscribe, filter})
  def request(client, demand), do: GenServer.call(client, {:demand, demand})
  def ack(client, delivery_ref), do: GenServer.call(client, {:ack, delivery_ref})
  def disconnect(client), do: GenServer.call(client, :disconnect)
  def status(client), do: GenServer.call(client, :status)
end
