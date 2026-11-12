defmodule GravitonMQ.Client.AMQP10.Config do
  @moduledoc false

  alias GravitonMQ.AMQP10.Codec.Limits
  alias GravitonMQ.AMQP10.SASL.{Anonymous, Machine, Plain}
  alias GravitonMQ.Client.Transport

  @endpoint_keys ~w(hostname max_frame_size channel_max max_sessions max_pending_operations max_pending_controls max_write_frames max_write_bytes
    control_reserve_frames control_reserve_bytes operation_timeout_ms close_timeout_ms
    idle_timeout_ms session_options)a
  @keys [
          :id,
          :name,
          :host,
          :port,
          :transport,
          :tls_options,
          :observer,
          :container_id,
          :connect_timeout,
          :send_timeout,
          :max_operations,
          :max_input_frames_per_turn,
          :max_input_bytes,
          :sasl,
          :allow_insecure_plain,
          :sasl_timeout,
          :reconnect
        ] ++ @endpoint_keys

  def build(options) when is_list(options) do
    if Keyword.keyword?(options) and Enum.all?(Keyword.keys(options), &(&1 in @keys)),
      do: assemble(options),
      else: {:error, :invalid_options}
  end

  def build(_options), do: {:error, :invalid_options}

  defp assemble(options) do
    transport = Keyword.get(options, :transport, :tcp)
    container_id = Keyword.get(options, :container_id)

    config = %{
      host: Keyword.get(options, :host, "localhost"),
      port: Keyword.get(options, :port, if(transport == :tls, do: 5671, else: 5672)),
      transport: transport,
      tls_options: Keyword.get(options, :tls_options, []),
      observer: Keyword.get(options, :observer),
      name: Keyword.get(options, :name),
      connect_timeout: Keyword.get(options, :connect_timeout, 2_000),
      send_timeout: Keyword.get(options, :send_timeout, 1_000),
      max_operations: Keyword.get(options, :max_operations, 64),
      max_input_bytes: Keyword.get(options, :max_input_bytes, 1_048_576),
      max_input_frames_per_turn: Keyword.get(options, :max_input_frames_per_turn, 32),
      allow_insecure_plain: Keyword.get(options, :allow_insecure_plain, false),
      sasl_timeout: Keyword.get(options, :sasl_timeout, 5_000)
    }

    frame_size = Keyword.get(options, :max_frame_size, 65_536)
    sasl = sasl(Keyword.get(options, :sasl, :none))
    reconnect = reconnect(Keyword.get(options, :reconnect, []))

    valid =
      transport in [:tcp, :tls] and valid_host?(config.host) and
        is_integer(config.port) and config.port in 1..65_535 and
        is_binary(container_id) and container_id != "" and String.valid?(container_id) and
        is_pid(config.observer) and valid_name?(config.name) and
        is_integer(frame_size) and frame_size in 512..16_777_216 and
        is_integer(config.max_operations) and config.max_operations in 1..65_536 and
        is_integer(config.max_input_bytes) and config.max_input_bytes in 65_536..16_777_216 and
        config.max_input_bytes >= frame_size and
        is_integer(config.max_input_frames_per_turn) and
        config.max_input_frames_per_turn in 1..1_024 and
        is_integer(config.connect_timeout) and config.connect_timeout in 1..3_000 and
        is_integer(config.send_timeout) and config.send_timeout in 1..3_000 and
        Transport.valid_tls_options?(config.tls_options) and
        Keyword.get(config.tls_options, :verify, :verify_peer) == :verify_peer and
        Keyword.get(config.tls_options, :server_name_indication) != :disable and
        valid_session_options?(Keyword.get(options, :session_options, [])) and
        is_boolean(config.allow_insecure_plain) and
        is_integer(config.sasl_timeout) and config.sasl_timeout in 1..10_000 and
        sasl != :invalid and plain_transport?(sasl, transport, config.allow_insecure_plain) and
        encodable?(sasl, Keyword.get(options, :hostname), config.sasl_timeout) and
        reconnect != :invalid

    if valid do
      endpoint =
        options
        |> Keyword.take(@endpoint_keys)
        |> Keyword.put(:container_id, container_id)
        |> Keyword.put(:max_frame_size, frame_size)
        |> Keyword.put_new(:max_pending_operations, config.max_operations)

      {:ok,
       config
       |> Map.put(:endpoint_options, endpoint)
       |> Map.put(:limits, %Limits{max_frame_size: frame_size})
       |> Map.put(:sasl, sasl)
       |> Map.put(:reconnect, reconnect)}
    else
      {:error, :invalid_options}
    end
  end

  # Normalized forms: :none | {:anonymous, trace | nil} |
  # {:plain, username, password | (-> password), authzid | nil}. The
  # credential (or provider) stays in redacted owner state for the connection
  # lifetime so a later reconnect can re-authenticate (owner Q11). A provider
  # runs in the owner's `handle_continue` before the SASL deadline is armed,
  # so it must return promptly; nothing here bounds it.
  defp sasl(:none), do: :none
  defp sasl(:anonymous), do: {:anonymous, nil}

  defp sasl({:anonymous, trace}) do
    case Anonymous.parse(trace) do
      {:ok, trace} -> {:anonymous, trace}
      {:error, _reason} -> :invalid
    end
  end

  defp sasl({:plain, username, password}), do: sasl({:plain, username, password, []})

  defp sasl({:plain, username, password, options}) when is_list(options) do
    authzid =
      if Keyword.keyword?(options) and Keyword.keys(options) -- [:authzid] == [],
        do: Keyword.get(options, :authzid),
        else: :invalid

    probe = if is_function(password, 0), do: "provided", else: password

    if authzid != :invalid and match?({:ok, _}, Plain.new(username, probe, authzid)),
      do: {:plain, username, password, authzid},
      else: :invalid
  end

  defp sasl(_sasl), do: :invalid

  # Mirrors the MQTT client's reconnect policy (owner Q3): 0..100 attempts
  # (0 = one connection, the historical behaviour), exponential delay from
  # `delay` capped at `max_delay`, no jitter, no infinity. Unlike the MQTT
  # config, an invalid policy is reported as the AMQP config's single
  # `{:error, :invalid_options}`.
  defp reconnect(options) when is_list(options) do
    if Keyword.keyword?(options) and
         Enum.all?(Keyword.keys(options), &(&1 in [:max_attempts, :delay, :max_delay])) do
      policy = %{
        max_attempts: Keyword.get(options, :max_attempts, 0),
        delay: Keyword.get(options, :delay, 250),
        max_delay: Keyword.get(options, :max_delay, 5_000)
      }

      if is_integer(policy.max_attempts) and policy.max_attempts in 0..100 and
           is_integer(policy.delay) and policy.delay in 1..60_000 and
           is_integer(policy.max_delay) and policy.max_delay in policy.delay..60_000,
         do: policy,
         else: :invalid
    else
      :invalid
    end
  end

  defp reconnect(_options), do: :invalid

  defp plain_transport?({:plain, _, _, _}, :tcp, allow_insecure_plain),
    do: allow_insecure_plain == true

  defp plain_transport?(_sasl, _transport, _allow_insecure_plain), do: true

  # A SASL frame is at most 512 octets; RFC 4616 permits longer messages.
  defp encodable?({:plain, username, password, authzid}, hostname, timeout)
       when is_binary(password) do
    with {:ok, plain} <- Plain.new(username, password, authzid),
         {:ok, _machine} <-
           Machine.new(role: :client, credentials: plain, hostname: hostname, timeout_ms: timeout) do
      true
    else
      _invalid -> false
    end
  end

  defp encodable?(_sasl, _hostname, _timeout), do: true

  defp valid_session_options?(options), do: is_list(options) and Keyword.keyword?(options)

  defp valid_host?(host) when is_binary(host),
    do: host != "" and String.valid?(host) and not String.contains?(host, <<0>>)

  defp valid_host?(host) when is_list(host),
    do: host != [] and Enum.all?(host, &(is_integer(&1) and &1 in 0..0x10FFFF))

  defp valid_host?(host) when is_tuple(host) and tuple_size(host) == 4,
    do: host |> Tuple.to_list() |> Enum.all?(&(is_integer(&1) and &1 in 0..255))

  defp valid_host?(host) when is_tuple(host) and tuple_size(host) == 8,
    do: host |> Tuple.to_list() |> Enum.all?(&(is_integer(&1) and &1 in 0..65_535))

  defp valid_host?(_host), do: false
  defp valid_name?(nil), do: true
  defp valid_name?(name) when is_atom(name), do: true
  defp valid_name?({:global, _name}), do: true
  defp valid_name?({:via, module, _name}) when is_atom(module), do: true
  defp valid_name?(_name), do: false
end
