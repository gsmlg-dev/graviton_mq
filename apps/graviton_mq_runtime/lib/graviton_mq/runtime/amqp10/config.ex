defmodule GravitonMQ.Runtime.AMQP10.Config do
  @moduledoc false

  alias GravitonMQ.AMQP10.{Endpoint, SessionEngine}
  alias GravitonMQ.Core.NodeId
  alias GravitonMQ.Queue.Machine

  @keys ~w(container_id queues queue_options listeners auth authorize profile max_connections
    max_auth_jobs auth_timeout send_timeout connect_timeout max_input_bytes
    max_input_frames_per_turn max_pump_actions endpoint_options persistence)a
  @endpoint_keys ~w(hostname max_frame_size channel_max max_sessions max_pending_operations
    max_pending_controls max_write_frames max_write_bytes control_reserve_frames
    control_reserve_bytes operation_timeout_ms close_timeout_ms idle_timeout_ms session_options)a
  @queue_keys ~w(max_messages max_bytes max_message_bytes max_consumers max_inflight max_batch max_mutation_bytes)a
  @listener_keys ~w(id transport ip port backlog tls_options)a
  @owned_socket_options ~w(active mode packet header packet_size deliver send_timeout send_timeout_close)a

  def build(options) when is_list(options) do
    with true <- keyword?(options, @keys),
         :ok <- authentication(options),
         {:ok, config} <- assemble(options),
         {:ok, listeners} <- listeners(Keyword.get(options, :listeners, [])) do
      {:ok, %{config | listeners: listeners}}
    else
      false -> {:error, :invalid_options}
      {:error, _} = error -> error
    end
  end

  def build(_options), do: {:error, :invalid_options}

  defp authentication(options) do
    case Keyword.get(options, :auth) do
      :anonymous -> :ok
      nil -> {:error, :explicit_auth_required}
      _ -> {:error, :unsupported_authentication}
    end
  end

  defp assemble(options) do
    container = Keyword.get(options, :container_id)
    queues = Keyword.get(options, :queues, [])
    queue_options = Keyword.get(options, :queue_options, [])
    endpoint = Keyword.get(options, :endpoint_options, [])

    persistence = Keyword.get(options, :persistence)

    config = %{
      persistence: nil,
      profile: Keyword.get(options, :profile, if(persistence, do: :local_durable, else: :memory)),
      container_id: container,
      queues: queues,
      queue_options: queue_options,
      endpoint_options: endpoint,
      listeners: [],
      authorize: Keyword.get(options, :authorize, fn _, _, _ -> :allow end),
      max_connections: Keyword.get(options, :max_connections, 64),
      max_auth_jobs: Keyword.get(options, :max_auth_jobs, 16),
      auth_timeout: Keyword.get(options, :auth_timeout, 2000),
      send_timeout: Keyword.get(options, :send_timeout, 1000),
      connect_timeout: Keyword.get(options, :connect_timeout, 2000),
      max_input_bytes: Keyword.get(options, :max_input_bytes, 1_048_576),
      max_input_frames_per_turn: Keyword.get(options, :max_input_frames_per_turn, 32),
      max_pump_actions: Keyword.get(options, :max_pump_actions, 16)
    }

    valid =
      config.profile in [:memory, :local_durable] and
        ((config.profile == :memory and is_nil(persistence)) or
           (config.profile == :local_durable and not is_nil(persistence))) and string?(container) and
        is_list(queues) and length(queues) in 1..32 and
        Enum.all?(queues, &string?/1) and length(Enum.uniq(queues)) == length(queues) and
        keyword?(queue_options, @queue_keys) and keyword?(endpoint, @endpoint_keys) and
        valid_session_options?(Keyword.get(endpoint, :session_options, [])) and
        valid_credit_limits?(queue_options, endpoint) and
        is_function(config.authorize, 3) and
        bounded?(config.max_connections, 1, 1024) and bounded?(config.max_auth_jobs, 1, 128) and
        bounded?(config.auth_timeout, 1, 3000) and bounded?(config.send_timeout, 1, 3000) and
        bounded?(config.connect_timeout, 1, 3000) and
        bounded?(config.max_input_bytes, 65_536, 16_777_216) and
        bounded?(config.max_input_frames_per_turn, 1, 1024) and
        bounded?(config.max_pump_actions, 1, 1024)

    if valid do
      endpoint = endpoint |> Keyword.put(:container_id, container) |> Keyword.put(:role, :broker)

      session_options = Keyword.get(endpoint, :session_options, [])

      maximum =
        Enum.min([
          Keyword.get(session_options, :max_message_bytes, 1_048_576),
          Keyword.get(queue_options, :max_message_bytes, 1_048_576),
          Keyword.get(queue_options, :max_bytes, 67_108_864)
        ])

      endpoint =
        Keyword.put(
          endpoint,
          :session_options,
          Keyword.put(session_options, :max_message_bytes, maximum)
        )

      session =
        endpoint |> Keyword.get(:session_options, []) |> Keyword.put(:peer_attach_policy, :defer)

      with {:ok, persistence} <- persistence(persistence),
           {:ok, machine} <- Endpoint.new(endpoint),
           true <- machine.limits.max_frame_size <= config.max_input_bytes,
           {:ok, _session} <- SessionEngine.new(0, 0, session),
           {:ok, _queue} <-
             Machine.new(
               NodeId.new("configuration-validation"),
               Keyword.put(queue_options, :incarnation, "configuration-validation")
             ) do
        {:ok, %{config | endpoint_options: endpoint, persistence: persistence}}
      else
        false -> {:error, :input_buffer_below_frame_limit}
        {:error, _} = error -> error
      end
    else
      {:error, :invalid_options}
    end
  end

  defp persistence(nil), do: {:ok, nil}

  defp persistence(options) do
    keys = [:directory, :stream_id, :max_record_bytes, :max_log_bytes, :fault_injector]

    if keyword?(options, keys) and length(options) == map_size(Map.new(options)) do
      config = %{
        directory: Keyword.get(options, :directory),
        stream_id: Keyword.get(options, :stream_id),
        max_record_bytes: Keyword.get(options, :max_record_bytes, 2_097_152),
        max_log_bytes: Keyword.get(options, :max_log_bytes, 1_073_741_824),
        fault_injector: Keyword.get(options, :fault_injector)
      }

      if is_binary(config.directory) and Path.type(config.directory) == :absolute and
           is_binary(config.stream_id) and byte_size(config.stream_id) in 1..255 and
           bounded?(config.max_record_bytes, 1024, 67_108_864) and
           bounded?(config.max_log_bytes, config.max_record_bytes, 9_223_372_036_854_775_807) and
           (is_nil(config.fault_injector) or is_function(config.fault_injector, 2)),
         do: {:ok, config},
         else: {:error, :invalid_persistence}
    else
      {:error, :invalid_persistence}
    end
  end

  defp listeners(options) when is_list(options) and length(options) <= 16 do
    options
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {options, index}, {:ok, listeners} ->
      case listener(options, index) do
        {:ok, listener} ->
          if Enum.any?(listeners, &(&1.id == listener.id)),
            do: {:halt, {:error, :duplicate_listener_identity}},
            else: {:cont, {:ok, [listener | listeners]}}

        error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, listeners} -> {:ok, Enum.reverse(listeners)}
      error -> error
    end
  end

  defp listeners(_options), do: {:error, :invalid_listeners}

  defp listener(options, index) do
    if keyword?(options, @listener_keys) do
      listener = %{
        id: Keyword.get(options, :id, index),
        transport: Keyword.get(options, :transport, :tcp),
        ip: Keyword.get(options, :ip, {127, 0, 0, 1}),
        port: Keyword.get(options, :port, 0),
        backlog: Keyword.get(options, :backlog, 128),
        tls_options: Keyword.get(options, :tls_options, [])
      }

      tls = listener.tls_options

      valid =
        listener.transport in [:tcp, :tls] and valid_ip?(listener.ip) and
          bounded?(listener.port, 0, 65_535) and bounded?(listener.backlog, 1, 1024) and
          is_list(tls) and Keyword.keyword?(tls) and
          Enum.all?(tls, fn {key, _} -> key not in @owned_socket_options end) and
          (listener.transport == :tcp or (certificate?(tls) and private_key?(tls)))

      if valid, do: {:ok, listener}, else: {:error, :invalid_listener}
    else
      {:error, :invalid_listener}
    end
  end

  defp valid_session_options?(options),
    do:
      is_list(options) and Keyword.keyword?(options) and
        not Keyword.has_key?(options, :remote_channel) and
        not Keyword.has_key?(options, :peer_attach_policy)

  defp valid_credit_limits?(queue, endpoint) do
    session = Keyword.get(endpoint, :session_options, [])

    Enum.all?(
      [
        Keyword.get(session, :max_message_bytes, 1_048_576),
        Keyword.get(queue, :max_message_bytes, 1_048_576),
        Keyword.get(queue, :max_bytes, 67_108_864)
      ],
      &(is_integer(&1) and &1 > 0)
    )
  end

  defp keyword?(options, allowed),
    do:
      is_list(options) and Keyword.keyword?(options) and
        Enum.all?(Keyword.keys(options), &(&1 in allowed))

  defp string?(value),
    do:
      is_binary(value) and byte_size(value) in 1..255 and String.valid?(value) and
        not String.contains?(value, <<0>>)

  defp bounded?(value, minimum, maximum),
    do: is_integer(value) and value >= minimum and value <= maximum

  defp valid_ip?(ip) when is_tuple(ip) and tuple_size(ip) in [4, 8] do
    maximum = if tuple_size(ip) == 4, do: 255, else: 65_535
    ip |> Tuple.to_list() |> Enum.all?(&bounded?(&1, 0, maximum))
  end

  defp valid_ip?(_ip), do: false

  defp certificate?(options),
    do: Keyword.has_key?(options, :certfile) or Keyword.has_key?(options, :cert)

  defp private_key?(options),
    do: Keyword.has_key?(options, :keyfile) or Keyword.has_key?(options, :key)
end
