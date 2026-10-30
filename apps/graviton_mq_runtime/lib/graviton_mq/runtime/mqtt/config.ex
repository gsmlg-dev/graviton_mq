defmodule GravitonMQ.Runtime.MQTT.Config do
  @moduledoc false

  alias GravitonMQ.MQTT.Codec.Limits

  @machine_keys ~w(max_connections max_sessions max_subscriptions max_pending_deliveries
    max_pending_bytes max_retained max_retained_bytes max_wills max_will_bytes
    max_prepared max_prepared_bytes max_inflight maximum_qos max_packet_size receive_maximum
    max_host_subscriptions max_host_outstanding max_demand auth_timeout max_session_bytes
    max_subscription_bytes max_offline_deliveries max_offline_bytes max_writes max_write_bytes
    max_incoming_bytes dispatch_retry_interval)a
  @keys @machine_keys ++ ~w(versions listeners auth host_principal auth_concurrency
    tick_interval connect_timeout send_timeout max_writer_packets max_writer_bytes persistence)a
  @persistence_keys ~w(directory stream_id max_snapshot_bytes max_log_bytes fault_injector)a
  @listener_keys ~w(id transport ip port tls_options backlog)a
  @tls_keys ~w(certfile keyfile password cacertfile cacerts certs_keys versions verify
    fail_if_no_peer_cert depth ciphers secure_renegotiate reuse_sessions honor_cipher_order)a

  def build(options) do
    with true <- keyword?(options, @keys),
         true <- Keyword.get(options, :versions, [:v5]) == [:v5],
         true <- valid_policy?(Keyword.get(options, :auth)),
         true <- valid_machine_limits?(options),
         {:ok, persistence} <- persistence(Keyword.get(options, :persistence)),
         {:ok, listeners} <- listeners(Keyword.get(options, :listeners, [])) do
      config = %{
        auth: Keyword.fetch!(options, :auth),
        host_principal: Keyword.get(options, :host_principal),
        persistence: persistence,
        listeners: listeners,
        limits: %Limits{max_packet_size: Keyword.get(options, :max_packet_size, 1_048_576)},
        machine_options:
          options |> Keyword.take(@machine_keys) |> Keyword.put_new(:auth_timeout, 1_000),
        auth_concurrency: Keyword.get(options, :auth_concurrency, 8),
        auth_timeout: Keyword.get(options, :auth_timeout, 1_000),
        tick_interval: Keyword.get(options, :tick_interval, 100),
        connect_timeout: Keyword.get(options, :connect_timeout, 3_000),
        send_timeout: Keyword.get(options, :send_timeout, 1_000),
        max_writer_packets: Keyword.get(options, :max_writer_packets, 64),
        max_writer_bytes: Keyword.get(options, :max_writer_bytes, 2_097_152)
      }

      if Enum.all?(
           [
             config.auth_timeout,
             config.tick_interval,
             config.connect_timeout,
             config.send_timeout
           ],
           &(is_integer(&1) and &1 in 1..3_000)
         ) and
           is_integer(config.auth_concurrency) and config.auth_concurrency in 1..128 and
           is_integer(config.max_writer_packets) and config.max_writer_packets > 0 and
           is_integer(config.max_writer_bytes) and config.max_writer_bytes > 0,
         do: {:ok, config},
         else: invalid()
    else
      _invalid -> invalid()
    end
  end

  defp valid_policy?(:anonymous), do: true

  defp valid_policy?({module, options}) when is_atom(module) and is_list(options) do
    Keyword.keyword?(options) and Code.ensure_loaded?(module) and
      function_exported?(module, :authenticate, 2) and function_exported?(module, :authorize, 4)
  end

  defp valid_policy?(_policy), do: false

  defp valid_machine_limits?(options) do
    Enum.all?(@machine_keys, fn key ->
      case Keyword.fetch(options, key) do
        :error ->
          true

        {:ok, value} when key == :maximum_qos ->
          is_integer(value) and value in 0..2

        {:ok, value} when key in [:max_inflight, :receive_maximum] ->
          is_integer(value) and value in 1..65_535

        {:ok, value} when key == :max_packet_size ->
          is_integer(value) and value in 1..268_435_460

        {:ok, value} ->
          is_integer(value) and value > 0
      end
    end)
  end

  defp persistence(nil), do: {:ok, nil}

  defp persistence(options) do
    with true <- keyword?(options, @persistence_keys),
         directory when is_binary(directory) <- Keyword.get(options, :directory),
         true <- Path.type(directory) == :absolute,
         stream when is_binary(stream) and byte_size(stream) in 1..1024 <-
           Keyword.get(options, :stream_id),
         snapshot when is_integer(snapshot) and snapshot in 1024..67_108_800 <-
           Keyword.get(options, :max_snapshot_bytes, 33_554_432),
         log when is_integer(log) and log >= snapshot + 64 and log <= 9_223_372_036_854_775_807 <-
           Keyword.get(options, :max_log_bytes, 1_073_741_824),
         fault <- Keyword.get(options, :fault_injector),
         true <- is_nil(fault) or is_function(fault, 2) do
      {:ok,
       %{
         directory: directory,
         stream_id: stream,
         max_snapshot_bytes: snapshot,
         max_log_bytes: log,
         fault_injector: fault
       }}
    else
      _ -> invalid()
    end
  end

  defp listeners(listeners) when is_list(listeners) and length(listeners) <= 16 do
    listeners
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {options, index}, {:ok, configured} ->
      with true <- keyword?(options, @listener_keys),
           transport when transport in [:tcp, :tls] <- Keyword.get(options, :transport),
           ip when is_tuple(ip) <- Keyword.get(options, :ip),
           true <- valid_ip?(ip),
           port when is_integer(port) and port in 0..65_535 <- Keyword.get(options, :port),
           backlog when is_integer(backlog) and backlog in 1..65_535 <-
             Keyword.get(options, :backlog, 128),
           tls <- Keyword.get(options, :tls_options, []),
           true <- valid_tls?(transport, tls) do
        listener = %{
          id: Keyword.get(options, :id, index),
          transport: transport,
          ip: ip,
          port: port,
          backlog: backlog,
          tls_options: tls
        }

        if Enum.any?(configured, &(&1.id == listener.id)),
          do: {:halt, invalid()},
          else: {:cont, {:ok, [listener | configured]}}
      else
        _invalid -> {:halt, invalid()}
      end
    end)
    |> case do
      {:ok, configured} -> {:ok, Enum.reverse(configured)}
      error -> error
    end
  end

  defp listeners(_listeners), do: invalid()

  defp valid_tls?(:tcp, []), do: true
  defp valid_tls?(:tcp, _tls), do: false

  defp valid_tls?(:tls, tls) do
    keyword?(tls, @tls_keys) and
      ((path?(Keyword.get(tls, :certfile)) and path?(Keyword.get(tls, :keyfile))) or
         match?([_ | _], Keyword.get(tls, :certs_keys)))
  end

  defp keyword?(options, keys) when is_list(options) do
    Keyword.keyword?(options) and Enum.all?(Keyword.keys(options), &(&1 in keys)) and
      length(Keyword.keys(options)) == length(Enum.uniq(Keyword.keys(options)))
  end

  defp keyword?(_options, _keys), do: false

  defp valid_ip?(ip) when tuple_size(ip) == 4,
    do: ip |> Tuple.to_list() |> Enum.all?(&(is_integer(&1) and &1 in 0..255))

  defp valid_ip?(ip) when tuple_size(ip) == 8,
    do: ip |> Tuple.to_list() |> Enum.all?(&(is_integer(&1) and &1 in 0..65_535))

  defp valid_ip?(_ip), do: false
  defp path?(path), do: (is_binary(path) and path != "") or (is_list(path) and path != [])
  defp invalid, do: {:error, :invalid_mqtt_options}
end
