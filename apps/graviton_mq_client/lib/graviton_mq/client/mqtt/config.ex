defmodule GravitonMQ.Client.MQTT.Config do
  @moduledoc false

  alias GravitonMQ.MQTT.Codec.Limits
  alias GravitonMQ.MQTT.Packet.{Connect, Will}
  alias GravitonMQ.Client.Transport

  @keys ~w(id name host port transport tls_options observer client_id version username password
    keep_alive clean_start session_expiry_interval properties will max_inflight max_operations
    max_deliveries max_subscriptions receive_maximum max_packet_size connect_timeout handshake_timeout
    operation_timeout send_timeout tick_interval reconnect persistence)a

  def build(options) when is_list(options) do
    with true <- Keyword.keyword?(options),
         nil <- Enum.find(Keyword.keys(options), &(&1 not in @keys)),
         {:ok, reconnect} <- reconnect(Keyword.get(options, :reconnect, [])),
         {:ok, will} <- will(Keyword.get(options, :will)) do
      assemble(options, reconnect, will)
    else
      {:error, _} = error -> error
      _ -> {:error, :invalid_options}
    end
  end

  def build(_options), do: {:error, :invalid_options}

  defp assemble(options, reconnect, will) do
    transport = Keyword.get(options, :transport, :tcp)
    host = Keyword.get(options, :host, "localhost")
    deliveries = Keyword.get(options, :max_deliveries, 32)
    maximum = Keyword.get(options, :max_packet_size, 1_048_576)
    properties = Keyword.get(options, :properties, [])

    config = %{
      host: host,
      port: Keyword.get(options, :port, if(transport == :tls, do: 8883, else: 1883)),
      transport: transport,
      tls_options: Keyword.get(options, :tls_options, []),
      observer: Keyword.get(options, :observer),
      name: Keyword.get(options, :name),
      reconnect: reconnect,
      persistence: Keyword.get(options, :persistence),
      connect_timeout: Keyword.get(options, :connect_timeout, 2_000),
      handshake_timeout: Keyword.get(options, :handshake_timeout, 5_000),
      operation_timeout: Keyword.get(options, :operation_timeout, 5_000),
      send_timeout: Keyword.get(options, :send_timeout, 1_000),
      tick_interval: Keyword.get(options, :tick_interval, 100),
      limits: %Limits{max_packet_size: maximum},
      machine_options: [
        limits: %Limits{max_packet_size: maximum},
        max_inflight: Keyword.get(options, :max_inflight, 16),
        max_operations: Keyword.get(options, :max_operations, 32),
        max_deliveries: deliveries,
        max_subscriptions: Keyword.get(options, :max_subscriptions, 1_024),
        reconnect_max: reconnect.max_attempts
      ]
    }

    receive_maximum = Keyword.get(options, :receive_maximum, deliveries)

    valid =
      transport in [:tcp, :tls] and valid_host?(host) and
        is_integer(config.port) and config.port in 1..65_535 and
        is_pid(config.observer) and valid_name?(config.name) and
        persistence?(config.persistence, Keyword.get(options, :client_id, "")) and
        is_list(properties) and Keyword.keyword?(properties) and
        Enum.all?(
          Keyword.keys(properties),
          &(&1 not in [:receive_maximum, :maximum_packet_size, :topic_alias_maximum])
        ) and
        Enum.all?([deliveries, receive_maximum], &(is_integer(&1) and &1 in 1..65_535)) and
        receive_maximum <= deliveries and
        Enum.all?(
          [
            config.connect_timeout,
            config.handshake_timeout,
            config.operation_timeout,
            config.send_timeout,
            config.tick_interval
          ],
          &(is_integer(&1) and &1 > 0 and &1 <= 60_000)
        ) and config.connect_timeout <= 3_000 and config.send_timeout <= 3_000 and
        Transport.valid_tls_options?(config.tls_options) and
        Keyword.get(config.tls_options, :verify, :verify_peer) == :verify_peer and
        Keyword.get(config.tls_options, :server_name_indication) != :disable

    if valid do
      properties =
        properties
        |> Keyword.put(:receive_maximum, receive_maximum)
        |> Keyword.put(:maximum_packet_size, maximum)
        |> Keyword.put(:topic_alias_maximum, 0)
        |> Keyword.put_new(
          :session_expiry_interval,
          Keyword.get(options, :session_expiry_interval, 60)
        )

      connect = %Connect{
        version: Keyword.get(options, :version, :v5),
        client_id: Keyword.get(options, :client_id, ""),
        clean_start: Keyword.get(options, :clean_start, true),
        keep_alive: Keyword.get(options, :keep_alive, 30),
        username: Keyword.get(options, :username),
        password: Keyword.get(options, :password),
        properties: properties,
        will: will
      }

      with {:ok, _} <- Limits.validate(config.limits, :packet_encode),
           do: {:ok, Map.put(config, :connect, connect)}
    else
      {:error, :invalid_options}
    end
  end

  defp persistence?(nil, _), do: true

  defp persistence?(options, client_id) when is_list(options) do
    Keyword.keyword?(options) and
      Enum.all?(Keyword.keys(options), &(&1 in [:directory, :max_bytes, :io])) and
      is_binary(client_id) and client_id != "" and
      is_binary(Keyword.get(options, :directory)) and
      Path.type(Keyword.fetch!(options, :directory)) == :absolute and
      is_integer(Keyword.get(options, :max_bytes, 8_388_608)) and
      Keyword.get(options, :max_bytes, 8_388_608) in 256..67_108_864 and
      valid_io?(Keyword.get(options, :io, GravitonMQ.Client.MQTT.Store.IO))
  end

  defp persistence?(_, _), do: false

  defp valid_io?(module) when is_atom(module) do
    Code.ensure_loaded?(module) and
      Enum.all?(
        [open: 1, write: 2, sync: 1, close: 1, rename: 2, sync_directory: 1],
        fn {function, arity} -> function_exported?(module, function, arity) end
      )
  end

  defp valid_io?(_), do: false

  defp reconnect(options) when is_list(options) do
    if Keyword.keyword?(options) and
         Enum.all?(Keyword.keys(options), &(&1 in [:max_attempts, :delay, :max_delay])) do
      config = %{
        max_attempts: Keyword.get(options, :max_attempts, 0),
        delay: Keyword.get(options, :delay, 250),
        max_delay: Keyword.get(options, :max_delay, 5_000)
      }

      if is_integer(config.max_attempts) and config.max_attempts in 0..100 and
           is_integer(config.delay) and config.delay in 1..60_000 and
           is_integer(config.max_delay) and config.max_delay in config.delay..60_000,
         do: {:ok, config},
         else: {:error, :invalid_reconnect}
    else
      {:error, :invalid_reconnect}
    end
  end

  defp reconnect(_options), do: {:error, :invalid_reconnect}

  defp will(nil), do: {:ok, nil}
  defp will(%Will{} = will), do: {:ok, will}

  defp will(options) when is_list(options) do
    if Keyword.keyword?(options) and Keyword.has_key?(options, :topic) and
         Keyword.has_key?(options, :payload) and
         Enum.all?(Keyword.keys(options), &(&1 in [:topic, :payload, :qos, :retain, :properties])),
       do: {:ok, struct!(Will, options)},
       else: {:error, :invalid_will}
  end

  defp will(_options), do: {:error, :invalid_will}

  defp valid_host?(host) when is_binary(host),
    do: host != "" and String.valid?(host) and not String.contains?(host, <<0>>)

  defp valid_host?(host) when is_list(host),
    do: host != [] and Enum.all?(host, &(is_integer(&1) and &1 in 1..0x10FFFF))

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
