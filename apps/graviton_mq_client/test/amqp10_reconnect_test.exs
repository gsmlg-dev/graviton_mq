defmodule GravitonMQ.AMQP10Client.ReconnectTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.{Error, Value}
  alias GravitonMQ.Client.AMQP10.{Config, Reconnect}

  describe "configuration" do
    test "reconnect mirrors the MQTT policy bounds and defaults to one connection" do
      assert {:ok, %{reconnect: %{max_attempts: 0, delay: 250, max_delay: 5_000}}} =
               Config.build(container_id: "c", observer: self())

      assert {:ok, %{reconnect: %{max_attempts: 100, delay: 1, max_delay: 60_000}}} =
               Config.build(
                 container_id: "c",
                 observer: self(),
                 reconnect: [max_attempts: 100, delay: 1, max_delay: 60_000]
               )

      for reconnect <- [
            [max_attempts: -1],
            [max_attempts: 101],
            [max_attempts: :infinity],
            [delay: 0],
            [delay: 60_001],
            [delay: 500, max_delay: 499],
            [max_delay: 60_001],
            [jitter: true],
            :yes,
            [{"delay", 1}]
          ] do
        assert {:error, :invalid_options} =
                 Config.build(container_id: "c", observer: self(), reconnect: reconnect)
      end
    end
  end

  describe "policy" do
    test "the delay before attempt n is min(max_delay, delay * 2^(n-1)) without jitter" do
      policy = %{max_attempts: 10, delay: 250, max_delay: 5_000}

      assert Enum.map(1..7, &Reconnect.delay(policy, &1)) ==
               [250, 500, 1_000, 2_000, 4_000, 5_000, 5_000]

      assert Reconnect.delay(%{policy | delay: 60_000, max_delay: 60_000}, 100) == 60_000
    end

    test "transient losses retry and terminal failures stop" do
      forced = %Error{condition: Value.symbol("amqp:connection:forced")}
      denied = %Error{condition: Value.symbol("amqp:unauthorized-access")}

      for reason <- [
            :closed,
            {:transport, :econnreset},
            {:write_failed, {:error, :timeout}},
            {:connect, :econnrefused},
            {:connect, :timeout},
            {:connect, :nxdomain},
            {:connect, :closed},
            :sasl_timeout,
            {:sasl, :sys_temp},
            :normal,
            {:peer_close, forced}
          ] do
        assert Reconnect.classify(reason, nil) == :retry, inspect(reason)
      end

      for reason <- [
            {:sasl, :auth},
            {:sasl, :sys},
            {:sasl, :sys_perm},
            {:sasl, {:unknown, 9}},
            :sasl_not_offered,
            :sasl_mechanism_not_offered,
            :sasl_protocol_error,
            :invalid_sasl_credentials,
            {:connect, {:tls_alert, :unknown_ca}},
            {:connect, :transport_error},
            {:write_failed, {:error, {:encode, :limit_exceeded, :frame_size_limit}}},
            {:decode_error, :input_buffer_limit},
            {:peer_close, denied},
            :owner_stopped,
            :close_timeout
          ] do
        assert Reconnect.classify(reason, nil) == :stop, inspect(reason)
      end

      # D1: a locally initiated Close is classified by its recorded cause.
      for reason <- [:normal, :close_timeout, :closed] do
        assert Reconnect.classify(reason, :idle_timeout) == :retry
        assert Reconnect.classify(reason, :open_timeout) == :retry
        assert Reconnect.classify(reason, :host_close) == :stop
        assert Reconnect.classify(reason, :protocol) == :stop
      end
    end
  end
end
