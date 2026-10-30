defmodule GravitonMQ.MQTTBroker.PrivacyLogHandler do
  @moduledoc false
  def log(event, %{config: %{parent: parent}}), do: send(parent, {:broker_privacy_log, event})
end

defmodule GravitonMQ.MQTTBroker.PrivacyTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureLog

  test "queued CONNECT credentials stay out of raw OTP crash reports" do
    filters = :logger.get_primary_config().filters
    :logger.remove_primary_filter(:logger_translator)

    :ok =
      :logger.add_handler(
        :graviton_mq_broker_privacy_test,
        GravitonMQ.MQTTBroker.PrivacyLogHandler,
        %{level: :error, config: %{parent: self()}}
      )

    on_exit(fn ->
      :logger.remove_handler(:graviton_mq_broker_privacy_test)
      :logger.set_primary_config(:filters, filters)
    end)

    instance =
      start_supervised!(
        {GravitonMQ, name: nil, runtime_supervisor_name: nil, mqtt: [auth: :anonymous]}
      )

    assert {:ok, %{owner: owner}} = GravitonMQ.status(instance)
    monitor = Process.monitor(owner)
    private = "synthetic-broker-private-#{System.unique_integer([:positive])}"

    packet = %GravitonMQ.MQTT.Packet.Connect{
      version: :v5,
      clean_start: true,
      client_id: "privacy",
      username: private,
      password: private
    }

    capture_log(fn ->
      :ok = :sys.suspend(owner)
      send(owner, {:"$gen_call", {self(), make_ref()}, :invalid_internal_call})
      send(owner, {:"$gen_call", {self(), make_ref()}, {:packet, 1, 1, packet, <<>>}})
      :ok = :sys.resume(owner)

      assert_receive {:DOWN, ^monitor, :process, ^owner,
                      {{:mqtt_broker_callback_failed, :error}, stack}},
                     1_000

      assert Enum.all?(stack, fn {_module, _function, arity, _location} -> is_integer(arity) end)
      Logger.flush()
      events = collect([])

      assert Enum.any?(events, &match?(%{msg: {:report, %{label: {:proc_lib, :crash}}}}, &1)),
             "regression must inspect a real raw proc_lib crash report"

      leaks =
        String.contains?(inspect(events, limit: :infinity, printable_limit: :infinity), private)

      refute leaks, "credential content leaked in a raw OTP report"
    end)
  end

  defp collect(events) do
    receive do
      {:broker_privacy_log, event} -> collect([event | events])
    after
      50 -> events
    end
  end
end
