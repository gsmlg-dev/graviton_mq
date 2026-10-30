defmodule GravitonMQ.Client.PrivacyLogHandler do
  @moduledoc false
  def log(event, %{config: %{parent: parent}}), do: send(parent, {:privacy_log, event})
end

defmodule GravitonMQ.Client.PrivacyTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureLog

  test "unexpected callback crashes retain failure without credentials in any OTP report" do
    :ok =
      :logger.add_handler(:graviton_mq_privacy_test, GravitonMQ.Client.PrivacyLogHandler, %{
        level: :error,
        config: %{parent: self()}
      })

    on_exit(fn -> :logger.remove_handler(:graviton_mq_privacy_test) end)

    {:ok, listener} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false])
    {:ok, port} = :inet.port(listener)
    :ok = :gen_tcp.close(listener)
    password = "synthetic-client-password"
    username = "synthetic-client-username"

    capture_log(fn ->
      child =
        Supervisor.child_spec(
          {GravitonMQ.MQTTClient,
           host: "127.0.0.1",
           port: port,
           client_id: "privacy",
           username: username,
           password: password,
           observer: self()},
          restart: :temporary
        )

      host =
        start_supervised!(%{
          id: :privacy_host,
          type: :supervisor,
          start: {Supervisor, :start_link, [[child], [strategy: :one_for_one]]}
        })

      [{_, client, _, _}] = Supervisor.which_children(host)
      monitor = Process.monitor(client)
      GravitonMQ.MQTTClient.status(client)

      try do
        GenServer.call(client, :unexpected_call)
      catch
        :exit, _reason -> :ok
      end

      assert_receive {:DOWN, ^monitor, :process, ^client, reason}, 1_000
      assert {{:mqtt_client_callback_failed, FunctionClauseError}, stacktrace} = reason

      assert Enum.all?(stacktrace, fn {_module, _function, arity, _location} ->
               is_integer(arity)
             end)

      assert Process.alive?(host)
      Logger.flush()
      events = collect_logs([])
      assert events != []

      formatted =
        Enum.map_join(events, "", fn event ->
          event |> :logger_formatter.format(%{}) |> IO.iodata_to_binary()
        end)

      refute String.contains?(formatted, password), "password leaked in OTP crash reports"
      refute String.contains?(formatted, username), "username leaked in OTP crash reports"
    end)
  end

  defp collect_logs(events) do
    receive do
      {:privacy_log, event} -> collect_logs([event | events])
    after
      50 -> events
    end
  end
end
