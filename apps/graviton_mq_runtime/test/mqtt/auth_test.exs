defmodule GravitonMQ.Runtime.MQTT.AuthTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.Runtime.MQTT.Auth

  defmodule Policy do
    def authenticate(credentials, options) do
      case Keyword.get(options, :mode) do
        :raise ->
          raise "private #{credentials.password}"

        :throw ->
          throw(credentials.password)

        :exit ->
          exit(credentials.password)

        :invalid ->
          :ok

        :deny ->
          {:error, credentials.password}

        mode when mode in [:wait, :trap_wait] ->
          if mode == :trap_wait, do: Process.flag(:trap_exit, true)
          send(options[:owner], {:callback_started, self()})

          receive do
            :finish -> {:ok, :principal}
          end

        _ ->
          {:ok, %{id: credentials.client_id}}
      end
    end

    def authorize(principal, action, resource, options) do
      send(options[:owner], {:authorization, principal, action, resource})
      Keyword.get(options, :result, :ok)
    end
  end

  setup do
    supervisor = start_supervised!({Task.Supervisor, max_children: 1})
    %{supervisor: supervisor, credentials: %{client_id: "client", password: "private-secret"}}
  end

  test "anonymous policy is explicit and missing policy fails closed", context do
    assert {:ok, :anonymous} =
             Auth.run(context.supervisor, :anonymous, {:authenticate, context.credentials}, 100)

    assert :ok =
             Auth.run(context.supervisor, :anonymous, {:authorize, :host, :publish, "a/b"}, 100)

    assert {:error, :invalid_policy} =
             Auth.run(context.supervisor, nil, {:authenticate, context.credentials}, 100)
  end

  test "callbacks receive the actual principal, action and resource", context do
    policy = {Policy, owner: self()}

    assert {:ok, %{id: "client"} = principal} =
             Auth.run(context.supervisor, policy, {:authenticate, context.credentials}, 100)

    for action <- [:publish, :subscribe, :deliver] do
      assert :ok =
               Auth.run(context.supervisor, policy, {:authorize, principal, action, "a/+"}, 100)

      assert_receive {:authorization, ^principal, ^action, "a/+"}
    end
  end

  test "denial, malformed return, raised, thrown and exit values fail closed without data",
       context do
    for {mode, reason} <- [
          deny: :denied,
          invalid: :invalid_callback_result,
          raise: :callback_failed,
          throw: :callback_failed,
          exit: :callback_failed
        ] do
      assert {:error, ^reason} =
               Auth.run(
                 context.supervisor,
                 {Policy, mode: mode},
                 {:authenticate, context.credentials},
                 100
               )
    end

    assert {:error, :denied} =
             Auth.run(
               context.supervisor,
               {Policy, owner: self(), result: {:error, "private-reason"}},
               {:authorize, :host, :publish, "a"},
               100
             )

    assert [] = Task.Supervisor.children(context.supervisor)
  end

  test "deadline terminates its callback instead of leaving work running", context do
    assert {:error, :callback_timeout} =
             Auth.run(
               context.supervisor,
               {Policy, mode: :wait, owner: self()},
               {:authenticate, context.credentials},
               20
             )

    assert_receive {:callback_started, worker}
    refute Process.alive?(worker)
    assert [] = Task.Supervisor.children(context.supervisor)
  end

  test "a saturated callback supervisor rejects another request without queuing", context do
    parent = self()

    caller =
      spawn(fn ->
        send(
          parent,
          {:first_result,
           Auth.run(
             context.supervisor,
             {Policy, mode: :wait, owner: parent},
             {:authenticate, context.credentials},
             1_000
           )}
        )
      end)

    on_exit(fn -> if Process.alive?(caller), do: Process.exit(caller, :kill) end)
    assert_receive {:callback_started, worker}

    assert {:error, :callback_busy} =
             Auth.run(context.supervisor, {Policy, []}, {:authenticate, context.credentials}, 100)

    send(worker, :finish)
    assert_receive {:first_result, {:ok, :principal}}
    assert [] = Task.Supervisor.children(context.supervisor)
  end

  test "caller death cancels its callback and releases the finite slot", context do
    parent = self()

    caller =
      spawn(fn ->
        Auth.run(
          context.supervisor,
          {Policy, mode: :wait, owner: parent},
          {:authenticate, context.credentials},
          1_000
        )
      end)

    on_exit(fn -> if Process.alive?(caller), do: Process.exit(caller, :kill) end)
    assert_receive {:callback_started, worker}
    monitor = Process.monitor(worker)
    Process.exit(caller, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^worker, _reason}, 200
  end

  test "forced supervisor-child death cannot leave a callback trapping exits alive", context do
    parent = self()

    caller =
      spawn(fn ->
        send(
          parent,
          {:result,
           Auth.run(
             context.supervisor,
             {Policy, mode: :trap_wait, owner: parent},
             {:authenticate, context.credentials},
             1_000
           )}
        )
      end)

    on_exit(fn -> if Process.alive?(caller), do: Process.exit(caller, :kill) end)
    assert_receive {:callback_started, callback}
    on_exit(fn -> if Process.alive?(callback), do: Process.exit(callback, :kill) end)
    monitor = Process.monitor(callback)
    [worker] = Task.Supervisor.children(context.supervisor)
    Process.exit(worker, :kill)
    assert_receive {:result, {:error, :callback_failed}}
    assert_receive {:DOWN, ^monitor, :process, ^callback, _reason}, 200
  end
end
