defmodule GravitonMQ.Runtime.BoundariesTest do
  use ExUnit.Case, async: true

  @modules [
    GravitonMQ.Runtime,
    GravitonMQ.Runtime.Supervisor,
    GravitonMQ.Runtime.InfrastructureSupervisor,
    GravitonMQ.Runtime.NodeSupervisor,
    GravitonMQ.Runtime.ListenerSupervisor,
    GravitonMQ.Runtime.ConnectionSupervisor,
    GravitonMQ.Runtime.Queue,
    GravitonMQ.Runtime.EffectExecutor
  ]

  test "runtime boundary modules load" do
    Enum.each(@modules, fn module ->
      assert Code.ensure_loaded?(module), "expected #{inspect(module)} to load"
    end)
  end

  test "the Milestone 0 runtime supervisor starts empty and stops cleanly" do
    assert {:ok, supervisor} = GravitonMQ.Runtime.Supervisor.start_link(name: nil)
    Process.unlink(supervisor)

    assert [] = Supervisor.which_children(supervisor)
    assert :ok = Supervisor.stop(supervisor)
    refute Process.alive?(supervisor)
  end

  test "the listener supervisor restarts only a failed listener" do
    children =
      for id <- [:a, :b],
          do: Supervisor.child_spec({Agent, fn -> id end}, id: {:listener, id})

    supervisor = start_supervised!({GravitonMQ.Runtime.ListenerSupervisor, children})
    [a, b] = for id <- [:a, :b], do: child(supervisor, {:listener, id})
    a_monitor = Process.monitor(a)

    Process.exit(a, :kill)
    assert_receive {:DOWN, ^a_monitor, :process, ^a, :killed}, 1_000

    assert restarted(supervisor, {:listener, :a}, a, 100) != a
    assert child(supervisor, {:listener, :b}) == b

    assert %{type: :supervisor, shutdown: :infinity} =
             GravitonMQ.Runtime.ListenerSupervisor.child_spec([])
  end

  defp restarted(supervisor, id, old, attempts) do
    case List.keyfind(Supervisor.which_children(supervisor), id, 0) do
      {^id, pid, _type, _modules} when is_pid(pid) and pid != old ->
        pid

      _not_yet when attempts > 0 ->
        Process.sleep(10)
        restarted(supervisor, id, old, attempts - 1)
    end
  end

  defp child(supervisor, id) do
    {^id, pid, _type, _modules} = List.keyfind(Supervisor.which_children(supervisor), id, 0)
    assert is_pid(pid)
    pid
  end
end
