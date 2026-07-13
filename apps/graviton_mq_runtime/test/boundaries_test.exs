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
end
