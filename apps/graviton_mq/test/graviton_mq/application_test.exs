defmodule GravitonMQ.ApplicationTest do
  use ExUnit.Case, async: true

  test "the public application owns the sole product Application callback" do
    assert {GravitonMQ.Application, []} = Application.spec(:graviton_mq, :mod)

    for app <- [
          :graviton_mq_core,
          :graviton_mq_amqp10,
          :graviton_mq_storage,
          :graviton_mq_runtime
        ] do
      assert [] == Application.spec(app, :mod)
    end
  end

  test "the application starts the public and runtime supervision boundaries" do
    public_supervisor = Process.whereis(GravitonMQ.Supervisor)
    runtime_supervisor = Process.whereis(GravitonMQ.Runtime.Supervisor)

    assert is_pid(public_supervisor)
    assert is_pid(runtime_supervisor)

    assert [
             {GravitonMQ.Runtime.Supervisor, ^runtime_supervisor, :supervisor,
              [GravitonMQ.Runtime.Supervisor]}
           ] = Supervisor.which_children(public_supervisor)
  end

  test "an isolated top-level supervision tree stops cleanly" do
    assert {:ok, supervisor} =
             GravitonMQ.Supervisor.start_link(name: nil, runtime_supervisor_name: nil)

    Process.unlink(supervisor)

    assert [
             {GravitonMQ.Runtime.Supervisor, runtime, :supervisor,
              [GravitonMQ.Runtime.Supervisor]}
           ] =
             Supervisor.which_children(supervisor)

    assert is_pid(runtime)
    assert :ok = Supervisor.stop(supervisor)
    refute Process.alive?(supervisor)
    refute Process.alive?(runtime)
  end
end
