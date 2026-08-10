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

  test "an explicitly named instance starts through the public API and stops cleanly" do
    assert {:ok, supervisor} =
             GravitonMQ.start_link(
               name: :graviton_mq_test_named_instance,
               runtime_supervisor_name: :graviton_mq_test_named_runtime
             )

    Process.unlink(supervisor)

    assert ^supervisor = Process.whereis(:graviton_mq_test_named_instance)
    runtime = Process.whereis(:graviton_mq_test_named_runtime)
    assert is_pid(runtime)

    assert [
             {GravitonMQ.Runtime.Supervisor, ^runtime, :supervisor,
              [GravitonMQ.Runtime.Supervisor]}
           ] =
             Supervisor.which_children(supervisor)

    assert [] = Supervisor.which_children(runtime)
    assert :ok = Supervisor.stop(supervisor)
    refute Process.alive?(supervisor)
    refute Process.alive?(runtime)
  end

  test "two differently named empty instances coexist" do
    assert {:ok, first} =
             GravitonMQ.start_link(
               name: :graviton_mq_test_first_instance,
               runtime_supervisor_name: :graviton_mq_test_first_runtime
             )

    assert {:ok, second} =
             GravitonMQ.start_link(
               name: :graviton_mq_test_second_instance,
               runtime_supervisor_name: :graviton_mq_test_second_runtime
             )

    Process.unlink(first)
    Process.unlink(second)

    assert first != second

    assert [] =
             :graviton_mq_test_first_runtime
             |> Process.whereis()
             |> Supervisor.which_children()

    assert [] =
             :graviton_mq_test_second_runtime
             |> Process.whereis()
             |> Supervisor.which_children()

    assert :ok = Supervisor.stop(first)
    assert :ok = Supervisor.stop(second)
  end

  test "a duplicate instance name fails predictably" do
    options = [
      name: :graviton_mq_test_duplicate_instance,
      runtime_supervisor_name: :graviton_mq_test_duplicate_runtime
    ]

    assert {:ok, supervisor} = GravitonMQ.start_link(options)
    Process.unlink(supervisor)

    assert {:error, {:already_started, ^supervisor}} = GravitonMQ.start_link(options)
    assert :ok = Supervisor.stop(supervisor)
  end

  test "child specifications are scoped by the explicit top-level name" do
    first =
      GravitonMQ.child_spec(
        name: :graviton_mq_test_child_one,
        runtime_supervisor_name: :graviton_mq_test_child_runtime_one
      )

    second =
      GravitonMQ.child_spec(
        name: :graviton_mq_test_child_two,
        runtime_supervisor_name: :graviton_mq_test_child_runtime_two
      )

    assert first.id != second.id
    assert {GravitonMQ, :start_link, [_]} = first.start
    assert :supervisor = first.type
  end
end
