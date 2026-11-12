defmodule GravitonMQ.Runtime.AMQP10.AuthenticatorTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

  alias GravitonMQ.Runtime.AMQP10.Authenticator

  defmodule Policy do
    @behaviour GravitonMQ.Runtime.AMQP10.Auth
    @impl true
    def authenticate(%{password: "good"}, options), do: {:ok, {:principal, options[:tag]}}
    def authenticate(%{password: "raise-sentinel"}, _options), do: raise("raise-sentinel")
    def authenticate(%{password: "invalid"}, _options), do: :yes

    def authenticate(%{password: "sleep" <> _, test: test}, _options) do
      send(test, {:sleeping, self()})
      Process.sleep(:infinity)
    end

    def authenticate(_credentials, _options), do: {:error, :denied}
  end

  test "starts callback tasks under :sasl_tasks and reports mapped results" do
    authenticator = authenticator!({Policy, [tag: 7]}, 2)

    for {password, expected} <- [
          {"good", {:ok, {:principal, 7}}},
          {"bad", {:error, :auth}},
          {"raise-sentinel", {:error, :sys}},
          {"invalid", {:error, :sys}}
        ] do
      reference = make_ref()

      assert {:ok, task} =
               Authenticator.start(
                 authenticator,
                 %{password: password},
                 self(),
                 reference,
                 deadline(1_000)
               )

      assert is_pid(task)
      assert_receive {:amqp10_sasl_result, ^reference, ^expected}, 1_000
    end
  end

  test "concurrency, deadlines and caller loss are bounded" do
    authenticator = authenticator!({Policy, []}, 1)
    credentials = %{password: "sleep", test: self()}

    # The occupying task cannot expire during the :busy check: its deadline
    # is far away and the test waits until it is running.
    assert {:ok, task} =
             Authenticator.start(authenticator, credentials, self(), make_ref(), deadline(60_000))

    assert_receive {:sleeping, ^task}, 1_000

    assert {:error, :busy} =
             Authenticator.start(
               authenticator,
               %{password: "good"},
               self(),
               make_ref(),
               deadline(60_000)
             )

    Process.exit(task, :kill)

    # The watcher backstops a missed deadline (deadline plus a short grace).
    authenticator = authenticator!({Policy, []}, 1)

    assert {:ok, task} =
             Authenticator.start(authenticator, credentials, self(), make_ref(), deadline(20))

    assert_receive {:sleeping, ^task}, 1_000
    # The 20 ms deadline may kill the task before this monitor exists.
    assert_killed(task, Process.monitor(task))

    # A fresh pool: the killed task's slot is released asynchronously.
    authenticator = authenticator!({Policy, []}, 1)
    caller = spawn(fn -> receive do: (:stop -> :ok) end)

    assert {:ok, task} =
             Authenticator.start(authenticator, credentials, caller, make_ref(), deadline(60_000))

    assert_receive {:sleeping, ^task}, 1_000
    monitor = Process.monitor(task)
    send(caller, :stop)
    assert_killed(task, monitor)
  end

  test "an anonymous policy or a missing authenticator never starts a callback" do
    assert {:error, :unavailable} =
             Authenticator.start(
               authenticator!(:anonymous, 1),
               %{},
               self(),
               make_ref(),
               deadline(100)
             )

    assert {:error, :unavailable} =
             Authenticator.start(nil, %{}, self(), make_ref(), deadline(100))
  end

  test "status and logs never contain policy options or credentials" do
    authenticator = authenticator!({Policy, [secret: "policy-secret"]}, 1)

    log =
      capture_log(fn ->
        reference = make_ref()

        {:ok, _task} =
          Authenticator.start(
            authenticator,
            %{password: "raise-sentinel"},
            self(),
            reference,
            deadline(1_000)
          )

        assert_receive {:amqp10_sasl_result, ^reference, {:error, :sys}}, 1_000
      end)

    refute log =~ "raise-sentinel"
    refute inspect(:sys.get_status(authenticator)) =~ "policy-secret"
  end

  test "a running callback and an Authenticator crash expose no credential or policy" do
    authenticator = authenticator!({Policy, [secret: "policy-secret"]}, 1)
    credentials = %{password: "sleep-password-sentinel", test: self()}

    assert {:ok, task} =
             Authenticator.start(authenticator, credentials, self(), make_ref(), deadline(5_000))

    assert_receive {:sleeping, ^task}, 1_000

    # A sensitive process hides its stack, mailbox and dictionary.
    send(task, {:probe, "mailbox-sentinel"})

    assert Process.info(task, [:messages, :dictionary, :backtrace]) ==
             [messages: [], dictionary: [], backtrace: ""]

    info = Process.info(task, [:current_stacktrace, :links, :monitored_by])
    refute printed(info) =~ "sleep-password-sentinel"
    refute printed(:sys.get_status(authenticator)) =~ "policy-secret"

    log =
      capture_log(fn ->
        monitor = Process.monitor(authenticator)
        :ok = :sys.terminate(authenticator, :crash_for_report)
        assert_receive {:DOWN, ^monitor, :process, ^authenticator, :crash_for_report}, 1_000
        # Let the crash report reach the logger before capture ends.
        Process.sleep(100)
      end)

    assert log =~ "terminating"
    assert log =~ "State: :redacted"
    refute log =~ "policy-secret"
    refute log =~ "sleep-password-sentinel"
  end

  test "the runtime keeps the policy out of every other owner and published table" do
    secret = "tree-policy-secret"

    {:ok, runtime} =
      GravitonMQ.Runtime.Supervisor.start_link(
        name: nil,
        amqp10: [
          container_id: "b",
          queues: ["q"],
          auth: {Policy, [secret: secret]},
          allow_insecure_plain: true,
          listeners: [[transport: :tcp, port: 0]]
        ]
      )

    on_exit(fn -> if Process.alive?(runtime), do: Supervisor.stop(runtime) end)

    processes = tree(runtime)
    [authenticator] = for {Authenticator, pid, _type} <- processes, do: pid

    # Supervisors hold child specifications as closures (never printed);
    # every other owner but the Authenticator must not hold the policy.
    supervisors = MapSet.new(for {_id, pid, :supervisor} <- processes, do: pid)

    for {id, pid, _type} <- processes do
      refute printed(:sys.get_status(pid)) =~ secret

      if pid != authenticator do
        refute printed(:sys.get_state(pid)) =~ secret, inspect(id)
      end

      if pid != authenticator and pid not in supervisors do
        refute :binary.match(:erlang.term_to_binary(:sys.get_state(pid)), secret) != :nomatch,
               inspect(id)
      end
    end

    # Only the Authenticator's start closure captures the policy.
    [amqp] = for {GravitonMQ.Runtime.AMQP10.Supervisor, pid, _type} <- processes, do: pid

    for {id, _pid, _type, _modules} <- Supervisor.which_children(amqp), id != Authenticator do
      {:ok, spec} = :supervisor.get_childspec(amqp, id)
      refute :binary.match(:erlang.term_to_binary(spec), secret) != :nomatch, inspect(id)
    end

    # No readable table (including the Listener's published Connection
    # specification) contains the policy.
    for table <- :ets.all(), :ets.info(table, :protection) in [:public, :protected] do
      contents =
        try do
          :ets.tab2list(table)
        rescue
          ArgumentError -> []
        end

      refute :binary.match(:erlang.term_to_binary(contents), secret) != :nomatch
    end

    Supervisor.stop(runtime)
  end

  test "an Authenticator crash restarts later owners but never the Broker" do
    {:ok, runtime} =
      GravitonMQ.Runtime.Supervisor.start_link(
        name: nil,
        amqp10: [
          container_id: "b",
          queues: ["q"],
          auth: {Policy, []},
          allow_insecure_plain: true,
          listeners: [[transport: :tcp, port: 0]]
        ]
      )

    on_exit(fn -> if Process.alive?(runtime), do: Supervisor.stop(runtime) end)
    [amqp] = for {GravitonMQ.Runtime.AMQP10.Supervisor, pid, _} <- tree(runtime), do: pid

    children = fn ->
      Map.new(Supervisor.which_children(amqp), fn {id, pid, _, _} -> {id, pid} end)
    end

    before = children.()
    monitor = Process.monitor(before[Authenticator])
    Process.exit(before[Authenticator], :kill)
    assert_receive {:DOWN, ^monitor, :process, _, :killed}, 1_000
    after_crash = replaced(children, before[Authenticator], 100)

    assert after_crash[GravitonMQ.Runtime.AMQP10.Broker] ==
             before[GravitonMQ.Runtime.AMQP10.Broker]

    assert after_crash[:sasl_tasks] == before[:sasl_tasks]
    assert is_pid(after_crash[Authenticator])
    assert after_crash[Authenticator] != before[Authenticator]
    assert after_crash[:connections] != before[:connections]
    Supervisor.stop(runtime)
  end

  defp replaced(children, old, 0),
    do: flunk("#{inspect(old)} was never replaced: #{inspect(children.())}")

  defp replaced(children, old, attempts) do
    current = children.()

    if is_pid(current[Authenticator]) and current[Authenticator] != old do
      current
    else
      Process.sleep(10)
      replaced(children, old, attempts - 1)
    end
  end

  defp tree(supervisor) do
    supervisor
    |> Supervisor.which_children()
    |> Enum.flat_map(fn
      {id, pid, :supervisor, _} when is_pid(pid) -> [{id, pid, :supervisor} | tree(pid)]
      {id, pid, :worker, _} when is_pid(pid) -> [{id, pid, :worker}]
      _other -> []
    end)
  end

  defp deadline(milliseconds), do: System.monotonic_time(:millisecond) + milliseconds

  defp printed(term), do: inspect(term, limit: :infinity, printable_limit: :infinity)

  defp authenticator!(policy, max_jobs) do
    parent =
      start_supervised!(%{
        id: make_ref(),
        type: :supervisor,
        start:
          {Supervisor, :start_link,
           [
             [Supervisor.child_spec({Task.Supervisor, max_children: max_jobs}, id: :sasl_tasks)],
             [strategy: :one_for_one]
           ]}
      })

    {:ok, authenticator} = Supervisor.start_child(parent, {Authenticator, {policy, parent}})
    authenticator
  end

  # A sensitive, sleeping callback task cannot answer a synchronous call, and
  # it may be killed before its monitor exists: `:noproc` is accepted only
  # for a task that is really gone.
  defp assert_killed(task, monitor) do
    assert_receive {:DOWN, ^monitor, :process, ^task, reason}, 1_000
    assert reason in [:killed, :noproc]
    refute Process.alive?(task)
  end
end
