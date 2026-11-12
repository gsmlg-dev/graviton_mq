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
               Authenticator.start(authenticator, %{password: password}, self(), reference, 1_000)

      assert is_pid(task)
      assert_receive {:amqp10_sasl_result, ^reference, ^expected}, 1_000
    end
  end

  test "concurrency, deadlines and caller loss are bounded" do
    authenticator = authenticator!({Policy, []}, 1)
    credentials = %{password: "sleep", test: self()}
    assert {:ok, task} = Authenticator.start(authenticator, credentials, self(), make_ref(), 100)
    assert_receive {:sleeping, ^task}, 1_000

    assert {:error, :busy} =
             Authenticator.start(authenticator, %{password: "good"}, self(), make_ref(), 100)

    monitor = Process.monitor(task)
    assert_receive {:DOWN, ^monitor, :process, ^task, :killed}, 1_000

    # A fresh pool: the killed task's slot is released asynchronously.
    authenticator = authenticator!({Policy, []}, 1)
    caller = spawn(fn -> receive do: (:stop -> :ok) end)

    assert {:ok, task} =
             Authenticator.start(authenticator, credentials, caller, make_ref(), 60_000)

    assert_receive {:sleeping, ^task}, 1_000
    monitor = Process.monitor(task)
    send(caller, :stop)
    assert_receive {:DOWN, ^monitor, :process, ^task, :killed}, 1_000
  end

  test "an anonymous policy or a missing authenticator never starts a callback" do
    assert {:error, :unavailable} =
             Authenticator.start(authenticator!(:anonymous, 1), %{}, self(), make_ref(), 100)

    assert {:error, :unavailable} = Authenticator.start(nil, %{}, self(), make_ref(), 100)
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
            1_000
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
             Authenticator.start(authenticator, credentials, self(), make_ref(), 5_000)

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

  defp tree(supervisor) do
    supervisor
    |> Supervisor.which_children()
    |> Enum.flat_map(fn
      {id, pid, :supervisor, _} when is_pid(pid) -> [{id, pid, :supervisor} | tree(pid)]
      {id, pid, :worker, _} when is_pid(pid) -> [{id, pid, :worker}]
      _other -> []
    end)
  end

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
end
