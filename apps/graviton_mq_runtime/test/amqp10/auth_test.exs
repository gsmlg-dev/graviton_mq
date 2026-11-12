defmodule GravitonMQ.Runtime.AMQP10.AuthTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.Runtime.AMQP10.Auth

  defmodule Policy do
    @behaviour GravitonMQ.Runtime.AMQP10.Auth
    @impl true
    def authenticate(%{password: "good"}, options), do: {:ok, {:principal, options[:tag]}}

    def authenticate(%{password: "raise"} = credentials, _),
      do: raise("leak #{credentials.password}")

    def authenticate(%{password: "exit"}, _options), do: exit(:boom)
    def authenticate(%{password: "throw"}, _options), do: throw(:boom)
    def authenticate(%{password: "invalid"}, _options), do: :yes
    def authenticate(%{password: "reserved"}, _options), do: {:ok, :unauthenticated}
    def authenticate(_credentials, _options), do: {:error, :denied}
  end

  test "callback results map to SASL outcome classes without callback data" do
    assert Auth.invoke(Policy, [tag: 1], %{password: "good"}) == {:ok, {:principal, 1}}
    assert Auth.invoke(Policy, [], %{password: "bad"}) == {:error, :auth}

    for password <- ["raise", "exit", "throw", "invalid", "reserved"] do
      assert Auth.invoke(Policy, [], %{password: password}) == {:error, :sys}
    end
  end

  test "the reserved deny-only principal is refused at SASL instead of admitted" do
    # `:unauthenticated` would otherwise succeed SASL and then silently deny
    # every Link; it is an invalid callback return (`sys`).
    assert Auth.invoke(Policy, [], %{password: "reserved"}) == {:error, :sys}
  end

  test "the watcher kills a callback at its deadline or when its caller dies" do
    test = self()
    callback = spawn(fn -> Process.sleep(:infinity) end)
    monitor = Process.monitor(callback)
    spawn(fn -> Auth.watch(callback, test, System.monotonic_time(:millisecond) + 20) end)
    assert_receive {:DOWN, ^monitor, :process, ^callback, :killed}, 1_000

    callback = spawn(fn -> Process.sleep(:infinity) end)
    caller = spawn(fn -> receive do: (:stop -> :ok) end)
    monitor = Process.monitor(callback)
    spawn(fn -> Auth.watch(callback, caller, System.monotonic_time(:millisecond) + 60_000) end)
    send(caller, :stop)
    assert_receive {:DOWN, ^monitor, :process, ^callback, :killed}, 1_000
  end

  test "the watcher exits as soon as its callback finishes" do
    test = self()
    callback = spawn(fn -> receive do: (:finish -> :ok) end)
    caller = spawn(fn -> receive do: (:stop -> :ok) end)

    watcher =
      spawn(fn -> Auth.watch(callback, caller, System.monotonic_time(:millisecond) + 60_000) end)

    watcher_monitor = Process.monitor(watcher)
    send(callback, :finish)
    assert_receive {:DOWN, ^watcher_monitor, :process, ^watcher, :normal}, 1_000
    # Neither the caller nor the test was touched.
    assert Process.alive?(caller)
    assert Process.alive?(test)
    send(caller, :stop)
  end
end
