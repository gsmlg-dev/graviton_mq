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
    def authenticate(_credentials, _options), do: {:error, :denied}
  end

  test "callback results map to SASL outcome classes without callback data" do
    assert Auth.invoke(Policy, [tag: 1], %{password: "good"}) == {:ok, {:principal, 1}}
    assert Auth.invoke(Policy, [], %{password: "bad"}) == {:error, :auth}

    for password <- ["raise", "exit", "throw", "invalid"] do
      assert Auth.invoke(Policy, [], %{password: password}) == {:error, :sys}
    end
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
end
