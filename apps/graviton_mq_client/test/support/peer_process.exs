defmodule GravitonMQ.Client.Test.PeerProcess do
  @moduledoc false
  use GenServer

  def start_link(options), do: GenServer.start_link(__MODULE__, options)
  def snapshot(owner), do: GenServer.call(owner, :snapshot)
  def command(owner, bytes), do: GenServer.call(owner, {:command, bytes})
  def restart(owner), do: GenServer.call(owner, :restart, 5_000)

  @impl true
  def init(options) do
    Process.flag(:trap_exit, true)
    {:ok, open(%{options: options, port: nil, output: "", exit_status: nil})}
  end

  @impl true
  def handle_call(:snapshot, _from, state) do
    {:reply, Map.take(state, [:output, :exit_status]), state}
  end

  def handle_call({:command, bytes}, _from, state),
    do: {:reply, Port.command(state.port, bytes), state}

  def handle_call(:restart, _from, state) do
    close(state.port)
    {:reply, :ok, open(%{state | output: "", exit_status: nil})}
  end

  @impl true
  def handle_info({port, {:data, data}}, %{port: port} = state) do
    output = state.output <> data

    output =
      binary_part(output, max(byte_size(output) - 65_536, 0), min(byte_size(output), 65_536))

    {:noreply, %{state | output: output}}
  end

  def handle_info({port, {:exit_status, status}}, %{port: port} = state),
    do: {:noreply, %{state | port: nil, exit_status: status}}

  def handle_info({:EXIT, _port, _reason}, state), do: {:noreply, state}
  def handle_info({_old_port, _message}, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state), do: close(state.port)

  defp open(state) do
    port =
      Port.open({:spawn_executable, Keyword.fetch!(state.options, :executable)}, [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        args: Keyword.fetch!(state.options, :args)
      ])

    %{state | port: port}
  end

  defp close(nil), do: :ok

  defp close(port) do
    case Port.info(port, :os_pid) do
      {:os_pid, pid} ->
        signal(pid, "-TERM")

        unless await_exit(port, 1_000) do
          signal(pid, "-KILL")
          await_exit(port, 1_000)
        end

      nil ->
        :ok
    end

    if Port.info(port), do: Port.close(port)
    :ok
  end

  defp signal(pid, signal) do
    System.cmd(System.find_executable("kill"), [signal, Integer.to_string(pid)],
      stderr_to_stdout: true
    )
  end

  defp await_exit(port, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    await_exit_until(port, deadline)
  end

  defp await_exit_until(port, deadline) do
    receive do
      {^port, {:exit_status, _status}} -> true
      {^port, {:data, _data}} -> await_exit_until(port, deadline)
    after
      max(deadline - System.monotonic_time(:millisecond), 0) -> false
    end
  end
end
