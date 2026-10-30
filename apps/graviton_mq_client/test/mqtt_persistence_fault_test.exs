defmodule GravitonMQ.MQTTClient.FaultIO do
  @moduledoc false
  alias GravitonMQ.Client.MQTT.Store.IO, as: Real
  def open(path), do: Real.open(path)

  def write(file, bytes) do
    cond do
      fault?(:write) ->
        {:error, :enospc}

      fault?(:partial) ->
        :ok = Real.write(file, binary_part(bytes, 0, div(byte_size(bytes), 2)))
        {:error, :enospc}

      true ->
        Real.write(file, bytes)
    end
  end

  def sync(file), do: if(fault?(:sync), do: {:error, :eio}, else: Real.sync(file))
  def close(file), do: Real.close(file)
  def rename(a, b), do: if(fault?(:rename), do: {:error, :eio}, else: Real.rename(a, b))

  def sync_directory(path) do
    if fault?(:directory) do
      {:error, :eio}
    else
      result = Real.sync_directory(path)
      if fault?(:kill_after_directory), do: Process.exit(self(), :kill)
      result
    end
  end

  defp fault?(kind), do: Process.get(:mqtt_persistence_fault) == kind
end

defmodule GravitonMQ.MQTTClient.PersistenceFaultTest do
  use ExUnit.Case, async: false
  alias GravitonMQ.Client.MQTT.Store
  alias GravitonMQ.MQTTClient, as: Client
  alias GravitonMQ.MQTTClient.FaultIO
  @moduletag :tmp_dir

  for {fault, class} <- [
        write: :failed,
        partial: :failed,
        sync: :failed,
        rename: :unknown,
        directory: :unknown
      ] do
    @fault fault
    @class class
    test "#{fault} failure poisons the store and exposes #{class} publication", context do
      directory = Path.join(context.tmp_dir, "store")
      {:ok, store, nil} = Store.open(directory: directory, io: FaultIO)
      {:ok, store} = Store.commit(store, %{value: :old})
      Process.put(:mqtt_persistence_fault, @fault)
      assert {:error, {@class, _}, poisoned} = Store.commit(store, %{value: :new})
      assert {:error, :poisoned, ^poisoned} = Store.commit(poisoned, %{value: :retry})
      Process.delete(:mqtt_persistence_fault)
      Store.close(poisoned)
      assert {:ok, reopened, %{value: value}} = Store.open(directory: directory)
      assert value == if(@fault == :directory, do: :new, else: :old)
      Store.close(reopened)
    end

    test "#{fault} failure prevents wire publication and ordinary admission success", context do
      {client, socket} = connected!(context)
      monitor = Process.monitor(client)

      :sys.replace_state(client, fn state ->
        Process.put(:mqtt_persistence_fault, @fault)
        state
      end)

      assert {:error, {:persistence, {@class, _}}} =
               Client.publish(client, "no-wire", "secret", qos: 2)

      assert_receive {:graviton_mq, ^client, {:storage_error, {@class, _}}}, 2_000
      assert_receive {:graviton_mq, ^client, {:operation, _, {:error, :unknown_outcome}}}, 2_000
      assert_receive {:DOWN, ^monitor, :process, ^client, :normal}, 2_000
      assert {:error, :closed} = :gen_tcp.recv(socket, 0, 1_000)
      refute_receive {:graviton_mq, ^client, {:operation, _, {:ok, _}}}, 20
    end
  end

  test "host ACK fails explicitly before PUBREC when synchronization fails", context do
    {client, socket} = connected!(context)
    assert :ok = Client.request(client, 1)
    # Independent wire PUBLISH, QoS2, packet ID 7, no properties.
    :ok = :gen_tcp.send(socket, <<0x34, 9, 0, 1, ?a, 0, 7, 0, "msg">>)
    assert_receive {:graviton_mq, ^client, {:delivery, delivery, _}}, 2_000

    :sys.replace_state(client, fn state ->
      Process.put(:mqtt_persistence_fault, :sync)
      state
    end)

    assert {:error, {:persistence, {:failed, :eio}}} = Client.ack(client, delivery)
    assert_receive {:graviton_mq, ^client, {:storage_error, {:failed, :eio}}}, 2_000
    assert {:error, :closed} = :gen_tcp.recv(socket, 0, 1_000)
  end

  test "a crash after terminal-result synchronization recovers the outbox before any ordinary success",
       context do
    {first, socket} = connected!(context)
    assert {:ok, ref} = Client.publish(first, "terminal", "body", qos: 1)
    assert_receive {:graviton_mq, ^first, {:operation_identity, ^ref, stable_id}}, 2_000
    assert {:ok, <<0x32, _::binary>>} = :gen_tcp.recv(socket, 0, 1_000)
    state = :sys.get_state(first)
    [{_, entry}] = Map.to_list(state.machine.outbound)
    monitor = Process.monitor(first)

    :sys.replace_state(first, fn state ->
      Process.put(:mqtt_persistence_fault, :kill_after_directory)
      state
    end)

    :ok = :gen_tcp.send(socket, <<0x40, 2, entry.packet.packet_id::16>>)
    assert_receive {:DOWN, ^monitor, :process, ^first, :killed}, 2_000
    refute_receive {:graviton_mq, ^first, {:operation, ^ref, {:ok, _}}}, 20
    {second, _socket} = connected!(context)

    assert_receive {:graviton_mq, ^second,
                    {:recovered_operation_result, ^stable_id, {:ok, :puback}}},
                   2_000

    refute_receive {:graviton_mq, ^second, {:operation, _, _}}, 20
  end

  test "persistent profile requires stable client identity and an existing absolute parent",
       context do
    assert {:error, :invalid_options} =
             Client.start_link(persistence: [directory: context.tmp_dir])

    assert {:error, :invalid_options} =
             Client.start_link(client_id: "x", persistence: [directory: "relative"])
  end

  test "validated reopen synchronizes recovered data before exposing its snapshot", context do
    directory = Path.join(context.tmp_dir, "store")
    {:ok, store, nil} = Store.open(directory: directory, io: FaultIO)
    {:ok, store} = Store.commit(store, %{value: :confirmed})
    Store.close(store)
    Process.put(:mqtt_persistence_fault, :sync)
    assert {:error, {:failed, :eio}} = Store.open(directory: directory, io: FaultIO)
    Process.delete(:mqtt_persistence_fault)
    assert {:ok, recovered, %{value: :confirmed}} = Store.open(directory: directory, io: FaultIO)
    Store.close(recovered)
  end

  defp connected!(context) do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false])
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, port} = :inet.port(listener)

    {:ok, client} =
      Client.start_link(
        client_id: "durable",
        host: "127.0.0.1",
        port: port,
        observer: self(),
        tick_interval: 60_000,
        persistence: [directory: Path.join(context.tmp_dir, "store"), io: FaultIO]
      )

    Process.unlink(client)
    on_exit(fn -> if Process.alive?(client), do: GenServer.stop(client) end)
    {:ok, socket} = :gen_tcp.accept(listener, 2_000)
    {:ok, <<0x10, _::binary>>} = :gen_tcp.recv(socket, 0, 2_000)
    :ok = :gen_tcp.send(socket, <<0x20, 3, 0, 0, 0>>)
    assert_receive {:graviton_mq, ^client, {:connected, _}}, 2_000
    {client, socket}
  end
end
