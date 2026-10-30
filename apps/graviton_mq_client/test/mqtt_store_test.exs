defmodule GravitonMQ.MQTTClient.StoreTest do
  use ExUnit.Case, async: false
  alias GravitonMQ.Client.MQTT.Store
  alias GravitonMQ.Core.CheckpointCodec, as: Codec
  @moduletag :tmp_dir

  test "real synchronized checkpoints recover with one inode owner, including aliases", context do
    assert Code.ensure_loaded?(Store)
    directory = Path.join(context.tmp_dir, "store")
    assert {:ok, store, nil} = Store.open(directory: directory)
    assert byte_size(store.id) == 16
    assert {:ok, store} = Store.commit(store, %{version: 1, value: <<1, 2, 3>>})
    assert {:error, :locked} = Store.open(directory: directory <> "/../store")
    assert :ok = Store.close(store)
    assert {:ok, recovered, %{version: 1, value: <<1, 2, 3>>}} = Store.open(directory: directory)
    assert recovered.id == store.id
    assert recovered.sequence == 1
    assert :ok = Store.close(recovered)
  end

  test "owner death releases kernel lock without deleting a lockfile", context do
    assert Code.ensure_loaded?(Store)
    directory = Path.join(context.tmp_dir, "store")
    parent = self()

    owner =
      spawn(fn ->
        {:ok, _store, nil} = Store.open(directory: directory)
        send(parent, :locked)

        receive do
          :stop -> :ok
        end
      end)

    assert_receive :locked, 5_000
    assert {:error, :locked} = Store.open(directory: directory)
    monitor = Process.monitor(owner)
    Process.exit(owner, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^owner, :killed}
    assert {:ok, store, nil} = Store.open(directory: directory)
    Store.close(store)
  end

  test "checksum corruption is rejected and input never creates nested parent directories",
       context do
    assert Code.ensure_loaded?(Store)
    assert {:error, _} = Store.open(directory: Path.join(context.tmp_dir, "missing/child"))
    directory = Path.join(context.tmp_dir, "store")
    {:ok, store, nil} = Store.open(directory: directory)
    {:ok, store} = Store.commit(store, %{version: 1})
    Store.close(store)
    path = Path.join(directory, "checkpoint")
    wire = File.read!(path)

    File.write!(
      path,
      binary_part(wire, 0, byte_size(wire) - 1) <> <<Bitwise.bxor(:binary.last(wire), 1)>>
    )

    assert {:error, :corrupt_checkpoint} = Store.open(directory: directory)
  end

  test "ETF is bounded before decoding and cannot serialize runtime handles" do
    assert Code.ensure_loaded?(Codec)

    for term <- [self(), make_ref(), fn -> :ok end, %{nested: self()}] do
      assert {:error, _} = Codec.encode(term, max_bytes: 4096)
    end

    assert {:error, _} =
             Codec.decode(
               :erlang.term_to_binary(%{value: :binary.copy("x", 1000)}, compressed: 9),
               []
             )

    assert {:error, _} = Codec.decode(<<131, 108, 0xFFFFFFFF::32, 106>>, max_items: 32)
    assert {:error, _} = Codec.decode(:erlang.term_to_binary([[[[1]]]]), max_depth: 2)
    assert {:error, _} = Codec.decode(:erlang.term_to_binary(self()), [])
    assert {:ok, wire} = Codec.encode(%{value: [1, 2, <<3>>]}, [])
    assert {:ok, %{value: [1, 2, <<3>>]}} = Codec.decode(wire, [])
  end
end
