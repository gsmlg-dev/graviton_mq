defmodule GravitonMQ.Storage.WALTest do
  use ExUnit.Case, async: true
  @moduletag :capture_log

  alias GravitonMQ.Core.{CommitRef, Message, MessageId, NodeId, Outcome}
  alias GravitonMQ.Queue.{Event, EventId, Machine}
  alias GravitonMQ.Storage.WAL

  setup do
    Process.flag(:trap_exit, true)
    parent = Path.join(System.tmp_dir!(), "graviton-wal-#{System.unique_integer([:positive])}")
    File.mkdir_p!(parent)
    on_exit(fn -> File.rm_rf!(parent) end)
    {:ok, path: Path.join(parent, "log")}
  end

  test "event append establishes order, synchronization establishes durability, and reopen streams",
       %{path: path} do
    assert {:ok, wal} = WAL.start_link(path: path, stream_id: "queue/orders")
    assert {:ok, :none} = WAL.durable_through(wal)
    assert {:ok, %CommitRef{position: 2} = last} = WAL.append(wal, [event(1), event(2)])
    assert {:ok, :none} = WAL.durable_through(wal)
    assert {:ok, ^last} = WAL.sync(wal, last)
    assert {:ok, ^last} = WAL.durable_through(wal)
    assert :ok = WAL.close(wal)

    assert {:ok, wal} = WAL.start_link(path: path, stream_id: "queue/orders")
    assert {:ok, ^last} = WAL.durable_through(wal)

    assert {:ok, [{^last, second}]} =
             WAL.fold(wal, CommitRef.new("queue/orders", 1), [], fn ref, item, acc ->
               {:cont, [{ref, item} | acc]}
             end)

    assert second == event(2)
    assert :ok = WAL.close(wal)
  end

  test "opaque records retain binary content and reject the logical event API", %{path: path} do
    assert {:ok, wal} = WAL.start_link(path: path, stream_id: "mqtt/broker", mode: :opaque)
    assert {:error, :stream_mode} = WAL.append(wal, [event(1)])
    assert {:error, :stream_mode} = WAL.fold(wal, :origin, [], fn _, _, acc -> {:cont, acc} end)
    assert {:ok, ref} = WAL.append_records(wal, [{"mqtt-broker-snapshot/v1", <<0, 255, 128>>}])
    assert {:ok, ^ref} = WAL.sync(wal, ref)

    assert {:ok, [{ref, "mqtt-broker-snapshot/v1", <<0, 255, 128>>}]} ==
             WAL.fold_records(wal, :origin, [], fn r, t, b, acc -> {:cont, [{r, t, b} | acc]} end)

    assert :ok = WAL.close(wal)
  end

  test "event and opaque APIs cannot cross modes", %{path: path} do
    assert {:ok, wal} = WAL.start_link(path: path, stream_id: "events")
    assert {:error, :stream_mode} = WAL.append_records(wal, [{"type", "bytes"}])

    assert {:error, :stream_mode} =
             WAL.fold_records(wal, :origin, [], fn _, _, _, acc -> {:cont, acc} end)

    assert :ok = WAL.close(wal)
    assert {:error, :stream_mode} = WAL.start_link(path: path, stream_id: "events", mode: :opaque)
  end

  test "a live directory is exclusively owned even through a symbolic alias", %{path: path} do
    assert {:ok, wal} = WAL.start_link(path: path, stream_id: "events")
    assert {:error, :already_open} = WAL.start_link(path: path, stream_id: "events")
    alias_path = path <> "-alias"
    File.ln_s!(path, alias_path)
    assert {:error, :already_open} = WAL.start_link(path: alias_path, stream_id: "events")
    assert :ok = WAL.close(wal)
    assert {:ok, reopened} = WAL.start_link(path: alias_path, stream_id: "events")
    assert :ok = WAL.close(reopened)
  end

  test "references must belong to the existing stream and cursor", %{path: path} do
    assert {:ok, wal} = WAL.start_link(path: path, stream_id: "events")
    assert {:ok, ref} = WAL.append(wal, [event(1)])
    assert {:error, :invalid_reference} = WAL.sync(wal, CommitRef.new("different", 1))
    assert {:error, :invalid_reference} = WAL.sync(wal, CommitRef.new("events", 2))

    assert {:error, :invalid_reference} =
             WAL.fold(wal, CommitRef.new("events", 2), [], fn _, _, acc -> {:cont, acc} end)

    assert {:ok, ^ref} = WAL.sync(wal, ref)
    assert :ok = WAL.close(wal)
    assert {:error, :stream_id} = WAL.start_link(path: path, stream_id: "different")
  end

  test "bounded event validation rejects runtime values before writing", %{path: path} do
    assert {:ok, wal} = WAL.start_link(path: path, stream_id: "events", max_record_bytes: 256)
    assert {:error, :invalid_event} = WAL.append(wal, [%{event(1) | data: %{"pid" => self()}}])

    assert {:error, :record_limit} =
             WAL.append(wal, [%{event(1) | data: %{"blob" => :binary.copy(<<0>>, 257)}}])

    assert {:ok, %CommitRef{position: 1}} = WAL.append(wal, [event(1)])
    assert :ok = WAL.close(wal)
  end

  test "fold early halt returns only the first logical event", %{path: path} do
    assert {:ok, wal} = WAL.start_link(path: path, stream_id: "events")
    assert {:ok, _} = WAL.append(wal, [event(1), event(2)])
    # Recovery has already validated the stream. Incremental fold must stop at
    # the first event without reading or decoding the remaining tail.
    assert {:ok, [first]} =
             WAL.fold(wal, :origin, [], fn _, item, acc -> {:halt, [item | acc]} end)

    assert first == event(1)
    assert :ok = WAL.close(wal)
  end

  test "real queue event histories preserve stable identities and authoritative encoded content",
       %{path: path} do
    assert {:ok, state} =
             Machine.new(NodeId.new("queue/orders"), incarnation: "stable-incarnation")

    message = %Message{
      id: MessageId.new("original"),
      encoded_content: <<0, 255, 128, 0>>,
      wire_format: %Message.Format{family: "amqp10", identifier: 0},
      index: %Message.Index{}
    }

    {state, effects} =
      Enum.reduce(
        [
          {:enqueue, :put, message},
          {:register_consumer, :register, "reader", 1},
          {:claim, :pull, "reader", 1, 1, 4096}
        ],
        {state, []},
        fn command, {state, effects} ->
          {next, more} = Machine.apply(state, command)
          {next, effects ++ more}
        end
      )

    [{:delivery, delivery, ^message, _}] = Enum.filter(effects, &match?({:delivery, _, _, _}, &1))

    {_state, settlement} =
      Machine.apply(
        state,
        {:settle, :release, "reader", 1, delivery.id, %Outcome.MakeAvailable{}}
      )

    events = for {:event, item} <- effects ++ settlement, do: item
    assert length(events) == 4
    assert {:ok, wal} = WAL.start_link(path: path, stream_id: "queue/orders")
    assert {:ok, ref} = WAL.append(wal, events)
    assert {:ok, ^ref} = WAL.sync(wal, ref)
    assert :ok = WAL.close(wal)
    assert {:ok, wal} = WAL.start_link(path: path, stream_id: "queue/orders")

    assert {:ok, ^events} =
             WAL.fold(wal, :origin, [], fn _, item, acc -> {:cont, acc ++ [item]} end)

    assert hd(events).data["encoded_content"] == message.encoded_content
    assert :ok = WAL.close(wal)
  end

  test "invalid and deeply nested data never becomes a partially accepted batch", %{path: path} do
    assert {:ok, wal} = WAL.start_link(path: path, stream_id: "events")
    deep = Enum.reduce(1..34, nil, fn _, acc -> [acc] end)

    for data <- [
          %{:atom_key => 1},
          %{"ref" => make_ref()},
          %{"struct" => %{} |> Map.put(:__struct__, Event)},
          %{"deep" => deep},
          %{"improper" => [1 | :tail]},
          %{"wide_integer" => 1_208_925_819_614_629_174_706_176}
        ] do
      assert {:error, :invalid_event} = WAL.append(wal, [event(1), %{event(2) | data: data}])
      assert {:ok, []} = WAL.fold(wal, :origin, [], fn _, item, acc -> {:cont, [item | acc]} end)
    end

    assert {:ok, %CommitRef{position: 1}} = WAL.append(wal, [event(1)])
    assert :ok = WAL.close(wal)
  end

  test "bad options and absent parents fail explicitly without recursively creating directories",
       %{path: path} do
    for options <- [
          [path: "relative", stream_id: "s"],
          [path: path, stream_id: ""],
          [path: path, stream_id: "s", mode: :other],
          [path: path, stream_id: "s", max_record_bytes: 0],
          [path: path, stream_id: "s", unknown: true],
          [path: path, path: path, stream_id: "s"]
        ] do
      assert {:error, :invalid_options} = WAL.start_link(options)
    end

    assert {:error, {:directory, :enoent}} =
             WAL.start_link(path: Path.join(path, "missing-parent/leaf"), stream_id: "s")

    refute File.exists?(path)
  end

  test "invalid reducers fail explicitly while the owner remains usable", %{path: path} do
    assert {:ok, wal} = WAL.start_link(path: path, stream_id: "events")
    assert {:ok, ref} = WAL.append(wal, [event(1)])
    assert {:error, :invalid_reducer_result} = WAL.fold(wal, :origin, [], fn _, _, _ -> :bad end)

    assert {:error, :reducer_failed} =
             WAL.fold(wal, :origin, [], fn _, _, _ -> raise "reducer failed" end)

    assert {:ok, ^ref} = WAL.sync(wal, ref)
    assert :ok = WAL.close(wal)
  end

  defp event(sequence) do
    %Event{
      id: EventId.new("event-#{sequence}"),
      node_id: NodeId.new("queue/orders"),
      sequence: sequence,
      type: "message_enqueued",
      data: %{
        "encoded_content" => <<0, 255>>,
        "values" => [nil, true, false, -4, 18_446_744_073_709_551_615, 1.5]
      }
    }
  end
end
