defmodule GravitonMQ.AMQP10.SessionEngineTest do
  use ExUnit.Case, async: true
  alias GravitonMQ.AMQP10.{SessionEngine, SessionState, Link, Outcome, Value, DeliveryState}
  alias GravitonMQ.AMQP10.Codec.Performative
  alias GravitonMQ.AMQP10.Performative.{Begin, Attach, Flow, Transfer, Disposition, Detach}
  alias GravitonMQ.AMQP10.Terminus.{Source, Target}

  setup do
    assert Code.ensure_loaded?(SessionEngine)
    :ok
  end

  test "new validates finite options and keeps caller identities and indexes" do
    assert {:ok, state} = SessionEngine.new(4, 2, incoming_window: 3, max_handle: 17)
    assert state.id == 4
    assert state.local_channel == 2
    assert state.incoming_window == 3
    assert state.links_by_identity == %{}

    for options <- [
          [max_links: 0],
          [max_message_bytes: :infinity],
          [incoming_window: 0x8000_0000],
          [unknown: 1],
          [max_incoming_bytes: 2, max_message_bytes: 3]
        ] do
      assert {:error, _} = SessionEngine.new(1, 0, options)
    end

    assert {:error, _} = SessionEngine.new(1, 65_536, [])
  end

  test "Begin waits for writer capacity and peer acknowledgement with negative monotonic time" do
    {:ok, state} = SessionEngine.new(4, 2, [])

    {state, effects} =
      SessionEngine.step(
        state,
        {:local_begin, :open},
        ctx(write_frames: 0, write_bytes: 0, control_frames: 0, control_bytes: 0, now_ms: -100)
      )

    assert emits(effects) == []
    refute complete?(effects, :open)
    {state, effects} = SessionEngine.step(state, :flush, ctx(now_ms: -99))
    assert [{receipt, %Begin{next_outgoing_id: %Value{value: 0}}, <<>>}] = emits(effects)
    {state, effects} = SessionEngine.step(state, {:write_result, receipt, :ok}, ctx(now_ms: -98))
    refute complete?(effects, :open)
    state = %{state | remote_channel: 19}
    {state, effects} = SessionEngine.step(state, {:peer_begin, peer_begin()}, ctx(now_ms: -97))
    assert {:complete, :open, {:ok, 4}} in effects
    assert state.next_incoming_id == 100
    assert state.remote_handle_max == 31
    assert SessionEngine.step(state, :flush, ctx(now_ms: -98)) == {state, []}
  end

  test "remote Begin emits a reply identifying its independent remote channel" do
    {:ok, state} = SessionEngine.new(7, 2, remote_channel: 19)
    {_state, effects} = SessionEngine.step(state, {:peer_begin, peer_begin()}, ctx())
    assert [{_, %Begin{remote_channel: %Value{type: :ushort, value: 19}}, <<>>}] = emits(effects)
  end

  test "sender attachment resolves different local and remote handles without copying links" do
    {state, _} = active()
    id = identity("s", :sender)

    {state, effects} =
      SessionEngine.step(
        state,
        {:command, :attach, {:attach, id, [address: Value.string("q")]}},
        ctx()
      )

    assert [{receipt, %Attach{handle: %Value{value: 0}, role: %Value{value: false}}, <<>>}] =
             emits(effects)

    {state, _} = SessionEngine.step(state, {:write_result, receipt, :ok}, ctx())
    {state, effects} = SessionEngine.step(state, {:peer, peer_attach(id, 9), <<>>}, ctx())
    assert {:complete, :attach, {:ok, id}} in effects
    assert state.local_handle_to_link == %{0 => id}
    assert state.remote_handle_to_link == %{9 => id}
    assert {:ok, link} = SessionState.fetch_incoming_link(state, 9)
    assert {:ok, ^link} = SessionState.fetch_outgoing_link(state, 0)
    assert Link.identity(link) == id
  end

  test "unsupported peer link receives Attach before Detach and is never exposed as attached" do
    {state, _} = active()
    pdu = %{peer_attach(identity("bad", :receiver), 8) | snd_settle_mode: Value.ubyte(1)}
    {_state, effects} = SessionEngine.step(state, {:peer, pdu, <<>>}, ctx())
    assert [{_, %Attach{}, <<>>}, {_, %Detach{error: error}, <<>>}] = emits(effects)
    assert error.condition == Value.symbol("amqp:not-implemented")
    refute Enum.any?(effects, &match?({:link_event, {:attached, _}}, &1))
  end

  test "unknown described Source and Target are refused before terminus normalization" do
    extension =
      Value.described(
        Value.symbol("example:terminus"),
        Value.map([{Value.null(), Value.boolean(true)}])
      )

    for role <- [:sender, :receiver], field <- [:source, :target], policy <- [:auto, :defer] do
      {state, _} = active(peer_attach_policy: policy)
      id = identity("extension", role)
      pdu = Map.put(peer_attach(id, 9), field, extension)
      {:ok, wire} = Performative.encode(pdu)
      {:ok, decoded, <<>>} = Performative.decode(wire)
      assert Map.fetch!(decoded, field) == extension
      {state, effects} = SessionEngine.step(state, {:peer, decoded, <<>>}, ctx())
      assert state.phase == :active

      assert [{_, %Attach{source: nil, target: nil}, <<>>}, {_, %Detach{error: error}, <<>>}] =
               emits(effects)

      assert error.condition == Value.symbol("amqp:not-implemented")
      refute Enum.any?(effects, &match?({:link_event, {:attached, _}}, &1))
      refute Enum.any?(effects, &match?({:link_event, {:attach_requested, _, _, _}}, &1))

      {state, _} = active(peer_attach_policy: policy)
      blocked = ctx(write_frames: 0, write_bytes: 0, control_frames: 0, control_bytes: 0)
      {state, effects} = SessionEngine.step(state, {:peer, decoded, <<>>}, blocked)
      assert emits(effects) == []

      {state, effects} =
        SessionEngine.step(
          state,
          {:peer, %Detach{handle: Value.uint(9), closed: Value.boolean(true)}, <<>>},
          ctx()
        )

      assert state.phase == :active

      # The pending Attach still needs a response, followed by the peer's close acknowledgment.
      assert [
               {_, %Attach{source: nil, target: nil, handle: %Value{value: 0}}, <<>>},
               {_, %Detach{handle: %Value{value: 0}, closed: %Value{value: true}, error: nil},
                <<>>}
             ] = emits(effects)

      assert state.links_by_identity == %{}
      assert state.local_handle_to_link == %{}
      assert state.remote_handle_to_link == %{}
      refute Enum.any?(effects, &match?({:link_event, {:attached, _}}, &1))
      refute Enum.any?(effects, &match?({:link_event, {:attach_requested, _, _, _}}, &1))
    end
  end

  test "peer attachment becomes observable only when its bounded response is emitted" do
    for role <- [:sender, :receiver] do
      {state, _} = active()
      id = identity("peer", role)
      blocked = ctx(write_frames: 0, write_bytes: 0, control_frames: 0, control_bytes: 0)
      {state, effects} = SessionEngine.step(state, {:peer, peer_attach(id, 9), <<>>}, blocked)
      assert emits(effects) == []
      refute {:link_event, {:attached, id}} in effects
      assert state.links_by_identity[id].phase == :attach_pending

      {state, effects} = SessionEngine.step(state, :flush, ctx(control_frames: 1))
      assert [{receipt, %Attach{}, <<>>}] = emits(effects)
      assert {:link_event, {:attached, id}} in effects
      assert state.links_by_identity[id].phase == :attached

      assert Enum.find_index(effects, &match?({:emit, _, _, _}, &1)) <
               Enum.find_index(effects, &(&1 == {:link_event, {:attached, id}}))

      {state, effects} = SessionEngine.step(state, {:write_result, receipt, :ok}, ctx())
      refute {:link_event, {:attached, id}} in effects
      {_, effects} = SessionEngine.step(state, :flush, ctx())
      refute {:link_event, {:attached, id}} in effects
    end
  end

  test "queued sends need credit and per-frame windows and fragment lazily within writer bounds" do
    {state, id} =
      attached(:sender,
        max_outgoing_bytes: 4096,
        max_message_bytes: 2048,
        max_incoming_bytes: 4096
      )

    raw = :binary.copy(<<17>>, 1200)
    {state, effects} = SessionEngine.step(state, {:command, :send, {:send, id, raw, []}}, ctx())
    assert emits(effects) == []

    {state, effects} =
      SessionEngine.step(
        state,
        {:peer, peer_flow(9, 1, 1), <<>>},
        ctx(write_frames: 8, max_frame_size: 512)
      )

    assert [
             {receipt,
              %Transfer{delivery_id: %Value{value: 0}, more: %Value{value: true}} = transfer,
              chunk}
           ] = data_emits(effects)

    assert byte_size(chunk) < 512
    {:ok, encoded} = Performative.encode(transfer)
    assert 8 + byte_size(encoded) + byte_size(chunk) <= 512
    assert state.next_outgoing_id == 1
    assert state.links_by_identity[id].credit == 0

    {state, effects} =
      SessionEngine.step(state, {:write_result, receipt, :ok}, ctx(max_frame_size: 512))

    assert data_emits(effects) == []

    {state, effects} =
      SessionEngine.step(
        state,
        {:peer, %{peer_flow(9, 1, 10) | next_incoming_id: Value.uint(1)}, <<>>},
        ctx(write_frames: 1, max_frame_size: 512)
      )

    assert [{_, %Transfer{delivery_id: nil}, _}] = data_emits(effects)
    assert state.next_outgoing_id == 2
  end

  test "socket acceptance and Received progress do not complete publication; terminal settlement does" do
    {state, id} = attached(:sender)
    {state, _} = SessionEngine.step(state, {:peer, peer_flow(9, 2, 10), <<>>}, ctx())

    {state, effects} =
      SessionEngine.step(state, {:command, :send, {:send, id, <<1, 2, 3>>, []}}, ctx())

    [{receipt, _, _}] = data_emits(effects)
    {state, effects} = SessionEngine.step(state, {:write_result, receipt, :ok}, ctx())
    refute complete?(effects, :send)

    progress =
      struct!(DeliveryState.Received,
        section_number: Value.uint(0),
        section_offset: Value.ulong(1)
      )

    {state, effects} =
      SessionEngine.step(state, {:peer, disposition(0, progress, false), <<>>}, ctx())

    refute complete?(effects, :send)

    {state, effects} =
      SessionEngine.step(state, {:peer, disposition(0, %Outcome.Accepted{}, false), <<>>}, ctx())

    refute complete?(effects, :send)

    {state, effects} =
      SessionEngine.step(state, {:peer, disposition(0, %Outcome.Accepted{}, true), <<>>}, ctx())

    assert {:complete, :send, {:ok, %Outcome.Accepted{}}} in effects
    assert state.outgoing_by_id == %{}
    assert state.outgoing_bytes == 0
  end

  test "receiver reserves worst-case slots and bytes before granting credit and assembles opaque fragments" do
    {state, id} =
      attached(:receiver,
        max_message_bytes: 16,
        max_incoming_bytes: 32,
        max_incoming_deliveries: 2
      )

    {state, effects} = SessionEngine.step(state, {:command, :demand, {:demand, id, 2, 32}}, ctx())
    assert {:complete, :demand, {:ok, 2}} in effects

    assert Enum.any?(emits(effects), fn {_, pdu, _} ->
             match?(%Flow{link_credit: %Value{value: 2}}, pdu)
           end)

    assert state.incoming_reserved_bytes == 32

    {state, effects} =
      SessionEngine.step(state, {:peer, transfer(9, 100, "a", true), <<1, 2>>}, ctx())

    refute Enum.any?(effects, &match?({:delivery, _, _, _, _}, &1))

    {state, effects} =
      SessionEngine.step(state, {:peer, %Transfer{handle: Value.uint(9)}, <<3, 4>>}, ctx())

    assert [{:delivery, ^id, token, <<1, 2, 3, 4>>, %{message_format: 0}}] = deliveries(effects)
    assert state.links_by_identity[id].partial == nil
    assert state.incoming_bytes == 4

    {state, effects} =
      SessionEngine.step(state, {:command, :settle, {:settle, token, %Outcome.Released{}}}, ctx())

    assert [
             {receipt,
              %Disposition{
                role: %Value{value: true},
                settled: %Value{value: true},
                state: %Outcome.Released{}
              }, <<>>}
           ] = Enum.filter(emits(effects), fn {_, p, _} -> is_struct(p, Disposition) end)

    {state, effects} = SessionEngine.step(state, {:write_result, receipt, :ok}, ctx())
    assert {:complete, :settle, {:ok, %Outcome.Released{}}} in effects
    assert state.incoming_bytes == 0
    assert state.incoming_reserved_bytes == 16
  end

  test "abort wins over more and ignores payload while releasing its reservation" do
    {state, id} = attached(:receiver, max_message_bytes: 8, max_incoming_bytes: 8)
    {state, _} = SessionEngine.step(state, {:command, :demand, {:demand, id, 1, 8}}, ctx())
    {state, _} = SessionEngine.step(state, {:peer, transfer(9, 5, "a", true), <<1, 2>>}, ctx())

    abort = %Transfer{
      handle: Value.uint(9),
      aborted: Value.boolean(true),
      more: Value.boolean(true),
      settled: Value.boolean(true)
    }

    {state, effects} = SessionEngine.step(state, {:peer, abort, :binary.copy(<<3>>, 100)}, ctx())
    assert deliveries(effects) == []
    assert state.incoming_by_id == %{}
    assert state.incoming_bytes == 0
    assert state.incoming_reserved_bytes == 0
    assert state.links_by_identity[id].partial == nil
  end

  test "disposition ranges iterate the bounded live ledger across uint wrap" do
    {state, id} = attached(:sender, initial_delivery_id: 0xFFFF_FFFF)
    {state, _} = SessionEngine.step(state, {:peer, peer_flow(9, 2, 10), <<>>}, ctx())
    {state, _} = SessionEngine.step(state, {:command, :a, {:send, id, <<1>>, []}}, ctx())
    {state, _} = SessionEngine.step(state, {:command, :b, {:send, id, <<2>>, []}}, ctx())

    pdu = %{
      disposition(0xFFFF_FFFF, %Outcome.Modified{delivery_failed?: true}, true)
      | last: Value.uint(0)
    }

    {state, effects} = SessionEngine.step(state, {:peer, pdu, <<>>}, ctx())
    assert {:complete, :a, {:ok, %Outcome.Modified{delivery_failed?: true}}} in effects
    assert {:complete, :b, {:ok, %Outcome.Modified{delivery_failed?: true}}} in effects
    assert state.outgoing_by_id == %{}

    {_state, effects} =
      SessionEngine.step(
        state,
        {:peer, %{pdu | first: Value.uint(0), last: Value.uint(0x8000_0000)}, <<>>},
        ctx()
      )

    assert {:session_error, :ambiguous_disposition_range} in effects
  end

  test "loss distinguishes never-emitted, possibly-sent and known terminal outcomes" do
    {state, id} = attached(:sender)
    {unsent, _} = SessionEngine.step(state, {:command, :unsent, {:send, id, <<1>>, []}}, ctx())
    {_state, effects} = SessionEngine.step(unsent, {:transport_down, :lost}, ctx())
    assert {:complete, :unsent, {:error, :lost}} in effects
    {state, _} = SessionEngine.step(state, {:peer, peer_flow(9, 2, 10), <<>>}, ctx())
    {sent, _} = SessionEngine.step(state, {:command, :sent, {:send, id, <<1>>, []}}, ctx())
    {_state, effects} = SessionEngine.step(sent, {:transport_down, :lost}, ctx())
    assert {:complete, :sent, {:unknown, :lost}} in effects

    {known, _} =
      SessionEngine.step(sent, {:peer, disposition(0, %Outcome.Rejected{}, false), <<>>}, ctx())

    {closed, effects} = SessionEngine.step(known, {:transport_down, :lost}, ctx())
    assert {:complete, :sent, {:error, {:outcome_known, %Outcome.Rejected{}, :lost}}} in effects
    assert SessionEngine.step(closed, :flush, ctx()) == {closed, []}
  end

  test "drain consumes unused sender credit and echo responses never request another echo" do
    {state, id} = attached(:sender)
    flow = %{peer_flow(9, 7, 10) | drain: Value.boolean(true), echo: Value.boolean(true)}
    {state, effects} = SessionEngine.step(state, {:peer, flow, <<>>}, ctx())
    assert state.links_by_identity[id].delivery_count == 7
    assert state.links_by_identity[id].credit == 0

    assert [
             {_,
              %Flow{
                delivery_count: %Value{value: 7},
                drain: %Value{value: true},
                echo: %Value{value: false}
              }, <<>>}
           ] = emits(effects)
  end

  test "broker drain waits for owner exhaustion and fences superseded confirmations" do
    {state, id} = attached(:sender, peer_attach_policy: :defer)
    flow = %{peer_flow(9, 3, 10) | drain: Value.boolean(true)}
    {state, effects} = SessionEngine.step(state, {:peer, flow, <<>>}, ctx())
    assert state.links_by_identity[id].credit == 3
    assert emits(effects) == []
    assert %{messages: 1, drain_token: token} = SessionEngine.send_capacity(state, id, ctx())

    {state, _} = SessionEngine.step(state, {:peer, flow, <<>>}, ctx())

    {state, effects} =
      SessionEngine.step(state, {:command, :stale, {:drain_exhausted, id, token}}, ctx())

    assert {:complete, :stale, {:ok, :stale}} in effects
    assert state.links_by_identity[id].credit == 3
    assert %{drain_token: current} = SessionEngine.send_capacity(state, id, ctx())
    refute current == token

    {state, _} = SessionEngine.step(state, {:command, :send, {:send, id, <<1>>, []}}, ctx())
    assert state.links_by_identity[id].credit == 2
    assert state.links_by_identity[id].delivery_count == 1

    {state, effects} =
      SessionEngine.step(state, {:command, :empty, {:drain_exhausted, id, current}}, ctx())

    assert state.links_by_identity[id].credit == 0

    assert [{_, %Flow{delivery_count: %Value{value: 3}, link_credit: %Value{value: 0}}, <<>>}] =
             emits(effects)

    assert {:complete, :empty, {:ok, :drained}} in effects
  end

  test "Flow synchronizes incoming transfer IDs and preserves outgoing windows across wrap" do
    {state, id} = attached(:sender, initial_outgoing_id: 0xFFFF_FFFF)

    flow = %{
      peer_flow(9, 2, 0)
      | next_incoming_id: Value.uint(0xFFFF_FFFF),
        next_outgoing_id: Value.uint(0xFFFF_FFFF)
    }

    {state, _} = SessionEngine.step(state, {:peer, flow, <<>>}, ctx())
    assert state.phase == :active
    assert state.next_incoming_id == 0xFFFF_FFFF
    {state, effects} = SessionEngine.step(state, {:command, :send, {:send, id, <<1>>, []}}, ctx())
    assert data_emits(effects) == []
    flow = %{flow | incoming_window: Value.uint(1), next_outgoing_id: Value.uint(0)}
    {state, effects} = SessionEngine.step(state, {:peer, flow, <<>>}, ctx())
    assert state.next_incoming_id == 0
    assert state.next_outgoing_id == 0
    assert [{_, %Transfer{}, <<1>>}] = data_emits(effects)

    {state, _} =
      SessionEngine.step(state, {:peer, %{flow | next_incoming_id: Value.uint(0)}, <<>>}, ctx())

    assert state.remote_next_incoming_id == 0

    {_state, effects} =
      SessionEngine.step(
        state,
        {:peer, %{flow | next_incoming_id: Value.uint(0x8000_0000)}, <<>>},
        ctx()
      )

    assert {:session_error, :invalid_flow} in effects
  end

  test "peer terminus replies advertise supported outcomes without echoing extension capabilities" do
    {state, _} = active()
    pdu = peer_attach(identity("negotiated", :sender), 9)

    pdu = %{
      pdu
      | source: %{pdu.source | capabilities: Value.symbol("unknown-source")},
        target: %{pdu.target | capabilities: Value.symbol("unknown-target")}
    }

    {_state, effects} = SessionEngine.step(state, {:peer, pdu, <<>>}, ctx())
    assert [{_, %Attach{source: source, target: target}, <<>>}] = emits(effects)
    assert source.capabilities == nil
    assert target.capabilities == nil
    assert source.expiry_policy == Value.symbol("session-end")
    assert target.expiry_policy == Value.symbol("session-end")

    assert source.outcomes ==
             Value.array(
               :symbol,
               Enum.map(
                 [
                   "amqp:accepted:list",
                   "amqp:rejected:list",
                   "amqp:released:list",
                   "amqp:modified:list"
                 ],
                 &Value.symbol/1
               )
             )
  end

  test "null and empty Source outcomes preserve sender authority for both Attach initiators" do
    for role <- [:sender, :receiver],
        initiator <- [:host, :peer],
        outcomes <- [nil, Value.array(:symbol, [])] do
      {state, _} = active()
      id = identity("source-authority", role)

      state =
        if initiator == :host do
          {state, effects} =
            SessionEngine.step(
              state,
              {:command, :attach, {:attach, id, [address: Value.string("q")]}},
              ctx()
            )

          [{receipt, %Attach{source: requested}, <<>>}] = emits(effects)
          assert requested.outcomes == all_outcomes()
          {state, _} = SessionEngine.step(state, {:write_result, receipt, :ok}, ctx())
          state
        else
          state
        end

      pdu = peer_attach(id, 9)
      pdu = %{pdu | source: %{pdu.source | outcomes: outcomes}}
      {state, effects} = SessionEngine.step(state, {:peer, pdu, <<>>}, ctx())

      expected =
        if role == :sender,
          do: all_outcomes(),
          else: Value.array(:symbol, [Value.symbol("amqp:accepted:list")])

      assert state.links_by_identity[id].source.outcomes == expected

      state =
        if initiator == :peer do
          [{receipt, %Attach{source: chosen}, <<>>}] = emits(effects)
          assert chosen.outcomes == expected
          {state, _} = SessionEngine.step(state, {:write_result, receipt, :ok}, ctx())
          state
        else
          state
        end

      if role == :sender do
        {state, _} = SessionEngine.step(state, {:peer, peer_flow(9, 1, 10), <<>>}, ctx())
        {state, _} = SessionEngine.step(state, {:command, :send, {:send, id, <<1>>, []}}, ctx())

        {_state, effects} =
          SessionEngine.step(
            state,
            {:peer, disposition(0, %Outcome.Released{}, true), <<>>},
            ctx()
          )

        assert {:complete, :send, {:ok, %Outcome.Released{}}} in effects
      else
        {state, _} =
          SessionEngine.step(state, {:command, :demand, {:demand, id, 1, 1_048_576}}, ctx())

        {state, effects} =
          SessionEngine.step(state, {:peer, transfer(9, 5, "tag", false), <<1>>}, ctx())

        [{:delivery, ^id, token, <<1>>, _}] = deliveries(effects)

        {state, effects} =
          SessionEngine.step(
            state,
            {:command, :release, {:settle, token, %Outcome.Released{}}},
            ctx()
          )

        assert {:complete, :release, {:error, :settlement_not_admitted}} in effects

        {_state, effects} =
          SessionEngine.step(
            state,
            {:command, :accept, {:settle, token, %Outcome.Accepted{}}},
            ctx()
          )

        assert [{_, %Disposition{state: %Outcome.Accepted{}}, <<>>}] = emits(effects)
      end
    end
  end

  test "unsupported expiry and outcome requests are refused rather than echoed" do
    for change <- [
          fn pdu -> %{pdu | source: %{pdu.source | expiry_policy: Value.symbol("never")}} end,
          fn pdu ->
            %{pdu | target: %{pdu.target | expiry_policy: Value.symbol("connection-close")}}
          end,
          fn pdu ->
            %{pdu | source: %{pdu.source | outcomes: Value.symbol("custom:outcome:list")}}
          end
        ] do
      {state, _} = active()
      pdu = change.(peer_attach(identity("unsupported", :sender), 9))
      {_state, effects} = SessionEngine.step(state, {:peer, pdu, <<>>}, ctx())
      assert [{_, %Attach{source: nil, target: nil}, <<>>}, {_, %Detach{}, <<>>}] = emits(effects)
    end
  end

  test "a peer cannot choose an outcome excluded by the negotiated Source" do
    {state, _} = active()
    id = identity("accepted-only", :sender)
    pdu = peer_attach(id, 9)
    pdu = %{pdu | source: %{pdu.source | outcomes: Value.symbol("amqp:accepted:list")}}
    {state, _} = SessionEngine.step(state, {:peer, pdu, <<>>}, ctx())
    {state, _} = SessionEngine.step(state, {:peer, peer_flow(9, 1, 10), <<>>}, ctx())
    {state, _} = SessionEngine.step(state, {:command, :send, {:send, id, <<1>>, []}}, ctx())

    {_state, effects} =
      SessionEngine.step(state, {:peer, disposition(0, %Outcome.Released{}, true), <<>>}, ctx())

    assert {:session_error, :invalid_disposition} in effects
  end

  test "our receiver respects the Source outcomes returned in an Attach reply" do
    {state, _} = active()
    id = identity("accepted-only", :receiver)

    {state, effects} =
      SessionEngine.step(
        state,
        {:command, :attach, {:attach, id, [address: Value.string("q")]}},
        ctx()
      )

    [{receipt, _, _}] = emits(effects)
    {state, _} = SessionEngine.step(state, {:write_result, receipt, :ok}, ctx())
    pdu = peer_attach(id, 9)
    pdu = %{pdu | source: %{pdu.source | outcomes: Value.symbol("amqp:accepted:list")}}
    {state, _} = SessionEngine.step(state, {:peer, pdu, <<>>}, ctx())

    {state, _} =
      SessionEngine.step(state, {:command, :demand, {:demand, id, 1, 1_048_576}}, ctx())

    {state, effects} =
      SessionEngine.step(state, {:peer, transfer(9, 5, "tag", false), <<1>>}, ctx())

    [{:delivery, ^id, token, <<1>>, _}] =
      Enum.filter(effects, &match?({:delivery, _, _, _, _}, &1))

    {state, effects} =
      SessionEngine.step(
        state,
        {:command, :release, {:settle, token, %Outcome.Released{}}},
        ctx()
      )

    assert {:complete, :release, {:error, :settlement_not_admitted}} in effects

    {_state, effects} =
      SessionEngine.step(state, {:command, :accept, {:settle, token, %Outcome.Accepted{}}}, ctx())

    assert [{_, %Disposition{state: %Outcome.Accepted{}}, <<>>}] = emits(effects)
  end

  test "early terminal settlement aborts remaining fragments and completes its authoritative outcome" do
    {state, id} = attached(:sender, max_message_bytes: 2048)
    {state, _} = SessionEngine.step(state, {:peer, peer_flow(9, 1, 10), <<>>}, ctx())

    {state, _} =
      SessionEngine.step(
        state,
        {:command, :send, {:send, id, :binary.copy(<<1>>, 1200), []}},
        ctx(write_frames: 1, max_frame_size: 512)
      )

    {state, effects} =
      SessionEngine.step(
        state,
        {:peer, disposition(0, %Outcome.Rejected{}, true), <<>>},
        ctx(write_frames: 1, max_frame_size: 512)
      )

    assert [{_, %Transfer{aborted: %Value{value: true}}, <<>>}] = data_emits(effects)
    assert {:complete, :send, {:ok, %Outcome.Rejected{}}} in effects
    assert state.outgoing_by_id == %{}
  end

  for progress <- [:none, :prior_received, :settled_received] do
    @progress progress
    test "receiver settlement without an outcome is unknown and preserves exact IDs (#{@progress})" do
      {state, id} = attached(:sender)
      {state, _} = SessionEngine.step(state, {:peer, peer_flow(9, 2, 10), <<>>}, ctx())
      {state, _} = SessionEngine.step(state, {:command, :first, {:send, id, <<1>>, []}}, ctx())
      {state, _} = SessionEngine.step(state, {:command, :second, {:send, id, <<2>>, []}}, ctx())

      received = %DeliveryState.Received{
        section_number: Value.uint(0),
        section_offset: Value.ulong(1)
      }

      {state, _} =
        if @progress == :prior_received,
          do: SessionEngine.step(state, {:peer, disposition(0, received, false), <<>>}, ctx()),
          else: {state, []}

      remote_state = if @progress == :settled_received, do: received

      {state, effects} =
        SessionEngine.step(state, {:peer, disposition(0, remote_state, true), <<>>}, ctx())

      assert state.phase == :active
      assert {:complete, :first, {:unknown, :receiver_settled_without_outcome}} in effects
      refute complete?(effects, :second)
      refute Enum.any?(effects, &match?({:session_error, _}, &1))
      assert Map.keys(state.outgoing_by_id) == [1]
      assert state.outgoing_bytes == 1
      assert map_size(state.writes) <= state.options.max_writes

      {state, effects} =
        SessionEngine.step(state, {:peer, disposition(0, nil, true), <<>>}, ctx())

      refute complete?(effects, :first)

      {state, effects} =
        SessionEngine.step(state, {:peer, disposition(1, %Outcome.Accepted{}, true), <<>>}, ctx())

      assert {:complete, :second, {:ok, %Outcome.Accepted{}}} in effects
      assert state.outgoing_by_id == %{}
      assert state.outgoing_bytes == 0
      assert state.phase == :active
    end
  end

  test "early receiver settlement without an outcome waits for a bounded abort and completes unknown once" do
    {state, id} = attached(:sender, max_message_bytes: 2048)
    {state, _} = SessionEngine.step(state, {:peer, peer_flow(9, 1, 10), <<>>}, ctx())

    {state, effects} =
      SessionEngine.step(
        state,
        {:command, :send, {:send, id, :binary.copy(<<1>>, 1200), []}},
        ctx(write_frames: 1, max_frame_size: 512)
      )

    [{first_receipt, %Transfer{more: %Value{value: true}}, _}] = data_emits(effects)

    blocked =
      ctx(
        write_frames: 0,
        write_bytes: 0,
        control_frames: 0,
        control_bytes: 0,
        max_frame_size: 512
      )

    {state, effects} =
      SessionEngine.step(state, {:peer, disposition(0, nil, true), <<>>}, blocked)

    assert state.phase == :active
    refute complete?(effects, :send)
    assert state.outgoing_by_id[0].settled
    assert state.outgoing_bytes == 1200

    {state, effects} =
      SessionEngine.step(state, :flush, ctx(write_frames: 1, max_frame_size: 512))

    [{abort_receipt, %Transfer{aborted: %Value{value: true}, more: %Value{value: false}}, <<>>}] =
      data_emits(effects)

    assert {:complete, :send, {:unknown, :receiver_settled_without_outcome}} in effects
    assert state.outgoing_by_id == %{}
    assert state.outgoing_bytes == 0
    assert state.phase == :active
    {state, effects} = SessionEngine.step(state, {:write_result, first_receipt, :ok}, ctx())
    refute complete?(effects, :send)
    {state, effects} = SessionEngine.step(state, {:write_result, abort_receipt, :ok}, ctx())
    refute complete?(effects, :send)
    assert state.phase == :active
  end

  test "partial assembly deadline is absolute, fenced and bounded by Link teardown" do
    {state, id} = attached(:receiver, operation_timeout: 10, max_message_bytes: 8)
    {state, _} = SessionEngine.step(state, {:command, :demand, {:demand, id, 1, 8}}, ctx())
    {state, effects} = SessionEngine.step(state, {:peer, transfer(9, 5, "a", true), <<1>>}, ctx())

    assert [{:deadline, key, token, 10}] =
             Enum.filter(effects, &match?({:deadline, {:partial, _}, _, _}, &1))

    {state, effects} = SessionEngine.step(state, {:timeout, key, :stale}, ctx(now_ms: 10))
    assert emits(effects) == []
    {state, effects} = SessionEngine.step(state, {:timeout, key, token}, ctx(now_ms: 10))
    assert state.phase == :active
    assert state.incoming_reserved_bytes == 0
    assert state.incoming_by_id == %{}
    assert Enum.any?(emits(effects), fn {_, pdu, _} -> is_struct(pdu, Detach) end)

    assert [{:deadline, close_key, close_token, 20}] =
             Enum.filter(effects, &match?({:deadline, {:link, _}, _, _}, &1))

    {state, effects} =
      SessionEngine.step(state, {:timeout, close_key, close_token}, ctx(now_ms: 20))

    assert state.phase == :closed
    assert {:session_error, :link_close_timeout} in effects
  end

  test "oversized receive refuses its Link and frees reserved resources while Session survives" do
    {state, id} = attached(:receiver, max_message_bytes: 4, max_incoming_bytes: 8)
    {state, _} = SessionEngine.step(state, {:command, :demand, {:demand, id, 2, 8}}, ctx())

    {state, effects} =
      SessionEngine.step(state, {:peer, transfer(9, 5, "a", false), <<1, 2, 3, 4, 5>>}, ctx())

    assert state.phase == :active
    assert state.incoming_reserved_bytes == 0
    assert state.incoming_bytes == 0

    assert [{_, %Detach{error: error}, <<>>}] =
             Enum.filter(emits(effects), fn {_, pdu, _} -> is_struct(pdu, Detach) end)

    assert error.condition == Value.symbol("amqp:link:message-size-exceeded")
  end

  test "expired unsent Attach never emits and late peer Detach cancels stale controls" do
    {state, _} = active(operation_timeout: 10)
    id = identity("waiting", :sender)

    {state, effects} =
      SessionEngine.step(
        state,
        {:command, :attach, {:attach, id, [address: Value.string("q")]}},
        ctx(control_frames: 0)
      )

    [{:deadline, key, token, 10}] = effects
    {state, effects} = SessionEngine.step(state, {:timeout, key, token}, ctx(now_ms: 10))
    assert {:complete, :attach, {:error, :timeout}} in effects
    assert emits(effects) == []
    assert state.links_by_identity == %{}
  end

  test "admitted retained binaries own their backing allocation" do
    {state, id} = attached(:sender, max_message_bytes: 128)
    large = :binary.copy(<<7>>, 1_000_000)
    tiny = binary_part(large, 2, 100)
    {state, _} = SessionEngine.step(state, {:command, :send, {:send, id, tiny, []}}, ctx())
    assert :binary.referenced_byte_size(hd(Map.values(state.outgoing_by_id)).raw) == 100
    {state, id} = attached(:receiver, max_message_bytes: 128)
    {state, _} = SessionEngine.step(state, {:command, :demand, {:demand, id, 1, 128}}, ctx())
    {state, _} = SessionEngine.step(state, {:peer, transfer(9, 5, "a", true), tiny}, ctx())
    assert [part] = state.links_by_identity[id].partial.parts
    assert :binary.referenced_byte_size(part) == 100
  end

  test "Flow omitted initial sequence and delivery count use negotiated initial counters" do
    {state, id} = attached(:sender, initial_outgoing_id: 0xFFFF_FFFF)
    flow = %{peer_flow(9, 2, 10) | next_incoming_id: nil, delivery_count: nil}
    {state, _} = SessionEngine.step(state, {:peer, flow, <<>>}, ctx())
    assert state.phase == :active
    assert state.links_by_identity[id].credit == 2
    {state, effects} = SessionEngine.step(state, {:command, :send, {:send, id, <<1>>, []}}, ctx())
    assert length(data_emits(effects)) == 1
    assert state.next_outgoing_id == 0
  end

  test "zero payload fragments still consume finite fragment allowance" do
    {state, id} = attached(:receiver, max_fragments: 2, max_message_bytes: 8)
    {state, _} = SessionEngine.step(state, {:command, :demand, {:demand, id, 1, 8}}, ctx())
    {state, _} = SessionEngine.step(state, {:peer, transfer(9, 5, "a", true), <<>>}, ctx())

    {state, _} =
      SessionEngine.step(
        state,
        {:peer, %Transfer{handle: Value.uint(9), more: Value.boolean(true)}, <<>>},
        ctx()
      )

    {state, effects} =
      SessionEngine.step(state, {:peer, %Transfer{handle: Value.uint(9)}, <<>>}, ctx())

    assert state.phase == :active
    assert state.incoming_reserved_slots == 0
    assert Enum.any?(emits(effects), fn {_, p, _} -> match?(%Detach{}, p) end)
  end

  test "receiver fragments interleave independently and refusing one Link preserves the other" do
    {state, id1} = attached(:receiver, max_message_bytes: 8, max_incoming_bytes: 32)
    id2 = identity("second", :receiver)

    {state, _} =
      SessionEngine.step(
        state,
        {:command, :attach2, {:attach, id2, [address: Value.string("q")]}},
        ctx()
      )

    {state, _} = SessionEngine.step(state, {:peer, peer_attach(id2, 10), <<>>}, ctx())
    {state, _} = SessionEngine.step(state, {:command, :d1, {:demand, id1, 1, 8}}, ctx())
    {state, _} = SessionEngine.step(state, {:command, :d2, {:demand, id2, 1, 8}}, ctx())
    {state, _} = SessionEngine.step(state, {:peer, transfer(9, 5, "a", true), <<1>>}, ctx())
    {state, _} = SessionEngine.step(state, {:peer, transfer(10, 6, "b", true), <<2>>}, ctx())

    {state, effects} =
      SessionEngine.step(
        state,
        {:peer, %Transfer{handle: Value.uint(9)}, :binary.copy(<<1>>, 8)},
        ctx()
      )

    assert state.phase == :active
    assert deliveries(effects) == []

    {state, effects} =
      SessionEngine.step(state, {:peer, %Transfer{handle: Value.uint(10)}, <<3>>}, ctx())

    assert [{:delivery, ^id2, _, <<2, 3>>, _}] = deliveries(effects)
    assert state.incoming_reserved_slots == 1
    assert state.incoming_bytes == 2
  end

  test "peer Detach removes refused Link controls and cancels Link close deadline" do
    {state, id} = attached(:receiver, max_message_bytes: 4)
    {state, _} = SessionEngine.step(state, {:command, :d, {:demand, id, 1, 4}}, ctx())

    {state, _} =
      SessionEngine.step(
        state,
        {:peer, transfer(9, 5, "a", false), <<1, 2, 3, 4, 5>>},
        ctx(control_frames: 0)
      )

    {state, effects} =
      SessionEngine.step(
        state,
        {:peer, %Detach{handle: Value.uint(9), closed: Value.boolean(true)}, <<>>},
        ctx()
      )

    assert map_size(state.links_by_identity) == 0
    assert state.deadlines == %{}

    assert [{_, %Detach{error: nil}, <<>>}] =
             Enum.filter(emits(effects), fn {_, pdu, _} -> is_struct(pdu, Detach) end)

    {state, effects} = SessionEngine.step(state, :flush, ctx())
    assert emits(effects) == []
    assert state.phase == :active
  end

  test "wire delivery IDs follow first emission when different Links have asymmetric credit" do
    {state, first} = attached(:sender)
    second = identity("second", :sender)

    {state, _} =
      SessionEngine.step(
        state,
        {:command, :attach2, {:attach, second, [address: Value.string("q")]}},
        ctx()
      )

    {state, _} = SessionEngine.step(state, {:peer, peer_attach(second, 10), <<>>}, ctx())
    {state, _} = SessionEngine.step(state, {:command, :a, {:send, first, <<1>>, []}}, ctx())
    {state, _} = SessionEngine.step(state, {:command, :b, {:send, second, <<2>>, []}}, ctx())
    {state, effects} = SessionEngine.step(state, {:peer, peer_flow(10, 1, 10), <<>>}, ctx())

    assert [{_, %Transfer{handle: %Value{value: 1}, delivery_id: %Value{value: 0}}, <<2>>}] =
             data_emits(effects)

    {state, effects} =
      SessionEngine.step(state, {:peer, disposition(0, %Outcome.Accepted{}, true), <<>>}, ctx())

    assert {:complete, :b, {:ok, %Outcome.Accepted{}}} in effects
    refute complete?(effects, :a)
    assert [{:pending, _}] = Map.keys(state.outgoing_by_id)

    {state, effects} = SessionEngine.step(state, {:peer, peer_flow(9, 1, 10), <<>>}, ctx())

    assert [{_, %Transfer{handle: %Value{value: 0}, delivery_id: %Value{value: 1}}, <<1>>}] =
             data_emits(effects)

    {state, effects} =
      SessionEngine.step(state, {:peer, disposition(1, %Outcome.Released{}, true), <<>>}, ctx())

    assert {:complete, :a, {:ok, %Outcome.Released{}}} in effects
    assert state.outgoing_by_id == %{}
  end

  test "oversized host settlement is rejected before operation admission under peer frame budget" do
    {state, id} = attached(:receiver, max_message_bytes: 8)
    {state, _} = SessionEngine.step(state, {:command, :d, {:demand, id, 1, 8}}, ctx())

    {state, effects} =
      SessionEngine.step(state, {:peer, transfer(9, 5, "a", false), <<1>>}, ctx())

    [{:delivery, ^id, token, _, _}] = deliveries(effects)

    outcome = %Outcome.Modified{
      message_annotations:
        Value.map([{Value.symbol("large"), Value.binary(:binary.copy(<<1>>, 1_000_000))}])
    }

    {state, effects} =
      SessionEngine.step(
        state,
        {:command, :settle, {:settle, token, outcome}},
        ctx(max_frame_size: 512, control_frames: 0, control_bytes: 0)
      )

    assert {:complete, :settle, {:error, :settlement_not_admitted}} in effects
    assert state.phase == :active
    assert state.operations == %{}
    assert state.incoming_by_id[5].phase == :delivered
    assert :queue.is_empty(state.control_queue)
  end

  test "both receiver owners detach unknown mandatory annotations before exposing a delivery" do
    for policy <- [:auto, :defer], key <- [Value.symbol("must-understand"), Value.ulong(7)] do
      {state, id} = attached(:receiver, peer_attach_policy: policy)

      {state, _} =
        SessionEngine.step(state, {:command, :demand, {:demand, id, 1, 1_048_576}}, ctx())

      {:ok, raw} =
        GravitonMQ.AMQP10.Codec.Message.encode_sections([
          {:message_annotations, Value.map([{key, Value.boolean(true)}])},
          {:data, Value.binary("private-message")}
        ])

      <<head::binary-size(5), tail::binary>> = raw

      {state, effects} =
        SessionEngine.step(state, {:peer, transfer(9, 5, "tag", true), head}, ctx())

      refute Enum.any?(effects, &match?({:delivery, _, _, _, _}, &1))

      {state, effects} =
        SessionEngine.step(state, {:peer, %Transfer{handle: Value.uint(9)}, tail}, ctx())

      assert [
               {_,
                %Detach{
                  error: %GravitonMQ.AMQP10.Error{
                    condition: %Value{value: "amqp:not-implemented"}
                  }
                }, <<>>}
             ] =
               Enum.filter(emits(effects), &match?({_, %Detach{}, _}, &1))

      refute Enum.any?(effects, &match?({:delivery, _, _, _, _}, &1))
      assert state.incoming_by_id == %{}
      assert state.incoming_reserved_bytes == 0
      assert state.incoming_bytes == 0
    end
  end

  test "optional annotation and floating-point body bytes stay opaque for either receiver" do
    for policy <- [:auto, :defer] do
      {state, id} = attached(:receiver, peer_attach_policy: policy)

      {state, _} =
        SessionEngine.step(state, {:command, :demand, {:demand, id, 1, 1_048_576}}, ctx())

      {:ok, prefix} =
        GravitonMQ.AMQP10.Codec.Message.encode_section(
          {:message_annotations, Value.map([{Value.symbol("x-opt-meta"), Value.boolean(true)}])}
        )

      raw = prefix <> <<0, 0x53, 0x77, 0x72, 0x3F800000::32>>

      {_state, effects} =
        SessionEngine.step(state, {:peer, transfer(9, 5, "tag", false), raw}, ctx())

      assert [{:delivery, ^id, _, ^raw, _}] =
               Enum.filter(effects, &match?({:delivery, _, _, _, _}, &1))
    end
  end

  test "late sender settlement preserves host ownership and unrelated Links" do
    {state, id} = attached(:receiver, max_message_bytes: 16, max_incoming_bytes: 32)
    {state, _} = SessionEngine.step(state, {:command, :demand, {:demand, id, 1, 16}}, ctx())

    {state, effects} =
      SessionEngine.step(state, {:peer, transfer(9, 100, "first", false), <<1, 2>>}, ctx())

    [{:delivery, ^id, token, <<1, 2>>, _}] = deliveries(effects)

    update = %Disposition{
      role: Value.boolean(false),
      first: Value.uint(100),
      settled: Value.boolean(true),
      state: %Outcome.Released{}
    }

    {state, effects} = SessionEngine.step(state, {:peer, update, <<>>}, ctx())
    assert state.phase == :active
    assert state.incoming_bytes == 2
    assert state.incoming_token_to_id[token] == 100
    refute Enum.any?(effects, &match?({:session_error, _}, &1))

    other = identity("other", :receiver)

    {state, effects} =
      SessionEngine.step(
        state,
        {:command, :other, {:attach, other, [address: Value.string("q")]}},
        ctx()
      )

    [{receipt, %Attach{}, _}] = emits(effects)
    {state, _} = SessionEngine.step(state, {:write_result, receipt, :ok}, ctx())
    {state, _} = SessionEngine.step(state, {:peer, peer_attach(other, 10), <<>>}, ctx())
    {state, _} = SessionEngine.step(state, {:command, :more, {:demand, other, 1, 16}}, ctx())

    {state, effects} =
      SessionEngine.step(state, {:peer, transfer(10, 101, "other", false), <<3>>}, ctx())

    assert [{:delivery, ^other, _, <<3>>, _}] = deliveries(effects)

    {state, effects} =
      SessionEngine.step(state, {:command, :local, {:settle, token, %Outcome.Accepted{}}}, ctx())

    assert {:complete, :local, {:ok, %Outcome.Accepted{}}} in effects
    refute Enum.any?(emits(effects), fn {_, pdu, _} -> is_struct(pdu, Disposition) end)
    assert state.phase == :active
    assert state.incoming_bytes == 1
    refute Map.has_key?(state.incoming_token_to_id, token)
  end

  test "sender settlement during fragmentation retains the partial delivery and reservation" do
    {state, id} = attached(:receiver, max_message_bytes: 16)
    {state, _} = SessionEngine.step(state, {:command, :demand, {:demand, id, 1, 16}}, ctx())

    {state, effects} =
      SessionEngine.step(state, {:peer, transfer(9, 100, "first", true), <<1>>}, ctx())

    assert deliveries(effects) == []

    update = %Disposition{
      role: Value.boolean(false),
      first: Value.uint(100),
      settled: Value.boolean(true)
    }

    {state, _} = SessionEngine.step(state, {:peer, update, <<>>}, ctx())
    assert state.phase == :active
    assert state.incoming_reserved_slots == 1
    assert state.incoming_reserved_bytes == 16
    assert state.incoming_bytes == 1
    assert state.links_by_identity[id].partial.id == 100

    {state, effects} =
      SessionEngine.step(state, {:peer, transfer(9, 100, "first", false), <<2>>}, ctx())

    [{:delivery, ^id, token, <<1, 2>>, _}] = deliveries(effects)

    {state, effects} =
      SessionEngine.step(state, {:command, :local, {:settle, token, %Outcome.Accepted{}}}, ctx())

    assert {:complete, :local, {:ok, %Outcome.Accepted{}}} in effects
    refute Enum.any?(emits(effects), fn {_, pdu, _} -> is_struct(pdu, Disposition) end)
    assert state.incoming_by_id == %{}
    assert state.incoming_reserved_slots == 0
    assert state.incoming_reserved_bytes == 0
    assert state.incoming_bytes == 0
  end

  test "sender terminal state is independent of the later host outcome" do
    {state, id} = attached(:receiver, max_message_bytes: 16)
    {state, _} = SessionEngine.step(state, {:command, :demand, {:demand, id, 1, 16}}, ctx())

    {state, effects} =
      SessionEngine.step(state, {:peer, transfer(9, 100, "first", false), <<1>>}, ctx())

    [{:delivery, ^id, token, _, _}] = deliveries(effects)

    update = %Disposition{
      role: Value.boolean(false),
      first: Value.uint(100),
      state: %Outcome.Released{}
    }

    {state, _} = SessionEngine.step(state, {:peer, update, <<>>}, ctx())
    assert state.phase == :active
    assert state.incoming_by_id[100].peer_state == %Outcome.Released{}
    refute state.incoming_by_id[100].peer_settled

    {state, effects} =
      SessionEngine.step(state, {:command, :local, {:settle, token, %Outcome.Accepted{}}}, ctx())

    [{receipt, %Disposition{state: %Outcome.Accepted{}}, _}] = emits(effects)
    {state, effects} = SessionEngine.step(state, {:write_result, receipt, :ok}, ctx())
    assert {:complete, :local, {:ok, %Outcome.Accepted{}}} in effects
    assert state.phase == :active
  end

  test "wrapped sender settlement range updates only its exact bounded delivery IDs" do
    {state, id} = attached(:receiver, max_message_bytes: 16)
    {state, _} = SessionEngine.step(state, {:command, :demand, {:demand, id, 3, 48}}, ctx())

    {state, tokens} =
      Enum.reduce([0xFFFF_FFFF, 0, 1], {state, []}, fn delivery_id, {state, tokens} ->
        {state, effects} =
          SessionEngine.step(
            state,
            {:peer, transfer(9, delivery_id, Integer.to_string(delivery_id), false), <<1>>},
            ctx()
          )

        [{:delivery, ^id, token, _, _}] = deliveries(effects)
        {state, tokens ++ [token]}
      end)

    update = %Disposition{
      role: Value.boolean(false),
      first: Value.uint(0xFFFF_FFFF),
      last: Value.uint(0),
      settled: Value.boolean(true)
    }

    {state, _} = SessionEngine.step(state, {:peer, update, <<>>}, ctx())
    assert map_size(state.incoming_by_id) == 3
    assert state.incoming_by_id[0xFFFF_FFFF].peer_settled
    assert state.incoming_by_id[0].peer_settled
    refute state.incoming_by_id[1].peer_settled

    {state, _} =
      Enum.reduce(Enum.take(tokens, 2), {state, []}, fn token, {state, _} ->
        {state, effects} =
          SessionEngine.step(
            state,
            {:command, token, {:settle, token, %Outcome.Accepted{}}},
            ctx()
          )

        assert {:complete, token, {:ok, %Outcome.Accepted{}}} in effects
        refute Enum.any?(emits(effects), fn {_, pdu, _} -> is_struct(pdu, Disposition) end)
        {state, effects}
      end)

    assert Map.keys(state.incoming_by_id) == [1]
    assert state.incoming_bytes == 1

    {state, effects} =
      SessionEngine.step(
        state,
        {:command, :third, {:settle, List.last(tokens), %Outcome.Accepted{}}},
        ctx()
      )

    [{receipt, %Disposition{first: %Value{value: 1}}, _}] = emits(effects)
    {state, _} = SessionEngine.step(state, {:write_result, receipt, :ok}, ctx())
    assert state.incoming_by_id == %{}
    assert state.incoming_reserved_slots == 0
    assert state.incoming_reserved_bytes == 0
  end

  for emitted? <- [false, true] do
    @settle_emitted emitted?
    test "sender settlement completes an already #{(@settle_emitted && "emitted") || "queued"} host settlement once" do
      {state, id} = attached(:receiver, max_message_bytes: 16)
      {state, _} = SessionEngine.step(state, {:command, :demand, {:demand, id, 1, 16}}, ctx())

      {state, effects} =
        SessionEngine.step(state, {:peer, transfer(9, 100, "first", false), <<1>>}, ctx())

      [{:delivery, ^id, token, _, _}] = deliveries(effects)

      blocked =
        if @settle_emitted,
          do: ctx(),
          else: ctx(write_frames: 0, write_bytes: 0, control_frames: 0, control_bytes: 0)

      {state, writes} =
        SessionEngine.step(
          state,
          {:command, :local, {:settle, token, %Outcome.Accepted{}}},
          blocked
        )

      {state, effects} =
        SessionEngine.step(
          state,
          {:peer,
           %Disposition{
             role: Value.boolean(false),
             first: Value.uint(100),
             settled: Value.boolean(true)
           }, <<>>},
          blocked
        )

      assert state.phase == :active
      assert {:complete, :local, {:ok, %Outcome.Accepted{}}} in effects
      assert state.incoming_by_id == %{}
      {state, effects} = SessionEngine.step(state, :flush, ctx())
      refute Enum.any?(emits(effects), fn {_, pdu, _} -> is_struct(pdu, Disposition) end)
      refute complete?(effects, :local)

      for {receipt, %Disposition{}, _} <- emits(writes) do
        {_state, effects} = SessionEngine.step(state, {:write_result, receipt, :ok}, ctx())
        refute complete?(effects, :local)
      end
    end
  end

  test "errored Links reject handle and role-correct delivery references until peer Detach" do
    for role <- [:sender, :receiver] do
      {state, identity} = attached(role)
      {state, id} = live_delivery(state, identity, role)
      error = %GravitonMQ.AMQP10.Error{condition: Value.symbol("amqp:internal-error")}

      {state, effects} =
        SessionEngine.step(state, {:command, :detach_error, {:detach, identity, error}}, ctx())

      state = acknowledge_all(state, effects)

      healthy = identity("healthy", role)

      {state, effects} =
        SessionEngine.step(
          state,
          {:command, :healthy, {:attach, healthy, [address: Value.string("q")]}},
          ctx()
        )

      state = acknowledge_all(state, effects)
      {state, effects} = SessionEngine.step(state, {:peer, peer_attach(healthy, 10), <<>>}, ctx())
      assert {:complete, :healthy, {:ok, healthy}} in effects
      assert state.phase == :active

      for pdu <- [
            peer_flow(9, 1, 100),
            transfer(9, 101, "late", false),
            peer_attach(identity, 9),
            peer_attach(identity, 11),
            %Disposition{
              role: Value.boolean(role == :sender),
              first: Value.uint(id),
              settled: Value.boolean(true)
            }
          ] do
        {closed, effects} = SessionEngine.step(state, {:peer, pdu, <<>>}, ctx())
        assert closed.phase == :closed
        assert {:session_error, :errant_link} in effects
      end

      wrong_role = %Disposition{
        role: Value.boolean(role != :sender),
        first: Value.uint(id),
        settled: Value.boolean(true)
      }

      {unchanged, effects} = SessionEngine.step(state, {:peer, wrong_role, <<>>}, ctx())
      assert unchanged.phase == :active
      refute Enum.any?(effects, &match?({:session_error, _}, &1))

      {state, _} =
        SessionEngine.step(
          state,
          {:peer, %Detach{handle: Value.uint(9), closed: Value.boolean(true)}, <<>>},
          ctx()
        )

      refute Map.has_key?(state.links_by_identity, identity)
      assert state.links_by_identity[healthy].phase == :attached

      {state, effects} =
        SessionEngine.step(
          state,
          {:command, :reuse, {:attach, identity, [address: Value.string("q")]}},
          ctx()
        )

      assert state.phase == :active
      assert [{_, %Attach{}, <<>>}] = emits(effects)
    end
  end

  test "clean Detach leaves late role-correct Disposition outside the errant-Link guard" do
    for role <- [:sender, :receiver] do
      {state, identity} = attached(role)
      {state, id} = live_delivery(state, identity, role)

      {state, effects} =
        SessionEngine.step(state, {:command, :clean_detach, {:detach, identity}}, ctx())

      state = acknowledge_all(state, effects)

      disposition = %Disposition{
        role: Value.boolean(role == :sender),
        first: Value.uint(id),
        settled: Value.boolean(true)
      }

      {state, effects} = SessionEngine.step(state, {:peer, disposition, <<>>}, ctx())
      assert state.phase == :active
      refute Enum.any?(effects, &match?({:session_error, :errant_link}, &1))
    end
  end

  test "internally refused payload retains only bounded delivery references until peer Detach" do
    {state, identity} = attached(:receiver)

    {state, _} =
      SessionEngine.step(state, {:command, :demand, {:demand, identity, 1, 1_048_576}}, ctx())

    {:ok, raw} =
      GravitonMQ.AMQP10.Codec.Message.encode_sections([
        {:message_annotations, Value.map([{Value.symbol("mandatory-unknown"), Value.uint(1)}])},
        {:data, Value.binary("body")}
      ])

    {state, effects} =
      SessionEngine.step(state, {:peer, transfer(9, 100, "refused", false), raw}, ctx())

    assert Enum.any?(emits(effects), &match?({_, %Detach{error: _}, _}, &1))
    assert state.incoming_by_id == %{}
    assert state.incoming_bytes == 0

    disposition = %Disposition{
      role: Value.boolean(false),
      first: Value.uint(100),
      settled: Value.boolean(true)
    }

    {closed, effects} = SessionEngine.step(state, {:peer, disposition, <<>>}, ctx())
    assert closed.phase == :closed
    assert {:session_error, :errant_link} in effects

    {state, _} =
      SessionEngine.step(
        state,
        {:peer, %Detach{handle: Value.uint(9), closed: Value.boolean(true)}, <<>>},
        ctx()
      )

    assert state.links_by_identity == %{}
  end

  test "error-ID markers remain bounded through uint-wrap ranges and repeated Detach/reuse" do
    for role <- [:sender, :receiver] do
      {state, identity} =
        attached(role,
          max_links: 1,
          max_incoming_deliveries: 1,
          max_outgoing_deliveries: 1,
          initial_delivery_id: 0xFFFF_FFFF
        )

      Enum.reduce(0..2, state, fn iteration, state ->
        id = rem(0xFFFF_FFFF + iteration, 0x1_0000_0000)

        state =
          case role do
            :sender ->
              {state, _} = SessionEngine.step(state, {:peer, peer_flow(9, 1, 100), <<>>}, ctx())

              {state, effects} =
                SessionEngine.step(
                  state,
                  {:command, {:send, iteration}, {:send, identity, "bounded-secret-body", []}},
                  ctx()
                )

              acknowledge_all(state, effects)

            :receiver ->
              {state, _} =
                SessionEngine.step(
                  state,
                  {:command, {:demand, iteration}, {:demand, identity, 1, 1_048_576}},
                  ctx()
                )

              {state, _} =
                SessionEngine.step(
                  state,
                  {:peer, transfer(9, id, "tag", false), "bounded-secret-body"},
                  ctx()
                )

              state
          end

        error = %GravitonMQ.AMQP10.Error{condition: Value.symbol("amqp:internal-error")}

        {state, effects} =
          SessionEngine.step(
            state,
            {:command, {:detach, iteration}, {:detach, identity, error}},
            ctx()
          )

        state = acknowledge_all(state, effects)
        marker = state.links_by_identity[identity]
        assert marker.error_detach?
        ids = if role == :sender, do: marker.error_outgoing_ids, else: marker.error_incoming_ids
        assert ids == MapSet.new([id])

        refute inspect({marker.error_incoming_ids, marker.error_outgoing_ids}) =~
                 "bounded-secret-body"

        first = rem(id + 0xFFFF_FFFF, 0x1_0000_0000)
        last = rem(id + 1, 0x1_0000_0000)

        pdu = %Disposition{
          role: Value.boolean(role == :sender),
          first: Value.uint(first),
          last: Value.uint(last),
          settled: Value.boolean(true)
        }

        {closed, effects} = SessionEngine.step(state, {:peer, pdu, <<>>}, ctx())
        assert {:session_error, :errant_link} in effects
        assert closed.links_by_identity == %{}
        {down, _} = SessionEngine.step(state, {:transport_down, :closed}, ctx())
        assert down.links_by_identity == %{}

        {state, _} =
          SessionEngine.step(
            state,
            {:peer, %Detach{handle: Value.uint(9), closed: Value.boolean(true)}, <<>>},
            ctx()
          )

        assert state.links_by_identity == %{}
        assert state.incoming_by_id == %{}
        assert state.outgoing_by_id == %{}

        {state, effects} =
          SessionEngine.step(
            state,
            {:command, {:attach, iteration}, {:attach, identity, [address: Value.string("q")]}},
            ctx()
          )

        state = acknowledge_all(state, effects)
        {state, _} = SessionEngine.step(state, {:peer, peer_attach(identity, 9), <<>>}, ctx())
        assert state.links_by_identity[identity].error_incoming_ids == MapSet.new()
        assert state.links_by_identity[identity].error_outgoing_ids == MapSet.new()
        state
      end)
    end
  end

  defp acknowledge_all(state, effects),
    do:
      Enum.reduce(emits(effects), state, fn {receipt, _, _}, state ->
        elem(SessionEngine.step(state, {:write_result, receipt, :ok}, ctx()), 0)
      end)

  defp live_delivery(state, identity, :sender) do
    {state, _} = SessionEngine.step(state, {:peer, peer_flow(9, 1, 100), <<>>}, ctx())

    {state, effects} =
      SessionEngine.step(state, {:command, :send, {:send, identity, "body", []}}, ctx())

    {acknowledge_all(state, effects), 0}
  end

  defp live_delivery(state, identity, :receiver) do
    {state, _} =
      SessionEngine.step(state, {:command, :demand, {:demand, identity, 1, 1_048_576}}, ctx())

    {state, _} =
      SessionEngine.step(state, {:peer, transfer(9, 100, "live", false), "body"}, ctx())

    {state, 100}
  end

  defp ctx(options \\ []) do
    Map.new(
      Keyword.merge(
        [
          now_ms: 0,
          generation: 0,
          max_frame_size: 1024,
          max_handle: 31,
          write_frames: 16,
          write_bytes: 65536,
          control_frames: 16,
          control_bytes: 65536
        ],
        options
      )
    )
  end

  defp active(options \\ []) do
    {:ok, state} = SessionEngine.new(1, 0, options)
    {state, effects} = SessionEngine.step(state, {:local_begin, :begin}, ctx())
    [{receipt, _, _}] = emits(effects)
    {state, _} = SessionEngine.step(state, {:write_result, receipt, :ok}, ctx())
    SessionEngine.step(%{state | remote_channel: 3}, {:peer_begin, peer_begin()}, ctx())
  end

  defp attached(role, options \\ []) do
    {state, _} = active(options)
    id = identity("link", role)

    {state, effects} =
      SessionEngine.step(
        state,
        {:command, :attach, {:attach, id, [address: Value.string("q")]}},
        ctx()
      )

    [{receipt, _, _}] = emits(effects)
    {state, _} = SessionEngine.step(state, {:write_result, receipt, :ok}, ctx())
    {state, _} = SessionEngine.step(state, {:peer, peer_attach(id, 9), <<>>}, ctx())
    {state, id}
  end

  defp peer_begin,
    do: %Begin{
      next_outgoing_id: Value.uint(100),
      incoming_window: Value.uint(100),
      outgoing_window: Value.uint(100),
      handle_max: Value.uint(31)
    }

  defp identity(name, role), do: {Value.string(name), role}

  defp all_outcomes,
    do:
      Value.array(
        :symbol,
        Enum.map(
          [
            "amqp:accepted:list",
            "amqp:rejected:list",
            "amqp:released:list",
            "amqp:modified:list"
          ],
          &Value.symbol/1
        )
      )

  defp peer_attach({name, role}, handle),
    do: %Attach{
      name: name,
      handle: Value.uint(handle),
      role: Value.boolean(role == :sender),
      snd_settle_mode: Value.ubyte(0),
      rcv_settle_mode: Value.ubyte(0),
      source: %Source{address: Value.string("q"), outcomes: all_outcomes()},
      target: %Target{address: Value.string("q")},
      initial_delivery_count: if(role == :receiver, do: Value.uint(0))
    }

  defp peer_flow(handle, credit, window),
    do: %Flow{
      next_incoming_id: Value.uint(0),
      incoming_window: Value.uint(window),
      next_outgoing_id: Value.uint(100),
      outgoing_window: Value.uint(100),
      handle: Value.uint(handle),
      delivery_count: Value.uint(0),
      link_credit: Value.uint(credit),
      drain: Value.boolean(false),
      echo: Value.boolean(false)
    }

  defp transfer(handle, id, tag, more),
    do: %Transfer{
      handle: Value.uint(handle),
      delivery_id: Value.uint(id),
      delivery_tag: Value.binary(tag),
      message_format: Value.uint(0),
      more: Value.boolean(more)
    }

  defp disposition(id, outcome, settled),
    do: %Disposition{
      role: Value.boolean(true),
      first: Value.uint(id),
      state: outcome,
      settled: Value.boolean(settled)
    }

  defp emits(effects),
    do: for({:emit, receipt, pdu, payload} <- effects, do: {receipt, pdu, payload})

  defp data_emits(effects),
    do: Enum.filter(emits(effects), fn {_, pdu, _} -> is_struct(pdu, Transfer) end)

  defp deliveries(effects), do: Enum.filter(effects, &match?({:delivery, _, _, _, _}, &1))
  defp complete?(effects, op), do: Enum.any?(effects, &match?({:complete, ^op, _}, &1))
end
