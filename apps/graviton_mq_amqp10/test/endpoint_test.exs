defmodule GravitonMQ.AMQP10.EndpointTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.{ConnectionState, Endpoint, Frame, Outcome, SessionState, Value}
  alias GravitonMQ.AMQP10.Codec.{Performative, ProtocolHeader}

  alias GravitonMQ.AMQP10.Performative.{
    Detach,
    Attach,
    Begin,
    Close,
    Disposition,
    End,
    Flow,
    Open,
    Transfer
  }

  alias GravitonMQ.AMQP10.Terminus.{Source, Target}

  defp value(type, value), do: %Value{type: type, value: value}

  defp step(state, event, now \\ 0),
    do: Endpoint.step(state, %{generation: state.generation, now_ms: now, event: event})

  defp frame(performative, channel \\ 0, payload \\ <<>>) do
    {:ok, body} = Performative.encode(performative)
    body = body <> payload

    %Frame{
      declared_size: 8 + byte_size(body),
      data_offset_words: 2,
      type: :amqp,
      channel: channel,
      extended_header: <<>>,
      body: body
    }
  end

  defp header, do: %ProtocolHeader{protocol_id: 0, major: 1, minor: 0, revision: 0}

  defp peer_open(opts \\ []),
    do: struct!(Open, Keyword.merge([container_id: value(:string, "peer")], opts))

  defp new(opts \\ []), do: Endpoint.new(Keyword.merge([container_id: "local"], opts))

  defp connected(opts \\ []) do
    {:ok, state} = new(opts)
    {state, effects} = step(state, :start)
    state = acknowledge(state, effects)
    {state, []} = step(state, {:header, header()})
    {state, effects} = step(state, {:frame, frame(peer_open())})
    {state, effects}
  end

  defp acknowledge(state, effects) do
    Enum.reduce(effects, state, fn
      {:send_header, _, id, _}, state -> elem(step(state, {:write_result, id, :ok}), 0)
      {:send_frame, _, id, _, _, _}, state -> elem(step(state, {:write_result, id, :ok}), 0)
      _, state -> state
    end)
  end

  defp active_session(state) do
    {state, effects} = step(state, {:command, :open1, {:open_session, 10, []}})

    {:send_frame, _, _, local_channel, %Begin{}, _} =
      Enum.find(effects, &match?({:send_frame, _, _, _, %Begin{}, _}, &1))

    peer = %Begin{
      remote_channel: value(:ushort, local_channel),
      next_outgoing_id: value(:uint, 0),
      incoming_window: value(:uint, 16),
      outgoing_window: value(:uint, 16)
    }

    {state, _} = step(state, {:frame, frame(peer, 19)})
    state
  end

  test "options reject unsupported and unbounded profiles" do
    assert {:error, _} = new(max_frame_size: 511)
    assert {:error, _} = new(max_sessions: 0)
    assert {:error, _} = new(container_id: "")
    assert {:error, _} = new(sasl: true)
    assert {:ok, state} = new(generation: 7)
    assert state.generation == 7
  end

  test "negative monotonic times start and advance normally" do
    {:ok, state} = new()
    {state, effects} = step(state, :start, -10_000)
    assert state.now_ms == -10_000
    assert Enum.any?(effects, &match?({:wake_at, _, :open, _, 0}, &1))
    assert {^state, []} = step(state, {:transport_down, :old}, -10_001)
  end

  test "unsupported host remote mapping and malformed defaults are rejected" do
    assert {:error, _} = new(session_options: [remote_channel: 9])
    assert {:error, _} = new(hostname: 1)

    assert {:error, :open_exceeds_initial_frame_limit} =
             new(hostname: String.duplicate("h", 1000))
  end

  test "start reserves distinct bounded header and Open writes" do
    {:ok, state} = new()
    {state, effects} = step(state, :start, 100)

    assert [
             {:send_header, 0, header_id, <<"AMQP", 0, 1, 0, 0>>},
             {:send_frame, 0, open_id, 0, %Open{}, <<>>} | _
           ] = effects

    refute header_id == open_id
    assert map_size(state.writes) == 2
    assert {^state, []} = step(state, :start, 100)
    assert Enum.any?(effects, &match?({:wake_at, 0, :open, _, _}, &1))
  end

  test "connection opens only after the independent peer header and Open" do
    {state, effects} = connected()
    assert state.phase == :active
    assert state.connection.remote_container_id == value(:string, "peer")
    assert {:event, {:connected, "peer"}} in effects
    {_, effects} = step(state, {:frame, frame(peer_open())})
    assert Enum.any?(effects, &match?({:send_frame, _, _, 0, %Close{error: %{}}, <<>>}, &1))
  end

  test "first remote frame must be Open and complete performative bodies reject trailing data" do
    {:ok, state} = new()
    {state, _} = step(state, :start)
    {state, []} = step(state, {:header, header()})
    {_, effects} = step(state, {:frame, frame(%Close{})})
    assert Enum.any?(effects, &match?({:send_frame, _, _, 0, %Close{error: %{}}, _}, &1))
    {_, effects} = step(state, {:frame, frame(peer_open(), 0, <<0x40>>)})
    assert Enum.any?(effects, &match?({:send_frame, _, _, 0, %Close{error: %{}}, _}, &1))
  end

  test "local and peer channel limits remain directional" do
    {state, _} = connected(channel_max: 2)

    invalid = %Frame{
      declared_size: 8,
      data_offset_words: 2,
      type: :amqp,
      channel: 3,
      extended_header: <<>>,
      body: <<>>
    }

    {_, effects} = step(state, {:frame, invalid})
    assert Enum.any?(effects, &match?({:send_frame, _, _, _, %Close{error: %{}}, _}, &1))
  end

  test "outgoing frame limits use the peer receive limit independently of our receive limit" do
    {state, _} = connected(max_frame_size: 512)
    assert state.limits.max_frame_size == 512
    assert Endpoint.outgoing_limits(state).max_frame_size == 0xFFFF_FFFF
  end

  test "Begin response binds independent channels and completes the admitted operation" do
    {state, _} = connected()
    {state, effects} = step(state, {:command, :open1, {:open_session, 10, []}})

    assert {:send_frame, 0, _, local_channel, %Begin{remote_channel: nil}, <<>>} =
             Enum.find(effects, &match?({:send_frame, _, _, _, %Begin{}, _}, &1))

    peer = %Begin{
      remote_channel: value(:ushort, local_channel),
      next_outgoing_id: value(:uint, 9),
      incoming_window: value(:uint, 12),
      outgoing_window: value(:uint, 13)
    }

    {state, effects} = step(state, {:frame, frame(peer, 19)})
    assert {:operation, :open1, {:ok, 10}} in effects

    assert {:ok, session} =
             ConnectionState.fetch_outgoing_session(state.connection, local_channel)

    assert {:ok, ^session} = ConnectionState.fetch_incoming_session(state.connection, 19)
    assert session.id == 10
    refute session.local_channel == session.remote_channel
  end

  test "remote initiated Begin gets bounded independently allocated channel" do
    {state, _} = connected()

    peer = %Begin{
      next_outgoing_id: value(:uint, 4),
      incoming_window: value(:uint, 20),
      outgoing_window: value(:uint, 20)
    }

    {state, effects} = step(state, {:frame, frame(peer, 7)})

    assert {:send_frame, 0, _, local_channel, %Begin{remote_channel: %Value{value: 7}}, <<>>} =
             Enum.find(effects, &match?({:send_frame, _, _, _, %Begin{}, _}, &1))

    assert local_channel != 7
    assert {:ok, _session} = ConnectionState.fetch_incoming_session(state.connection, 7)
  end

  test "local host cannot preassign a remote channel" do
    {state, _} = connected()
    {state, effects} = step(state, {:command, :a, {:open_session, 1, [remote_channel: 4]}})
    assert {:operation, :a, {:error, :remote_channel_is_peer_owned}} in effects
    assert state.connection.sessions_by_identity == %{}
  end

  test "End retains mapping until acknowledgment and rejects a second end operation" do
    {state, _} = connected()
    state = active_session(state)
    {state, effects} = step(state, {:command, :end1, {:end_session, 10}})
    assert Enum.any?(effects, &match?({:send_frame, _, _, _, %End{}, <<>>}, &1))
    assert {:ok, _} = ConnectionState.fetch_incoming_session(state.connection, 19)
    {state, effects} = step(state, {:command, :end2, {:end_session, 10}})
    assert {:operation, :end2, {:error, :session_not_active}} in effects
    {state, effects} = step(state, {:frame, frame(%End{}, 19)})
    assert {:operation, :end1, {:ok, 10}} in effects
    assert :error = ConnectionState.fetch_incoming_session(state.connection, 19)
    assert state.session_controls == %{}
  end

  test "session limit rejects admission without adding pending operations" do
    {state, _} = connected(max_sessions: 1)
    {state, _} = step(state, {:command, :a, {:open_session, 1, []}})
    {state, effects} = step(state, {:command, :b, {:open_session, 2, []}})
    assert {:operation, :b, {:error, :session_limit}} in effects
    assert map_size(state.connection.sessions_by_identity) == 1
  end

  test "stale generation and unknown write completions have no effects" do
    {state, _} = connected(generation: 8)

    assert {^state, []} =
             Endpoint.step(state, %{generation: 7, now_ms: 500, event: {:transport_down, :old}})

    assert {^state, []} = step(state, {:write_result, :unknown, :ok})
  end

  test "session timeout after emitted Begin is unknown; transport loss clears finite state" do
    {state, _} = connected()
    {state, effects} = step(state, {:command, :a, {:open_session, 1, []}}, 20)

    assert {:wake_at, 0, {:operation, :a}, token, due} =
             Enum.find(effects, &match?({:wake_at, _, {:operation, :a}, _, _}, &1))

    assert {ignored, []} = step(state, {:timeout, {:operation, :a}, token + 1}, due)
    assert ignored == %{state | now_ms: due}
    {state, effects} = step(state, {:timeout, {:operation, :a}, token}, due)
    assert {:operation, :a, {:unknown, :timeout}} in effects
    assert Enum.any?(effects, &match?({:send_frame, _, _, _, %End{}, <<>>}, &1))
    assert Map.has_key?(state.connection.sessions_by_identity, 1)
    {state, effects} = step(state, {:transport_down, :closed}, due)
    assert state.phase == :closed
    assert state.writes == %{}
    assert Enum.any?(effects, &match?({:event, {:disconnected, :closed}}, &1))
  end

  test "End acknowledgment timeout closes the connection before channel reuse" do
    {state, _} = connected()
    state = active_session(state)
    {state, effects} = step(state, {:command, :end1, {:end_session, 10}})

    assert {:wake_at, 0, {:session_close, 10}, token, due} =
             Enum.find(effects, &match?({:wake_at, _, {:session_close, 10}, _, _}, &1))

    {state, effects} = step(state, {:timeout, {:session_close, 10}, token}, due)
    assert state.phase == :closing
    assert Map.has_key?(state.connection.sessions_by_identity, 10)
    assert Enum.any?(effects, &match?({:send_frame, _, _, 0, %Close{}, _}, &1))
  end

  test "peer idle interval drives heartbeat and receive deadline uses received frames" do
    {:ok, state} = new(idle_timeout_ms: 1000)
    {state, effects} = step(state, :start)
    state = acknowledge(state, effects)
    {state, []} = step(state, {:header, header()})

    {state, effects} =
      step(state, {:frame, frame(peer_open(idle_time_out: value(:uint, 100)))}, 10)

    assert {:wake_at, 0, :heartbeat, token, due} =
             Enum.find(effects, &match?({:wake_at, _, :heartbeat, _, _}, &1))

    {state, effects} = step(state, {:timeout, :heartbeat, token}, due)
    assert Enum.any?(effects, &match?({:send_frame, 0, _, 0, nil, <<>>}, &1))
    assert state.last_received_ms == 10
  end

  test "empty frames are accepted on any valid channel after Open" do
    {state, _} = connected()

    empty = %Frame{
      declared_size: 8,
      data_offset_words: 2,
      type: :amqp,
      channel: 17,
      extended_header: <<>>,
      body: <<>>
    }

    {state, effects} = step(state, {:frame, empty}, 5)
    assert state.phase == :active
    refute Enum.any?(effects, &match?({:send_frame, _, _, _, %Close{}, _}, &1))
  end

  test "local Close completes only at remote acknowledgment and closes transport" do
    {state, _} = connected()
    {state, effects} = step(state, {:command, :close, :close})
    assert Enum.any?(effects, &match?({:send_frame, _, _, 0, %Close{}, <<>>}, &1))
    refute {:operation, :close, {:ok, :closed}} in effects
    {state, effects} = step(state, {:frame, frame(%Close{})})
    assert {:operation, :close, {:ok, :closed}} in effects
    assert {:close_transport, 0, :normal} in effects
    assert state.phase == :closed
  end

  test "either role accepts peer Close on an unassigned negotiated channel" do
    for role <- [:client, :broker], channel <- [1, 7] do
      {state, _} = connected(role: role, channel_max: 7)
      {state, effects} = step(state, {:frame, frame(%Close{}, channel)})
      assert Enum.any?(effects, &match?({:send_frame, _, _, 0, %Close{error: nil}, <<>>}, &1))
      state = acknowledge(state, effects)
      assert state.phase == :closed
    end
  end

  test "peer Close acknowledges a local Close on any negotiated channel" do
    for role <- [:client, :broker] do
      {state, _} = connected(role: role, channel_max: 7)
      {state, _} = step(state, {:command, :close, :close})
      {state, effects} = step(state, {:frame, frame(%Close{}, 7)})
      assert {:operation, :close, {:ok, :closed}} in effects
      assert {:close_transport, 0, :normal} in effects
      assert state.phase == :closed
    end
  end

  test "closing transport after emitted Close reports uncertainty" do
    {state, _} = connected()
    {state, _} = step(state, {:command, :close, :close})
    {_, effects} = step(state, {:transport_down, :reset})
    assert {:operation, :close, {:unknown, :reset}} in effects
  end

  test "decode errors produce a protocol Close and a bounded close deadline" do
    {state, _} = connected()
    error = GravitonMQ.AMQP10.Codec.Error.new(:frame_decode, :malformed, :invalid_data_offset)
    {state, effects} = step(state, {:decode_error, error})
    assert state.phase == :closing
    assert Enum.any?(effects, &match?({:send_frame, _, _, 0, %Close{error: %{}}, _}, &1))
    assert Enum.any?(effects, &match?({:wake_at, _, :close, _, _}, &1))
  end

  test "error Close discards subsequent Session inputs and closes once within its deadline" do
    {state, _} = connected(close_timeout_ms: 25)
    state = active_session(state)
    error = GravitonMQ.AMQP10.Codec.Error.new(:frame_decode, :malformed, :invalid_data_offset)
    {state, effects} = step(state, {:decode_error, error})

    assert [{:wake_at, _, :close, token, 25}] =
             Enum.filter(effects, &match?({:wake_at, _, :close, _, _}, &1))

    assert Enum.count(effects, &match?({:send_frame, _, _, _, %Close{error: %{}}, _}, &1)) == 1

    unsupported = %{frame(%End{}, 19) | body: <<0, 0x53, 0x19, 0x45>>, declared_size: 12}

    inputs = [
      frame(
        %Begin{
          next_outgoing_id: Value.uint(0),
          incoming_window: Value.uint(1),
          outgoing_window: Value.uint(1)
        },
        19
      ),
      frame(
        %Flow{
          incoming_window: Value.uint(1),
          next_outgoing_id: Value.uint(0),
          outgoing_window: Value.uint(1)
        },
        19
      ),
      frame(
        %Transfer{
          handle: Value.uint(0),
          delivery_id: Value.uint(0),
          delivery_tag: Value.binary("x"),
          message_format: Value.uint(0)
        },
        19,
        <<1>>
      ),
      unsupported
    ]

    for input <- inputs do
      assert {^state, []} = step(state, {:frame, input})
    end

    assert {^state, []} = step(state, {:timeout, :close, token}, 0)
    {closed, effects} = step(state, {:frame, frame(%Close{})})
    assert closed.phase == :closed
    assert Enum.count(effects, &match?({:close_transport, _, _}, &1)) == 1
    assert {^closed, []} = step(closed, {:frame, frame(%Close{})})
    {expired, effects} = step(state, {:timeout, :close, token}, 25)
    assert expired.phase == :closed
    assert {:close_transport, 0, :close_timeout} in effects
  end

  test "peer Close carrying Error fails the close operation explicitly" do
    {state, _} = connected()
    {state, _} = step(state, {:command, :close, :close})
    error = %GravitonMQ.AMQP10.Error{condition: value(:symbol, "amqp:internal-error")}
    {_, effects} = step(state, {:frame, frame(%Close{error: error})})
    assert {:operation, :close, {:error, {:peer_close, error}}} in effects
  end

  test "Close retains reserved capacity and no queued heartbeat is written after Close" do
    {:ok, state} = new(max_write_frames: 3, control_reserve_frames: 2)
    {state, effects} = step(state, :start)
    state = acknowledge(state, effects)
    {state, []} = step(state, {:header, header()})
    {state, _} = step(state, {:frame, frame(peer_open(idle_time_out: value(:uint, 2)))})

    state =
      Enum.reduce(1..3, state, fn now, state ->
        {token, _} = Map.fetch!(state.deadlines, :heartbeat)
        elem(step(state, {:timeout, :heartbeat, token}, now), 0)
      end)

    {state, effects} = step(state, {:command, :close, :close}, 3)
    assert Enum.any?(effects, &match?({:send_frame, _, _, 0, %Close{}, <<>>}, &1))
    assert map_size(state.writes) <= 3
    {state, effects} = step(state, {:write_result, 2, :ok}, 3)
    assert state.phase == :closing
    refute Enum.any?(effects, &match?({:send_frame, _, _, _, nil, _}, &1))
  end

  test "connection index replacement releases only the replaced session's indexes" do
    {:ok, first} = SessionState.new(id: 1, local_channel: 3, remote_channel: 19)
    {:ok, second} = SessionState.new(id: 2, local_channel: 4, remote_channel: 20)
    {:ok, connection} = ConnectionState.new(sessions: [first, second])

    assert {:error, {:duplicate_remote_channel, 20}} =
             ConnectionState.put_session(connection, %{first | remote_channel: 20})

    assert {:ok, updated} = ConnectionState.put_session(connection, %{first | remote_channel: 21})
    assert :error = ConnectionState.fetch_incoming_session(updated, 19)
    assert {:ok, ^second} = ConnectionState.fetch_incoming_session(updated, 20)
    assert {:ok, updated} = ConnectionState.remove_session(updated, 1)
    assert :error = ConnectionState.fetch_outgoing_session(updated, 3)
    assert {:ok, ^second} = ConnectionState.fetch_outgoing_session(updated, 4)
  end

  test "outgoing publish uses local handle, peer frame bound and completes only at settled outcome" do
    {:ok, state} = new(max_frame_size: 512)
    {state, effects} = step(state, :start)
    state = acknowledge(state, effects)
    {state, []} = step(state, {:header, header()})
    {state, _} = step(state, {:frame, frame(peer_open(max_frame_size: value(:uint, 4096)))})
    state = active_session(state)
    identity = {value(:string, "send"), :sender}

    {state, effects} =
      step(
        state,
        {:command, :attach, {:session, 10, {:attach, identity, [address: value(:string, "q")]}}}
      )

    state = acknowledge(state, effects)

    peer = %Attach{
      name: elem(identity, 0),
      handle: value(:uint, 17),
      role: value(:boolean, true),
      snd_settle_mode: value(:ubyte, 0),
      rcv_settle_mode: value(:ubyte, 0),
      source: %Source{},
      target: %Target{address: value(:string, "q")}
    }

    {state, effects} = step(state, {:frame, frame(peer, 19)})
    assert {:operation, :attach, {:ok, identity}} in effects

    flow = %Flow{
      next_incoming_id: value(:uint, 0),
      incoming_window: value(:uint, 16),
      next_outgoing_id: value(:uint, 0),
      outgoing_window: value(:uint, 16),
      handle: value(:uint, 17),
      delivery_count: value(:uint, 0),
      link_credit: value(:uint, 1)
    }

    {state, _} = step(state, {:frame, frame(flow, 19)})
    raw = <<0, 0x53, 0x75, 0xB0, 1200::32, String.duplicate("x", 1200)::binary>>

    {state, effects} =
      step(state, {:command, :publish, {:session, 10, {:send, identity, raw, []}}})

    assert {:send_frame, 0, id, _, %Transfer{handle: %Value{value: 0}} = transfer, ^raw} =
             Enum.find(effects, &match?({:send_frame, _, _, _, %Transfer{}, _}, &1))

    {:ok, encoded} = Performative.encode(transfer)
    assert byte_size(encoded) + byte_size(raw) + 8 > 512

    assert byte_size(encoded) + byte_size(raw) + 8 <=
             Endpoint.outgoing_limits(state).max_frame_size

    {state, effects} = step(state, {:write_result, id, :ok})
    refute Enum.any?(effects, &match?({:operation, :publish, _}, &1))

    disposition = %Disposition{
      role: value(:boolean, true),
      first: value(:uint, 0),
      settled: value(:boolean, true),
      state: %Outcome.Accepted{}
    }

    {state, effects} = step(state, {:frame, frame(disposition, 19)})
    assert {:operation, :publish, {:ok, %Outcome.Accepted{}}} in effects
    refute Map.has_key?(state.operations, :publish)

    refute Enum.any?(
             Map.keys(state.deadlines),
             &match?({:session, 10, {:operation, :publish}}, &1)
           )
  end

  test "every peer handle above the advertised maximum closes the Connection with framing-error" do
    {state, _} = connected(session_options: [max_handle: 1])
    state = active_session(state)

    attach = %Attach{
      name: value(:string, "over-limit"),
      handle: value(:uint, 2),
      role: value(:boolean, false),
      snd_settle_mode: value(:ubyte, 0),
      rcv_settle_mode: value(:ubyte, 0),
      source: %Source{},
      target: %Target{},
      initial_delivery_count: value(:uint, 0)
    }

    for pdu <- [
          attach,
          %Flow{
            next_incoming_id: value(:uint, 0),
            incoming_window: value(:uint, 16),
            next_outgoing_id: value(:uint, 0),
            outgoing_window: value(:uint, 16),
            handle: value(:uint, 2)
          },
          %Transfer{
            handle: value(:uint, 2),
            delivery_id: value(:uint, 0),
            delivery_tag: value(:binary, "late"),
            message_format: value(:uint, 0)
          },
          %Detach{handle: value(:uint, 2), closed: value(:boolean, true)}
        ] do
      {state, effects} = step(state, {:frame, frame(pdu, 19)})
      assert state.phase == :closing

      assert Enum.any?(
               effects,
               &match?(
                 {:send_frame, _, _, 0,
                  %Close{error: %{condition: %Value{value: "amqp:connection:framing-error"}}}, _},
                 &1
               )
             )

      refute Enum.any?(effects, &match?({:send_frame, _, _, _, %End{}, _}, &1))
    end
  end

  test "ending Sessions discard Begin and out-of-range handles while other Session and Close stay live" do
    for ending <- [:host, :error] do
      {state, _} = connected()
      state = active_session(state)
      {state, effects} = step(state, {:command, :open2, {:open_session, 20, []}})
      state = acknowledge(state, effects)

      begin = %Begin{
        remote_channel: value(:ushort, 1),
        next_outgoing_id: value(:uint, 0),
        incoming_window: value(:uint, 16),
        outgoing_window: value(:uint, 16)
      }

      {state, _} = step(state, {:frame, frame(begin, 20)})

      {state, _} =
        case ending do
          :host ->
            step(state, {:command, :end1, {:end_session, 10}})

          :error ->
            step(
              state,
              {:frame,
               frame(%Detach{handle: value(:uint, 23), closed: value(:boolean, true)}, 19)}
            )
        end

      assert state.session_controls[10].phase == :ending
      {state, effects} = step(state, {:frame, frame(%{begin | remote_channel: nil}, 19)})
      assert state.phase == :active
      refute Enum.any?(effects, &match?({:send_frame, _, _, _, _, _}, &1))

      {state, effects} =
        step(state, {:frame, frame(%Transfer{handle: value(:uint, 0xFFFF_FFFF)}, 19)})

      assert state.phase == :active
      refute Enum.any?(effects, &match?({:send_frame, _, _, _, _, _}, &1))

      for body <- [
            <<0xFF>>,
            <<0, 0x53, 0x12, 0x40>>,
            <<0, 0x53, 0x17, 0x40>>,
            <<0, 0x53, 0x14, 0xC0, 3, 1, 0xFF>>
          ] do
        ignored = %{frame(%End{}, 19) | declared_size: 8 + byte_size(body), body: body}
        {still_active, effects} = step(state, {:frame, ignored})
        assert still_active.phase == :active
        assert still_active.session_controls[10].phase == :ending
        refute Enum.any?(effects, &match?({:send_frame, _, _, _, _, _}, &1))
      end

      for descriptor <- [
            value(:ulong, 0x10),
            value(:ulong, 0x18),
            value(:symbol, "amqp:open:list"),
            value(:symbol, "amqp:close:list")
          ] do
        {:ok, encoded} = GravitonMQ.AMQP10.Codec.Value.encode(descriptor)
        body = <<0, encoded::binary, 0x40>>
        invalid_control = %{frame(%Close{}, 19) | declared_size: 8 + byte_size(body), body: body}
        {closing, effects} = step(state, {:frame, invalid_control})
        assert closing.phase == :closing
        assert Enum.any?(effects, &match?({:send_frame, _, _, 0, %Close{}, _}, &1))
      end

      invalid_header = %{frame(%End{}, 19) | declared_size: 7}
      {closing, _} = step(state, {:frame, invalid_header})
      assert closing.phase == :closing

      echo = %Flow{
        next_incoming_id: value(:uint, 0),
        incoming_window: value(:uint, 16),
        next_outgoing_id: value(:uint, 0),
        outgoing_window: value(:uint, 16),
        echo: value(:boolean, true)
      }

      {state, effects} = step(state, {:frame, frame(echo, 20)})
      assert Enum.any?(effects, &match?({:send_frame, _, _, 1, %Flow{}, _}, &1))
      {closed, effects} = step(state, {:frame, frame(%Close{}, 19)})
      assert closed.phase == :closed
      assert Enum.any?(effects, &match?({:send_frame, _, _, 0, %Close{}, _}, &1))
      {state, _} = step(state, {:frame, frame(%End{}, 19)})
      assert state.phase == :active
      refute Map.has_key?(state.session_controls, 10)
      assert state.session_controls[20].phase == :active
    end
  end

  test "outgoing Link names reserve their container-pair direction across Sessions until removal" do
    for ending <- [:host, :session_error] do
      {state, _} = connected()
      state = active_session(state)
      {state, effects} = step(state, {:command, :open2, {:open_session, 20, []}})
      state = acknowledge(state, effects)

      begin = %Begin{
        remote_channel: value(:ushort, 1),
        next_outgoing_id: value(:uint, 0),
        incoming_window: value(:uint, 16),
        outgoing_window: value(:uint, 16)
      }

      {state, _} = step(state, {:frame, frame(begin, 20)})
      identity = {value(:string, "unique"), :sender}

      {state, effects} =
        step(
          state,
          {:command, :first, {:session, 10, {:attach, identity, [address: value(:string, "q")]}}}
        )

      assert Enum.any?(effects, &match?({:send_frame, _, _, _, %Attach{}, _}, &1))

      {state, effects} =
        step(
          state,
          {:command, :duplicate,
           {:session, 20, {:attach, identity, [address: value(:string, "q")]}}}
        )

      assert {:operation, :duplicate, {:error, :attach_not_admitted}} in effects
      refute Enum.any?(effects, &match?({:send_frame, _, _, _, %Attach{}, _}, &1))
      assert state.connection.sessions_by_identity[20].links_by_identity == %{}
      opposite = {elem(identity, 0), :receiver}

      {state, effects} =
        step(
          state,
          {:command, :opposite,
           {:session, 20, {:attach, opposite, [address: value(:string, "q")]}}}
        )

      assert Enum.any?(effects, &match?({:send_frame, _, _, _, %Attach{}, _}, &1))

      {state, _} =
        case ending do
          :host ->
            step(state, {:command, :ending, {:end_session, 10}})

          :session_error ->
            step(
              state,
              {:frame,
               frame(%Detach{handle: value(:uint, 99), closed: value(:boolean, true)}, 19)}
            )
        end

      assert state.session_controls[10].phase == :ending

      {state, effects} =
        step(
          state,
          {:command, :still_reserved,
           {:session, 20, {:attach, identity, [address: value(:string, "q")]}}}
        )

      assert {:operation, :still_reserved, {:error, :attach_not_admitted}} in effects
      {state, _} = step(state, {:frame, frame(%End{}, 19)})

      {state, effects} =
        step(
          state,
          {:command, :reuse, {:session, 20, {:attach, identity, [address: value(:string, "q")]}}}
        )

      assert Enum.any?(effects, &match?({:send_frame, _, _, _, %Attach{}, _}, &1))
      assert state.phase == :active
    end
  end

  test "incoming delivery stays opaque and receiver settlement completes at write receipt" do
    {state, _} = connected()
    state = active_session(state)
    identity = {value(:string, "receive"), :receiver}

    {state, effects} =
      step(
        state,
        {:command, :attach, {:session, 10, {:attach, identity, [address: value(:string, "q")]}}}
      )

    state = acknowledge(state, effects)

    peer = %Attach{
      name: elem(identity, 0),
      handle: value(:uint, 23),
      role: value(:boolean, false),
      snd_settle_mode: value(:ubyte, 0),
      rcv_settle_mode: value(:ubyte, 0),
      source: %Source{address: value(:string, "q")},
      target: %Target{},
      initial_delivery_count: value(:uint, 0)
    }

    {state, _} = step(state, {:frame, frame(peer, 19)})

    {state, effects} =
      step(state, {:command, :demand, {:session, 10, {:demand, identity, 1, 1_048_576}}})

    assert {:operation, :demand, {:ok, 1}} in effects
    state = acknowledge(state, effects)
    raw = <<0, 0x53, 0x75, 0xA0, 3, 1, 2, 3>>

    transfer = %Transfer{
      handle: value(:uint, 23),
      delivery_id: value(:uint, 0),
      delivery_tag: value(:binary, "tag"),
      message_format: value(:uint, 0),
      settled: value(:boolean, false)
    }

    {state, effects} = step(state, {:frame, frame(transfer, 19, raw)})

    assert {:delivery, 10, ^identity, token, ^raw, %{delivery_id: 0}} =
             Enum.find(effects, &match?({:delivery, _, _, _, _, _}, &1))

    {state, effects} =
      step(state, {:command, :settle, {:session, 10, {:settle, token, %Outcome.Accepted{}}}})

    refute {:operation, :settle, {:ok, %Outcome.Accepted{}}} in effects

    assert {:send_frame, 0, id, _,
            %Disposition{role: %Value{value: true}, settled: %Value{value: true}},
            <<>>} =
             Enum.find(effects, &match?({:send_frame, _, _, _, %Disposition{}, _}, &1))

    {state, effects} = step(state, {:write_result, id, :ok})
    assert {:operation, :settle, {:ok, %Outcome.Accepted{}}} in effects
    assert state.connection.sessions_by_identity[10].incoming_by_id == %{}
  end

  test "Session operation deadlines follow shorter and longer per-operation timeouts" do
    for {op, delay} <- [{:short, 20}, {:long, 2000}] do
      {state, _} = connected(operation_timeout_ms: 1000)
      state = active_session(state)
      identity = {value(:string, Atom.to_string(op)), :sender}

      {state, effects} =
        step(
          state,
          {:command, op,
           {:session, 10, {:attach, identity, [address: value(:string, "q"), timeout: delay]}}}
        )

      refute Map.has_key?(state.deadlines, {:operation, op})

      assert {:wake_at, 0, {:session, 10, {:operation, ^op}}, token, ^delay} =
               Enum.find(
                 effects,
                 &match?({:wake_at, _, {:session, _, {:operation, ^op}}, _, _}, &1)
               )

      if delay > 1000 do
        {unchanged, effects} = step(state, {:timeout, {:operation, op}, :absent}, 1000)
        assert effects == []
        assert Map.has_key?(unchanged.operations, op)
      end

      {_, effects} = step(state, {:timeout, {:session, 10, {:operation, op}}, token}, delay)
      assert {:operation, op, {:unknown, :timeout}} in effects
    end
  end

  test "Session default deadline inherits Endpoint settings with explicit Session override" do
    for {session_options, expected} <- [{[], 1000}, {[operation_timeout: 1500], 1500}] do
      {state, _} = connected(operation_timeout_ms: 1000, session_options: session_options)
      state = active_session(state)
      identity = {value(:string, "sender"), :sender}

      {_, effects} =
        step(
          state,
          {:command, :attach, {:session, 10, {:attach, identity, [address: value(:string, "q")]}}}
        )

      assert {:wake_at, 0, {:session, 10, {:operation, :attach}}, _, ^expected} =
               Enum.find(
                 effects,
                 &match?({:wake_at, _, {:session, _, {:operation, :attach}}, _, _}, &1)
               )
    end
  end
end
