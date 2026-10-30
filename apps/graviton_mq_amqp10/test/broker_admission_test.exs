defmodule GravitonMQ.AMQP10.BrokerAdmissionTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.{Endpoint, Frame, Outcome, SessionEngine, Value}
  alias GravitonMQ.AMQP10.Codec.{Performative, ProtocolHeader}
  alias GravitonMQ.AMQP10.Error, as: ProtocolError

  alias GravitonMQ.AMQP10.Performative.{
    Attach,
    Begin,
    Detach,
    Disposition,
    End,
    Flow,
    Open,
    Transfer
  }

  alias GravitonMQ.AMQP10.Terminus.{Source, Target}

  test "deferred Attach requests admission without exposing an attached Link or granting resources" do
    {state, id, token, pdu, effects} = pending(:receiver)
    assert emits(effects) == []
    assert {:link_event, {:attach_requested, id, token, pdu}} in effects
    assert {:deadline, {:admission, id}, token, 10} in effects
    assert state.links_by_identity[id].phase == :admission_pending
    assert state.incoming_reserved_slots == 0
    assert state.operations == %{}
    {state, effects} = SessionEngine.step(state, {:command, :demand, {:demand, id, 1, 32}}, ctx())
    assert {:complete, :demand, {:error, :demand_not_admitted}} in effects
    assert state.incoming_reserved_bytes == 0
    {_, effects} = SessionEngine.step(state, {:command, :send, {:send, id, <<1>>, []}}, ctx())
    assert {:complete, :send, {:error, :send_not_admitted}} in effects
  end

  test "accepted admission waits for bounded Attach emission and keeps the existing event shape" do
    {state, id, token, _, _} = pending(:sender)

    {state, effects} =
      SessionEngine.step(
        state,
        {:command, :accept, {:accept_link, id, token}},
        ctx(control_frames: 0)
      )

    assert emits(effects) == []
    refute {:link_event, {:attached, id}} in effects
    assert {:cancel_deadline, {:admission, id}} in effects
    assert state.links_by_identity[id].phase == :attach_pending
    {state, effects} = SessionEngine.step(state, :flush, ctx())
    assert [{_, %Attach{source: %Source{}, target: %Target{}}, <<>>}] = emits(effects)
    assert {:link_event, {:attached, id}} in effects
    assert {:complete, :accept, {:ok, id}} in effects
    assert state.links_by_identity[id].phase == :attached
    {_, effects} = SessionEngine.step(state, {:command, :late, {:accept_link, id, token}}, ctx())
    assert {:complete, :late, {:error, :stale_link_request}} in effects
    assert emits(effects) == []
  end

  test "explicit rejection emits null Attach before the provided error Detach" do
    {state, id, token, _, _} = pending(:receiver)
    error = error("amqp:not-found")

    {state, effects} =
      SessionEngine.step(
        state,
        {:command, :reject, {:reject_link, id, token, error}},
        ctx(control_frames: 1)
      )

    assert [{_, %Attach{source: nil, target: nil}, <<>>}] = emits(effects)
    assert {:complete, :reject, {:ok, id}} in effects
    refute {:link_event, {:attached, id}} in effects
    {state, effects} = SessionEngine.step(state, :flush, ctx())
    assert [{_, %Detach{error: ^error}, <<>>}] = emits(effects)
    assert state.links_by_identity[id].phase == :refused
    assert state.incoming_reserved_bytes == 0
    assert Map.has_key?(state.deadlines, {:link, id})
    refute Map.has_key?(state.deadlines, {:admission, id})
  end

  test "admission expiry fences stale timers, refuses the Link and bounds its Detach handshake" do
    {state, id, token, _, _} = pending(:receiver)

    {state, effects} =
      SessionEngine.step(state, {:timeout, {:admission, id}, :stale}, ctx(now_ms: 10))

    assert emits(effects) == []

    {state, effects} =
      SessionEngine.step(state, {:timeout, {:admission, id}, token}, ctx(now_ms: 10))

    assert [
             {_, %Attach{source: nil, target: nil}, <<>>},
             {_, %Detach{error: %ProtocolError{}}, <<>>}
           ] = emits(effects)

    assert state.phase == :active
    assert state.incoming_reserved_slots == 0
    assert {:link_event, {:refused, id, :admission_timeout}} in effects

    {:deadline, {:link, ^id}, close_token, 20} =
      Enum.find(effects, &match?({:deadline, {:link, _}, _, _}, &1))

    {state, effects} =
      SessionEngine.step(state, {:timeout, {:link, id}, close_token}, ctx(now_ms: 20))

    assert state.phase == :closed
    assert {:session_error, :link_close_timeout} in effects
  end

  test "expired and old-generation decisions cannot admit a Link before its timer is processed" do
    {state, id, token, _, _} = pending(:receiver)

    {state, effects} =
      SessionEngine.step(state, {:command, :old, {:accept_link, id, token}}, ctx(generation: 1))

    assert {:complete, :old, {:error, :stale_link_request}} in effects
    assert emits(effects) == []

    {state, effects} =
      SessionEngine.step(state, {:command, :expired, {:accept_link, id, token}}, ctx(now_ms: 10))

    assert {:complete, :expired, {:error, :link_request_expired}} in effects
    refute {:link_event, {:attached, id}} in effects
    assert state.links_by_identity[id].phase == :refused
  end

  test "peer cancellation replies in order and a reused identity rejects the old decision token" do
    {state, id, token, pdu, _} = pending(:receiver)

    {state, effects} =
      SessionEngine.step(
        state,
        {:peer, %Detach{handle: Value.uint(9), closed: Value.boolean(true)}, <<>>},
        ctx()
      )

    assert [{_, %Attach{source: nil, target: nil}, <<>>}, {_, %Detach{}, <<>>}] = emits(effects)
    assert {:cancel_deadline, {:admission, id}} in effects
    assert state.links_by_identity == %{}
    {state, effects} = SessionEngine.step(state, {:peer, pdu, <<>>}, ctx())

    {:link_event, {:attach_requested, ^id, new_token, ^pdu}} =
      Enum.find(effects, &match?({:link_event, {:attach_requested, _, _, _}}, &1))

    refute new_token == token

    {_, effects} =
      SessionEngine.step(
        state,
        {:command, :stale, {:reject_link, id, token, error("amqp:not-found")}},
        ctx()
      )

    assert {:complete, :stale, {:error, :stale_link_request}} in effects
    assert emits(effects) == []
  end

  test "admitted reply still blocked at operation timeout is refused rather than silently dropped" do
    {state, id, token, _, _} = pending(:receiver)

    {state, effects} =
      SessionEngine.step(
        state,
        {:command, :accept, {:accept_link, id, token}},
        ctx(control_frames: 0)
      )

    {:deadline, {:operation, :accept}, operation_token, 10} =
      Enum.find(effects, &match?({:deadline, {:operation, _}, _, _}, &1))

    {state, effects} =
      SessionEngine.step(
        state,
        {:timeout, {:operation, :accept}, operation_token},
        ctx(now_ms: 10)
      )

    assert {:complete, :accept, {:error, :timeout}} in effects
    assert [{_, %Attach{source: nil, target: nil}, <<>>}, {_, %Detach{}, <<>>}] = emits(effects)
    assert state.links_by_identity[id].phase == :refused
  end

  test "unadmitted peer Transfer creates no delivery or queue responsibility" do
    {state, _, _, _, _} = pending(:receiver)

    transfer = %Transfer{
      handle: Value.uint(9),
      delivery_id: Value.uint(0),
      delivery_tag: Value.binary("tag"),
      message_format: Value.uint(0)
    }

    {state, effects} = SessionEngine.step(state, {:peer, transfer, <<1>>}, ctx())
    assert state.phase == :closed
    assert state.incoming_by_id == %{}
    refute Enum.any?(effects, &match?({:delivery, _, _, _, _}, &1))
  end

  test "broker profile refuses copy distribution before requesting admission and pending Links consume max_links" do
    state = active(peer_attach_policy: :defer)
    pdu = peer_attach(identity(:sender), :sender)
    pdu = %{pdu | source: %{pdu.source | distribution_mode: Value.symbol("copy")}}
    {state, effects} = SessionEngine.step(state, {:peer, pdu, <<>>}, ctx())
    assert length(emits(effects)) == 2
    refute Enum.any?(effects, &match?({:link_event, {:attach_requested, _, _, _}}, &1))
    assert state.links_by_identity[identity(:sender)].phase == :refused
    {state, _, _, _, _} = pending(:receiver, max_links: 1)

    other = %{
      peer_attach(identity(:sender), :sender)
      | name: Value.string("second"),
        handle: Value.uint(10)
    }

    {state, effects} = SessionEngine.step(state, {:peer, other, <<>>}, ctx())
    assert state.phase == :closed
    assert {:session_error, :resource_limit} in effects
  end

  test "runtime can close an admitted Link with a validated protocol error" do
    {state, id, token, _, _} = pending(:sender)
    {state, _} = SessionEngine.step(state, {:command, :accept, {:accept_link, id, token}}, ctx())
    error = error("amqp:resource-deleted")
    {state, effects} = SessionEngine.step(state, {:command, :detach, {:detach, id, error}}, ctx())
    assert [{_, %Detach{error: ^error}, <<>>}] = emits(effects)

    {state, effects} =
      SessionEngine.step(
        state,
        {:peer, %Detach{handle: Value.uint(9), closed: Value.boolean(true)}, <<>>},
        ctx()
      )

    assert {:complete, :detach, {:ok, id}} in effects
    assert state.links_by_identity == %{}
  end

  test "send capacity applies credit, windows, peer size, one outstanding send, and writer bytes" do
    {state, id, token, _, _} = pending(:sender, max_message_bytes: 64)
    assert capacity(state, id) == %{messages: 0, bytes: 0}
    {state, _} = SessionEngine.step(state, {:command, :accept, {:accept_link, id, token}}, ctx())
    assert capacity(state, id) == %{messages: 0, bytes: 0}
    {state, _} = SessionEngine.step(state, {:peer, flow(), <<>>}, ctx())
    assert capacity(state, id) == %{messages: 1, bytes: 32}

    assert SessionEngine.send_capacity(state, id, ctx(write_frames: 0)) == %{
             messages: 0,
             bytes: 0
           }

    assert SessionEngine.send_capacity(state, id, ctx(write_bytes: 8)) == %{messages: 0, bytes: 0}

    {state, _} =
      SessionEngine.step(
        state,
        {:command, :send, {:send, id, :binary.copy(<<1>>, 32), []}},
        ctx()
      )

    assert capacity(state, id) == %{messages: 0, bytes: 0}

    pdu = %Disposition{
      role: Value.boolean(true),
      first: Value.uint(0),
      state: %Outcome.Accepted{},
      settled: Value.boolean(true)
    }

    {state, _} = SessionEngine.step(state, {:peer, pdu, <<>>}, ctx())
    assert capacity(state, id) == %{messages: 1, bytes: 32}
    assert capacity(%{state | remote_incoming_window: 0}, id) == %{messages: 0, bytes: 0}
    assert capacity(%{state | outgoing_window: 0}, id) == %{messages: 0, bytes: 0}

    assert capacity(%{state | outgoing_bytes: state.options.max_outgoing_bytes}, id) == %{
             messages: 0,
             bytes: 0
           }
  end

  test "Endpoint broker role defers peer admission through generic commands and exposes bounded capacity" do
    endpoint = connected(role: :broker)
    {endpoint, _} = endpoint_step(endpoint, {:frame, frame(peer_begin(), 3)})
    id = identity(:sender)
    {endpoint, effects} = endpoint_step(endpoint, {:frame, frame(peer_attach(id, :sender), 3)})

    {:event, {:session, 0, {:attach_requested, ^id, token, _}}} =
      Enum.find(effects, &match?({:event, {:session, _, {:attach_requested, _, _, _}}}, &1))

    refute Enum.any?(effects, &match?({:send_frame, _, _, _, %Attach{}, _}, &1))
    assert Endpoint.send_capacity(endpoint, 0, id) == %{messages: 0, bytes: 0}

    {endpoint, effects} =
      endpoint_step(endpoint, {:command, :accept, {:session, 0, {:accept_link, id, token}}})

    assert {:event, {:session, 0, {:attached, id}}} in effects
    assert {:operation, :accept, {:ok, id}} in effects
    {endpoint, _} = endpoint_step(endpoint, {:frame, frame(flow(), 3)})
    assert Endpoint.send_capacity(endpoint, 0, id) == %{messages: 1, bytes: 32}

    full = %{
      endpoint
      | write_bytes: endpoint.options.max_write_bytes - endpoint.options.control_reserve_bytes
    }

    assert Endpoint.send_capacity(full, 0, id) == %{messages: 0, bytes: 0}
    assert Endpoint.send_capacity(endpoint, 99, id) == %{messages: 0, bytes: 0}
    assert Endpoint.send_capacity(%{endpoint | phase: :closed}, 0, id) == %{messages: 0, bytes: 0}
  end

  test "Endpoint role owns the admission policy and client peer links retain automatic acceptance" do
    assert {:error, :peer_attach_policy_is_role_owned} =
             Endpoint.new(
               container_id: "local",
               role: :broker,
               session_options: [peer_attach_policy: :auto]
             )

    endpoint = connected(role: :broker)

    {_, effects} =
      endpoint_step(endpoint, {:command, :open, {:open_session, 1, [peer_attach_policy: :auto]}})

    assert {:operation, :open, {:error, :peer_attach_policy_is_role_owned}} in effects
    endpoint = connected(role: :client)
    {endpoint, _} = endpoint_step(endpoint, {:frame, frame(peer_begin(), 3)})
    id = identity(:sender)
    {_, effects} = endpoint_step(endpoint, {:frame, frame(peer_attach(id, :sender), 3)})
    assert {:event, {:session, 0, {:attached, id}}} in effects
    refute Enum.any?(effects, &match?({:event, {:session, _, {:attach_requested, _, _, _}}}, &1))
  end

  test "pipelined peer credit remains inert until broker admission emits its Attach" do
    {state, id, token, _, _} = pending(:sender)
    {state, effects} = SessionEngine.step(state, {:peer, flow(), <<>>}, ctx())
    assert state.phase == :active
    assert capacity(state, id) == %{messages: 0, bytes: 0}
    assert emits(effects) == []

    {state, effects} =
      SessionEngine.step(state, {:command, :accept, {:accept_link, id, token}}, ctx())

    assert {:link_event, {:attached, id}} in effects
    assert capacity(state, id) == %{messages: 1, bytes: 32}
  end

  test "pipelined drain and echo wait for admitted Attach and owner exhaustion" do
    {state, id, token, _, _} = pending(:sender)
    pdu = %{flow() | drain: Value.boolean(true), echo: Value.boolean(true)}
    {state, effects} = SessionEngine.step(state, {:peer, pdu, <<>>}, ctx())
    assert state.phase == :active
    assert emits(effects) == []
    assert state.links_by_identity[id].delivery_count == 0

    {state, effects} =
      SessionEngine.step(state, {:command, :accept, {:accept_link, id, token}}, ctx())

    assert [
             {_, %Attach{}, <<>>},
             {_,
              %Flow{
                delivery_count: %Value{value: 0},
                link_credit: %Value{value: 2},
                drain: %Value{value: true},
                echo: %Value{value: false}
              }, <<>>}
           ] = emits(effects)

    assert state.links_by_identity[id].credit == 2
    assert %{messages: 1, drain_token: drain_token} = capacity(state, id)

    {state, effects} =
      SessionEngine.step(
        state,
        {:command, :exhausted, {:drain_exhausted, id, drain_token}},
        ctx()
      )

    assert [{_, %Flow{delivery_count: %Value{value: 2}, link_credit: %Value{value: 0}}, <<>>}] =
             emits(effects)

    assert state.links_by_identity[id].credit == 0
    assert capacity(state, id) == %{messages: 0, bytes: 0}
  end

  test "admission timer rejects a stale generation and transport closure cancels the live request" do
    {state, id, token, _, _} = pending(:sender)

    {state, effects} =
      SessionEngine.step(
        state,
        {:timeout, {:admission, id}, token},
        ctx(now_ms: 10, generation: 1)
      )

    assert emits(effects) == []
    assert state.links_by_identity[id].phase == :admission_pending
    {state, effects} = SessionEngine.step(state, {:transport_down, :closed}, ctx(now_ms: 10))
    assert {:cancel_deadline, {:admission, id}} in effects
    assert state.deadlines == %{}
    assert state.links_by_identity == %{}
  end

  test "oversized rejection data cannot consume control or operation capacity" do
    {state, id, token, _, _} = pending(:receiver)
    error = %{error("amqp:not-found") | description: Value.string(:binary.copy(<<1>>, 100_000))}

    {state, effects} =
      SessionEngine.step(state, {:command, :reject, {:reject_link, id, token, error}}, ctx())

    assert {:complete, :reject, {:error, :invalid_link_error}} in effects
    assert state.links_by_identity[id].phase == :admission_pending
    assert state.operations == %{}
    assert :queue.is_empty(state.control_queue)
    assert emits(effects) == []
  end

  test "capacity accounts for free bytes, fragment limits and full operation or receipt ledgers" do
    {state, id, token, _, _} = pending(:sender, max_message_bytes: 64, max_fragments: 2)
    {state, _} = SessionEngine.step(state, {:command, :accept, {:accept_link, id, token}}, ctx())
    {state, _} = SessionEngine.step(state, {:peer, flow(), <<>>}, ctx())

    assert capacity(%{state | outgoing_bytes: state.options.max_outgoing_bytes - 7}, id) == %{
             messages: 1,
             bytes: 7
           }

    operations = Map.new(1..state.options.max_pending, &{&1, %{}})
    writes = Map.new(1..state.options.max_writes, &{&1, %{}})
    assert capacity(%{state | operations: operations}, id) == %{messages: 0, bytes: 0}
    assert capacity(%{state | writes: writes}, id) == %{messages: 0, bytes: 0}
    query = SessionEngine.send_capacity(state, id, ctx(write_bytes: 36))
    assert query.messages == 1
    assert query.bytes < 32
    assert query.bytes > 0
  end

  test "a late admission decision cannot target a reused Session identity on the same connection" do
    endpoint = connected(role: :broker)
    {endpoint, effects} = endpoint_step(endpoint, {:command, :open1, {:open_session, 7, []}})

    {:send_frame, _, old_write_id, _, %Begin{}, _} =
      Enum.find(effects, &match?({:send_frame, _, _, _, %Begin{}, _}, &1))

    begin_reply = %{peer_begin() | remote_channel: Value.ushort(0)}
    {endpoint, _} = endpoint_step(endpoint, {:frame, frame(begin_reply, 3)})
    id = identity(:sender)
    {endpoint, effects} = endpoint_step(endpoint, {:frame, frame(peer_attach(id, :sender), 3)})

    {:event, {:session, 7, {:attach_requested, ^id, old_token, _}}} =
      Enum.find(effects, &match?({:event, {:session, _, {:attach_requested, _, _, _}}}, &1))

    {endpoint, _} = endpoint_step(endpoint, {:frame, frame(%End{}, 3)})
    {endpoint, _} = endpoint_step(endpoint, {:command, :open2, {:open_session, 7, []}})
    {endpoint, _} = endpoint_step(endpoint, {:frame, frame(begin_reply, 4)})
    {endpoint, effects} = endpoint_step(endpoint, {:frame, frame(peer_attach(id, :sender), 4)})

    {:event, {:session, 7, {:attach_requested, ^id, new_token, _}}} =
      Enum.find(effects, &match?({:event, {:session, _, {:attach_requested, _, _, _}}}, &1))

    refute old_token == new_token
    new_writes = endpoint.connection.sessions_by_identity[7].writes
    {endpoint, _} = endpoint_step(endpoint, {:write_result, old_write_id, :ok})
    assert endpoint.connection.sessions_by_identity[7].writes == new_writes

    {_, effects} =
      endpoint_step(endpoint, {:command, :old, {:session, 7, {:accept_link, id, old_token}}})

    assert {:operation, :old, {:error, :stale_link_request}} in effects
    refute Enum.any?(effects, &match?({:send_frame, _, _, _, %Attach{}, _}, &1))
  end

  test "incoming active names across Sessions refuse before admission and become reusable after End" do
    endpoint = connected(role: :broker)
    {endpoint, _} = endpoint_step(endpoint, {:frame, frame(peer_begin(), 3)})
    {endpoint, _} = endpoint_step(endpoint, {:frame, frame(peer_begin(), 4)})
    id = identity(:receiver)
    {endpoint, effects} = endpoint_step(endpoint, {:frame, frame(peer_attach(id, :receiver), 3)})

    assert Enum.any?(
             effects,
             &match?({:event, {:session, 0, {:attach_requested, ^id, _, _}}}, &1)
           )

    {endpoint, effects} = endpoint_step(endpoint, {:frame, frame(peer_attach(id, :receiver), 4)})

    assert [
             {:send_frame, _, _, _, %Attach{source: nil, target: nil}, <<>>},
             {:send_frame, _, _, _,
              %Detach{error: %ProtocolError{condition: %Value{value: "amqp:resource-locked"}}},
              <<>>}
           ] = Enum.filter(effects, &match?({:send_frame, _, _, _, _, _}, &1))

    refute Enum.any?(effects, &match?({:event, {:session, _, {:attach_requested, _, _, _}}}, &1))

    assert endpoint.connection.sessions_by_identity[0].links_by_identity[id].phase ==
             :admission_pending

    assert endpoint.connection.sessions_by_identity[1].incoming_reserved_slots == 0

    {endpoint, _} =
      endpoint_step(
        endpoint,
        {:frame, frame(%Detach{handle: Value.uint(9), closed: Value.boolean(true)}, 4)}
      )

    {endpoint, _} = endpoint_step(endpoint, {:frame, frame(%End{}, 3)})
    {_endpoint, effects} = endpoint_step(endpoint, {:frame, frame(peer_attach(id, :receiver), 4)})

    assert Enum.any?(
             effects,
             &match?({:event, {:session, 1, {:attach_requested, ^id, _, _}}}, &1)
           )
  end

  defp pending(role, options \\ []) do
    state = active(Keyword.merge([peer_attach_policy: :defer, operation_timeout: 10], options))
    id = identity(role)
    pdu = peer_attach(id, role)
    {state, effects} = SessionEngine.step(state, {:peer, pdu, <<>>}, ctx())

    {:link_event, {:attach_requested, ^id, token, ^pdu}} =
      Enum.find(effects, &match?({:link_event, {:attach_requested, _, _, _}}, &1))

    {state, id, token, pdu, effects}
  end

  defp active(options) do
    {:ok, state} = SessionEngine.new(1, 0, options)
    {state, _} = SessionEngine.step(state, {:local_begin, :begin}, ctx())

    {state, _} =
      SessionEngine.step(%{state | remote_channel: 3}, {:peer_begin, peer_begin()}, ctx())

    state
  end

  defp capacity(state, id), do: SessionEngine.send_capacity(state, id, ctx())
  defp identity(role), do: {Value.string("link"), role}
  defp error(condition), do: %ProtocolError{condition: Value.symbol(condition)}

  defp peer_begin,
    do: %Begin{
      next_outgoing_id: Value.uint(100),
      incoming_window: Value.uint(100),
      outgoing_window: Value.uint(100),
      handle_max: Value.uint(31)
    }

  defp flow,
    do: %Flow{
      next_incoming_id: Value.uint(0),
      incoming_window: Value.uint(100),
      next_outgoing_id: Value.uint(100),
      outgoing_window: Value.uint(100),
      handle: Value.uint(9),
      delivery_count: Value.uint(0),
      link_credit: Value.uint(2)
    }

  defp peer_attach({name, role}, _),
    do: %Attach{
      name: name,
      handle: Value.uint(9),
      role: Value.boolean(role == :sender),
      snd_settle_mode: Value.ubyte(0),
      rcv_settle_mode: Value.ubyte(0),
      source: %Source{address: Value.string("q")},
      target: %Target{address: Value.string("q")},
      max_message_size: Value.ulong(32),
      initial_delivery_count: if(role == :receiver, do: Value.uint(0))
    }

  defp ctx(options \\ []),
    do:
      Map.new(
        Keyword.merge(
          [
            now_ms: 0,
            generation: 0,
            max_frame_size: 512,
            max_handle: 31,
            write_frames: 16,
            write_bytes: 65536,
            control_frames: 16,
            control_bytes: 65536
          ],
          options
        )
      )

  defp emits(effects),
    do: for({:emit, receipt, pdu, payload} <- effects, do: {receipt, pdu, payload})

  defp endpoint_step(endpoint, event),
    do: Endpoint.step(endpoint, %{generation: endpoint.generation, now_ms: 0, event: event})

  defp connected(options) do
    {:ok, endpoint} = Endpoint.new(Keyword.merge([container_id: "local"], options))
    {endpoint, effects} = endpoint_step(endpoint, :start)

    endpoint =
      Enum.reduce(effects, endpoint, fn
        {:send_header, _, id, _}, endpoint ->
          elem(endpoint_step(endpoint, {:write_result, id, :ok}), 0)

        {:send_frame, _, id, _, _, _}, endpoint ->
          elem(endpoint_step(endpoint, {:write_result, id, :ok}), 0)

        _, endpoint ->
          endpoint
      end)

    {endpoint, _} =
      endpoint_step(
        endpoint,
        {:header, %ProtocolHeader{protocol_id: 0, major: 1, minor: 0, revision: 0}}
      )

    {endpoint, _} =
      endpoint_step(endpoint, {:frame, frame(%Open{container_id: Value.string("peer")})})

    endpoint
  end

  defp frame(pdu, channel \\ 0) do
    {:ok, body} = Performative.encode(pdu)

    %Frame{
      declared_size: 8 + byte_size(body),
      data_offset_words: 2,
      type: :amqp,
      channel: channel,
      extended_header: <<>>,
      body: body
    }
  end
end
