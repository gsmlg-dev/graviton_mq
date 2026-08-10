defmodule GravitonMQ.AMQP10.ProtocolStateTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.ConnectionState
  alias GravitonMQ.AMQP10.Link
  alias GravitonMQ.AMQP10.SessionState
  alias GravitonMQ.AMQP10.Value

  test "incoming and outgoing indexes resolve different handles to the same link" do
    link = link("orders", :sender, 3, 19)

    assert {:ok, session} =
             SessionState.new(
               id: 7,
               local_channel: 2,
               remote_channel: 11,
               links: [link]
             )

    identity = Link.identity(link)

    assert 3 != 19
    assert %{^identity => ^link} = session.links_by_identity
    assert %{3 => ^identity} = session.local_handle_to_link
    assert %{19 => ^identity} = session.remote_handle_to_link
    assert {:ok, ^link} = SessionState.fetch_outgoing_link(session, 3)
    assert {:ok, ^link} = SessionState.fetch_incoming_link(session, 19)
  end

  test "the link identity includes the local role" do
    sender = link("shared-name", :sender, 1, 2)
    receiver = link("shared-name", :receiver, 3, 4)

    refute Link.identity(sender) == Link.identity(receiver)

    assert {:ok, session} = SessionState.new(id: 1, links: [sender, receiver])
    assert map_size(session.links_by_identity) == 2
  end

  test "duplicate local handles are rejected while building indexes" do
    first = link("first", :sender, 5, 10)
    second = link("second", :sender, 5, 11)

    assert {:error, {:duplicate_local_handle, 5}} =
             SessionState.new(id: 1, links: [first, second])
  end

  test "duplicate remote handles are rejected while building indexes" do
    first = link("first", :sender, 5, 10)
    second = link("second", :sender, 6, 10)

    assert {:error, {:duplicate_remote_handle, 10}} =
             SessionState.new(id: 1, links: [first, second])
  end

  test "a session identity is required before handle indexes can be constructed" do
    assert {:error, :missing_session_identity} = SessionState.new([])
  end

  test "one session may have different local and remote connection channels" do
    assert {:ok, session} = SessionState.new(id: 23, local_channel: 4, remote_channel: 17)
    assert {:ok, connection} = ConnectionState.new(sessions: [session])

    assert 4 != 17
    assert %{23 => ^session} = connection.sessions_by_identity
    assert %{4 => 23} = connection.local_channel_to_session
    assert %{17 => 23} = connection.remote_channel_to_session
    assert {:ok, ^session} = ConnectionState.fetch_outgoing_session(connection, 4)
    assert {:ok, ^session} = ConnectionState.fetch_incoming_session(connection, 17)
  end

  defp link(name, role, local_handle, remote_handle) do
    %Link{
      name: Value.string(name),
      role: role,
      local_handle: local_handle,
      remote_handle: remote_handle,
      sender_settle_mode: :unsettled,
      receiver_settle_mode: :first
    }
  end
end
