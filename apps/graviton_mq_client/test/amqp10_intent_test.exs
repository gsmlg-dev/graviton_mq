defmodule GravitonMQ.Client.AMQP10.IntentTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.Client.AMQP10.Intent

  @a {"a", :sender}
  @b {"b", :receiver}

  defp declared do
    Intent.new()
    |> Intent.put_session(2, [])
    |> Intent.put_session(1, incoming_window: 8)
    |> Intent.put_link(1, @a, address: "q")
    |> Intent.put_link(1, @b, address: "r")
    |> Intent.put_link(9, {"orphan", :sender}, [])
  end

  test "declarations are bounded identities and options, removable by the host or peer" do
    intent = declared()
    assert Intent.counts(intent) == %{sessions: 2, links: 2}
    assert Intent.counts(Intent.drop_link(intent, 1, @a)) == %{sessions: 2, links: 1}
    assert Intent.counts(Intent.drop_session(intent, 1)) == %{sessions: 1, links: 0}
    refute Intent.pending?(intent, 1, nil)
  end

  test "restoration opens Sessions in ascending order, then each Session's Links in order" do
    intent = Intent.start(declared())
    assert Intent.pending?(intent, 1, nil) and Intent.pending?(intent, 2, @a)
    assert Intent.restoring(intent) == 4

    assert {[{{:session, 1}, {:open_session, 1, [incoming_window: 8]}}], intent} =
             Intent.take(intent, 1)

    assert {[{{:session, 2}, {:open_session, 2, []}}], intent} = Intent.take(intent, 8)
    assert {[], intent} = Intent.take(intent, 8)

    assert {intent, [{{:session, 1}, {:ok, 1}}]} =
             Intent.complete(intent, {:session, 1}, {:ok, 1})

    refute Intent.pending?(intent, 1, nil)
    assert Intent.pending?(intent, 1, @a)

    assert {[
              {{:link, 1, @a}, {:session, 1, {:attach, @a, [address: "q"]}}},
              {{:link, 1, @b}, {:session, 1, {:attach, @b, [address: "r"]}}}
            ], intent} = Intent.take(intent, 8)

    {intent, _} = Intent.complete(intent, {:link, 1, @a}, {:ok, @a})

    {intent, [{{:link, 1, @b}, {:error, :refused}}]} =
      Intent.complete(intent, {:link, 1, @b}, {:error, :refused})

    refute Intent.finished?(intent)
    {intent, _} = Intent.complete(intent, {:session, 2}, {:ok, 2})
    assert Intent.finished?(intent)
    assert {%{restored: 3, failed: 1}, intent} = Intent.finish(intent)
    assert Intent.counts(intent) == %{sessions: 2, links: 1}
    refute Intent.finished?(intent)
  end

  test "a failed Session drops its Links with explicit reports; interruption keeps entries" do
    intent = Intent.start(declared())
    {_commands, intent} = Intent.take(intent, 8)

    assert {intent,
            [
              {{:session, 1}, {:unknown, :timeout}},
              {{:link, 1, @a}, {:error, :session_not_restored}},
              {{:link, 1, @b}, {:error, :session_not_restored}}
            ]} = Intent.complete(intent, {:session, 1}, {:unknown, :timeout})

    assert Intent.counts(intent) == %{sessions: 1, links: 0}
    intent = intent |> Intent.interrupt() |> Intent.stop()
    assert Intent.counts(intent) == %{sessions: 1, links: 0}
    refute Intent.waiting?(intent)
    assert {[{{:session, 2}, _}], _} = intent |> Intent.start() |> Intent.take(8)
  end
end
