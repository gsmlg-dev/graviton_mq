defmodule GravitonMQ.Runtime.AMQP10.QueueRequestsTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.Runtime.AMQP10.QueueRequests

  defp add!(requests, queue, kind, deadline) do
    assert {:ok, entry, requests} =
             QueueRequests.add(requests, queue, %{token: make_ref(), kind: kind}, deadline)

    {entry, requests}
  end

  test "entries resolve per queue in FIFO order and only the head deadline is armed" do
    {first, requests} = add!(QueueRequests.new(), :a, :settle, 100)
    {second, requests} = add!(requests, :a, :claim, 150)
    {other, requests} = add!(requests, :b, :status, 120)

    assert %{key: nil, route_token: nil, data: %{}, queue: :a, kind: :settle, deadline: 100} =
             first

    assert first.id < second.id
    assert QueueRequests.size(requests) == 3
    assert QueueRequests.next_deadline(requests, :a) == 100
    assert QueueRequests.next_deadline(requests, :b) == 120
    assert QueueRequests.pending?(requests, :a, :claim)
    refute QueueRequests.pending?(requests, :a, :status)

    assert {:pending, ^first, requests} = QueueRequests.resolve(requests, :a, first.id)
    assert QueueRequests.next_deadline(requests, :a) == 150
    assert {:pending, ^second, requests} = QueueRequests.resolve(requests, :a, second.id)
    assert QueueRequests.next_deadline(requests, :a) == nil
    assert {:unknown, ^requests} = QueueRequests.resolve(requests, :a, second.id)
    assert {:pending, ^other, requests} = QueueRequests.resolve(requests, :b, other.id)
    assert QueueRequests.size(requests) == 0
  end

  test "expire returns only due head entries, which stay counted until their late reply" do
    {first, requests} = add!(QueueRequests.new(), :a, :settle, 100)
    {second, requests} = add!(requests, :a, :claim, 200)
    assert {[], ^requests} = QueueRequests.expire(requests, :a, 99)
    assert {[^first], requests} = QueueRequests.expire(requests, :a, 100)
    assert QueueRequests.next_deadline(requests, :a) == 200
    assert QueueRequests.size(requests) == 2
    assert {:waiting, ^first, requests} = QueueRequests.resolve(requests, :a, first.id)
    assert QueueRequests.size(requests) == 1
    assert {:pending, ^second, requests} = QueueRequests.resolve(requests, :a, second.id)
    assert QueueRequests.size(requests) == 0
  end

  test "abandon_all takes every pending entry of one queue, oldest first" do
    {first, requests} = add!(QueueRequests.new(), :a, :register, 10)
    {second, requests} = add!(requests, :a, :enqueue, 20)
    {_other, requests} = add!(requests, :b, :status, 30)
    assert {[^first, ^second], requests} = QueueRequests.abandon_all(requests, :a)
    assert QueueRequests.next_deadline(requests, :a) == nil
    assert QueueRequests.next_deadline(requests, :b) == 30
    assert QueueRequests.size(requests) == 3
    refute QueueRequests.pending?(requests, :a, :enqueue)
    assert QueueRequests.pending?(requests, :b, :status)
    assert {:waiting, ^second, requests} = QueueRequests.resolve(requests, :a, second.id)
    assert {[], _requests} = QueueRequests.abandon_all(requests, :a)
  end

  test "the cap counts pending and waiting entries" do
    {first, requests} = add!(QueueRequests.new(2), :a, :status, 10)
    {_second, requests} = add!(requests, :b, :status, 10)
    assert QueueRequests.full?(requests)

    assert {:error, :full} =
             QueueRequests.add(requests, :c, %{token: make_ref(), kind: :status}, 10)

    assert {[^first], requests} = QueueRequests.expire(requests, :a, 10)
    assert QueueRequests.full?(requests)
    assert {:waiting, ^first, requests} = QueueRequests.resolve(requests, :a, first.id)
    refute QueueRequests.full?(requests)

    assert {:ok, _entry, _} =
             QueueRequests.add(requests, :c, %{token: make_ref(), kind: :status}, 10)
  end

  test "an exchange take-back classifies into exactly three outcomes" do
    token = make_ref()
    assert QueueRequests.classify([{token, {:request, {:enqueue, "x"}}}]) == :not_executed
    assert QueueRequests.classify([{token, {:result, {:ok, 1}}}]) == {:known, {:ok, 1}}
    assert QueueRequests.classify([]) == :uncertain
  end
end
