defmodule GravitonMQ.Core.DurableIdentityTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.Core.CommitRef
  alias GravitonMQ.Core.DeliveryRef
  alias GravitonMQ.Core.MessageId
  alias GravitonMQ.Core.NodeId
  alias GravitonMQ.Queue.EventId

  test "durable broker identities contain only stable binary or integer values" do
    assert %MessageId{value: "message-1"} = MessageId.new("message-1")
    assert %NodeId{value: "queue/orders"} = NodeId.new("queue/orders")
    assert %DeliveryRef{value: "delivery-1"} = DeliveryRef.new("delivery-1")
    assert %EventId{value: "event-1"} = EventId.new("event-1")

    assert %CommitRef{stream_id: "queue/orders", position: 27} =
             CommitRef.new("queue/orders", 27)
  end

  test "transient protocol-shaped integers and runtime references are not accepted as IDs" do
    assert_raise FunctionClauseError, fn -> MessageId.new(1) end
    assert_raise FunctionClauseError, fn -> NodeId.new(make_ref()) end
    assert_raise FunctionClauseError, fn -> DeliveryRef.new(self()) end
    assert_raise FunctionClauseError, fn -> EventId.new({:delivery_id, 9}) end
    assert_raise FunctionClauseError, fn -> CommitRef.new("queue/orders", make_ref()) end
  end
end
