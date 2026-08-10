defmodule GravitonMQ.Queue.EventTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.Core.MessageId
  alias GravitonMQ.Core.NodeId
  alias GravitonMQ.Queue.Event
  alias GravitonMQ.Queue.EventId

  test "logical persistence events contain only broker identities and serializable data" do
    event = %Event{
      id: EventId.new("event-7"),
      node_id: NodeId.new("queue/orders"),
      sequence: 7,
      type: "message_enqueued",
      data: %{"message_id" => MessageId.new("message-9").value}
    }

    assert %Event{
             id: %EventId{value: "event-7"},
             node_id: %NodeId{value: "queue/orders"},
             sequence: 7,
             type: "message_enqueued",
             data: %{"message_id" => "message-9"}
           } = event

    for forbidden <- [
          :session_id,
          :link_handle,
          :delivery_id,
          :pid,
          :reference,
          :checksum,
          :segment_offset
        ] do
      refute Map.has_key?(event, forbidden)
    end
  end
end
