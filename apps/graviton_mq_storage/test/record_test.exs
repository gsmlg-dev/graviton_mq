defmodule GravitonMQ.Storage.RecordTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.Core.CommitRef
  alias GravitonMQ.Storage.Record

  test "a physical record wraps encoded logical event data and storage metadata" do
    position = CommitRef.new("queue/orders", 12)

    record = %Record{
      format_version: 1,
      position: position,
      record_type: "queue_event",
      encoded_event: <<131, 116, 0, 0, 0, 0>>,
      checksum: <<1, 2, 3, 4>>,
      segment_id: 2
    }

    assert %Record{
             format_version: 1,
             position: ^position,
             record_type: "queue_event",
             encoded_event: encoded_event,
             checksum: checksum,
             segment_id: 2
           } = record

    assert is_binary(encoded_event)
    assert is_binary(checksum)
    refute Map.has_key?(record, :payload)
  end
end
