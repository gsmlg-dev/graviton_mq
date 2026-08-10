defmodule GravitonMQ.Core.MessageTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.Core.Message
  alias GravitonMQ.Core.MessageId

  test "the broker message keeps opaque wire content beside a parsed broker index" do
    id = MessageId.new("message-1")
    format = %Message.Format{family: "amqp-1.0", identifier: 7}

    index = %Message.Index{
      routing: %{"address" => "orders"},
      policy: %{"priority" => 4},
      expires_at: 1_800_000_000_000,
      durability: %{"requested" => true},
      management: %{"content_type" => "application/octet-stream"}
    }

    encoded_content = <<0, 83, 112, 69, 0, 83, 117, 160, 2, 1, 2>>

    message = %Message{
      id: id,
      encoded_content: encoded_content,
      wire_format: format,
      index: index,
      durable?: true
    }

    assert %Message{
             id: ^id,
             encoded_content: ^encoded_content,
             wire_format: ^format,
             index: ^index,
             durable?: true
           } = message

    refute Map.has_key?(message, :body)
  end

  test "opaque mutation data is serializable without importing a protocol value type" do
    mutation = %Message.Mutation{
      format: "amqp-1.0/message-annotations",
      encoded_data: <<0, 83, 114, 193, 1, 0>>
    }

    assert %Message.Mutation{format: format, encoded_data: encoded_data} = mutation
    assert is_binary(format)
    assert is_binary(encoded_data)
  end
end
