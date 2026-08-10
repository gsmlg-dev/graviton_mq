defmodule GravitonMQ.AMQP10.OutcomeMapperTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Error, as: AMQPError
  alias GravitonMQ.AMQP10.Outcome.Accepted
  alias GravitonMQ.AMQP10.Outcome.Modified
  alias GravitonMQ.AMQP10.Outcome.Rejected
  alias GravitonMQ.AMQP10.Outcome.Released
  alias GravitonMQ.AMQP10.OutcomeMapper
  alias GravitonMQ.AMQP10.Value
  alias GravitonMQ.Core.Message
  alias GravitonMQ.Core.Outcome

  test "Accepted maps to terminal message retirement" do
    assert %Outcome.Retire{} = OutcomeMapper.to_core(%Accepted{})
  end

  test "Released maps to unchanged availability" do
    assert %Outcome.MakeAvailable{} = OutcomeMapper.to_core(%Released{})
  end

  test "Rejected preserves the reason and selected discard policy" do
    error = %AMQPError{
      condition: Value.symbol("amqp:unauthorized-access"),
      description: Value.string("denied"),
      info: Value.map([{Value.symbol("policy"), Value.string("restricted")}])
    }

    encoded_error = <<0, 83, 29, 192, 1, 0>>

    assert %Outcome.Reject{
             reason: "amqp:unauthorized-access",
             description: "denied",
             action: :dead_letter,
             details: %Message.Mutation{
               format: "amqp-1.0/error",
               encoded_data: ^encoded_error
             }
           } =
             OutcomeMapper.to_core(
               %Rejected{error: error, encoded_error: encoded_error},
               rejection_action: :dead_letter
             )
  end

  test "Rejected refuses to discard parsed error info without its encoded form" do
    error = %AMQPError{
      condition: Value.symbol("amqp:resource-limit-exceeded"),
      info: Value.map([{Value.symbol("limit"), Value.uint(10)}])
    }

    assert_raise ArgumentError,
                 "encoded error data is required for lossless core outcome mapping",
                 fn ->
                   OutcomeMapper.to_core(%Rejected{error: error, encoded_error: nil})
                 end
  end

  test "Modified preserves redelivery flags and opaque message-annotation mutation data" do
    annotations =
      Value.map([
        {Value.symbol("x-opt-delivery-count"), Value.ulong(2)},
        {Value.symbol("x-opt-route"), Value.string("alternate")}
      ])

    encoded_annotations = <<0, 83, 114, 193, 1, 0>>

    modified = %Modified{
      delivery_failed?: true,
      undeliverable_here?: true,
      message_annotations: annotations,
      encoded_message_annotations: encoded_annotations
    }

    assert ^annotations = modified.message_annotations

    assert %Outcome.ModifyAndMakeAvailable{
             delivery_failed?: true,
             undeliverable_here?: true,
             annotation_mutation: %Message.Mutation{
               format: "amqp-1.0/message-annotations",
               encoded_data: ^encoded_annotations
             }
           } = OutcomeMapper.to_core(modified)
  end
end
