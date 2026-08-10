defmodule GravitonMQ.Core.OutcomeTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.Core.Message
  alias GravitonMQ.Core.Outcome

  test "terminal success, release, rejection, and modification are distinct instructions" do
    retire = %Outcome.Retire{}
    make_available = %Outcome.MakeAvailable{}

    reject = %Outcome.Reject{
      reason: "amqp:unauthorized-access",
      description: "denied",
      action: :dead_letter
    }

    mutation = %Message.Mutation{
      format: "amqp-1.0/message-annotations",
      encoded_data: <<1, 2, 3>>
    }

    modify = %Outcome.ModifyAndMakeAvailable{
      delivery_failed?: true,
      undeliverable_here?: true,
      annotation_mutation: mutation
    }

    assert MapSet.size(MapSet.new([retire, make_available, reject, modify])) == 4

    assert %Outcome.Reject{reason: "amqp:unauthorized-access", action: :dead_letter} = reject

    assert %Outcome.ModifyAndMakeAvailable{
             delivery_failed?: true,
             undeliverable_here?: true,
             annotation_mutation: ^mutation
           } = modify
  end
end
