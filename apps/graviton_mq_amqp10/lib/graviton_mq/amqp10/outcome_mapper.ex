defmodule GravitonMQ.AMQP10.OutcomeMapper do
  @moduledoc """
  Maps AMQP 1.0 outcomes to protocol-independent broker instructions.

  The mapping is pure data transformation. It performs no settlement, queue
  transition, persistence, or network operation.
  """

  alias GravitonMQ.AMQP10.Outcome
  alias GravitonMQ.Core.Message
  alias GravitonMQ.Core.Outcome, as: CoreOutcome

  @spec to_core(Outcome.t(), keyword()) :: CoreOutcome.t()
  def to_core(outcome, options \\ [])

  def to_core(%Outcome.Accepted{}, _options), do: %CoreOutcome.Retire{}

  def to_core(%Outcome.Released{}, _options), do: %CoreOutcome.MakeAvailable{}

  def to_core(
        %Outcome.Rejected{
          error: %GravitonMQ.AMQP10.Error{info: %GravitonMQ.AMQP10.Value{}},
          encoded_error: nil
        },
        _options
      ) do
    raise ArgumentError,
          "encoded error data is required for lossless core outcome mapping"
  end

  def to_core(%Outcome.Rejected{error: error, encoded_error: encoded_error}, options) do
    {reason, description} = rejection_reason(error)

    %CoreOutcome.Reject{
      reason: reason,
      description: description,
      details: mutation("amqp-1.0/error", encoded_error),
      action: rejection_action(options)
    }
  end

  def to_core(
        %Outcome.Modified{
          message_annotations: %GravitonMQ.AMQP10.Value{},
          encoded_message_annotations: nil
        },
        _options
      ) do
    raise ArgumentError,
          "encoded message annotations are required for lossless core outcome mapping"
  end

  def to_core(%Outcome.Modified{} = outcome, _options) do
    %CoreOutcome.ModifyAndMakeAvailable{
      delivery_failed?: outcome.delivery_failed?,
      undeliverable_here?: outcome.undeliverable_here?,
      annotation_mutation:
        mutation("amqp-1.0/message-annotations", outcome.encoded_message_annotations)
    }
  end

  defp rejection_reason(nil), do: {"rejected", nil}

  defp rejection_reason(%GravitonMQ.AMQP10.Error{
         condition: %GravitonMQ.AMQP10.Value{type: :symbol, value: reason},
         description: description
       }) do
    {reason, string_value(description)}
  end

  defp string_value(nil), do: nil
  defp string_value(%GravitonMQ.AMQP10.Value{type: :string, value: value}), do: value

  defp mutation(_format, nil), do: nil

  defp mutation(format, encoded_data) when is_binary(encoded_data) do
    %Message.Mutation{format: format, encoded_data: encoded_data}
  end

  defp rejection_action(options) do
    case Keyword.get(options, :rejection_action, :discard) do
      action when action in [:discard, :dead_letter] -> action
      action -> raise ArgumentError, "invalid rejection action: #{inspect(action)}"
    end
  end
end
