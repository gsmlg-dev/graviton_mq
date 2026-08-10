defmodule GravitonMQ.Core.Outcome do
  @moduledoc """
  Protocol-independent instructions resulting from delivery settlement.

  A protocol frontend maps its outcomes into these data structures. Successful
  retirement, plain release, rejection, and modification remain distinct so
  AMQP Accepted, Released, Rejected, and Modified semantics are not collapsed.
  """

  defmodule Retire do
    @moduledoc "Retires a successfully processed message from the queue."
    defstruct []
    @type t :: %__MODULE__{}
  end

  defmodule MakeAvailable do
    @moduledoc "Makes an unchanged message available for another delivery."
    defstruct []
    @type t :: %__MODULE__{}
  end

  defmodule Reject do
    @moduledoc "Rejects a message and records the selected discard policy."

    @enforce_keys [:reason, :action]
    defstruct [:reason, :description, :details, :action]

    @type action :: :discard | :dead_letter
    @type t :: %__MODULE__{
            reason: binary(),
            description: binary() | nil,
            details: GravitonMQ.Core.Message.Mutation.t() | nil,
            action: action()
          }
  end

  defmodule ModifyAndMakeAvailable do
    @moduledoc """
    Makes a message available with redelivery hints and opaque mutation data.
    """

    defstruct delivery_failed?: false,
              undeliverable_here?: false,
              annotation_mutation: nil

    @type t :: %__MODULE__{
            delivery_failed?: boolean(),
            undeliverable_here?: boolean(),
            annotation_mutation: GravitonMQ.Core.Message.Mutation.t() | nil
          }
  end

  @type t :: Retire.t() | MakeAvailable.t() | Reject.t() | ModifyAndMakeAvailable.t()
end
