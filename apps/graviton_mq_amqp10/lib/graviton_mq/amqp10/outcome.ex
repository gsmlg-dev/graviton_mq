defmodule GravitonMQ.AMQP10.Outcome do
  @moduledoc """
  Exact AMQP 1.0 delivery outcome data before mapping into broker policy.

  This module models outcome fields only. It does not implement Disposition or
  settlement behavior.
  """

  defmodule Accepted do
    @moduledoc "Represents the AMQP 1.0 Accepted outcome."
    defstruct []
    @type t :: %__MODULE__{}
  end

  defmodule Released do
    @moduledoc "Represents the AMQP 1.0 Released outcome."
    defstruct []
    @type t :: %__MODULE__{}
  end

  defmodule Rejected do
    @moduledoc "Represents the AMQP 1.0 Rejected outcome and its optional error."

    defstruct [:error, :encoded_error]

    @type t :: %__MODULE__{
            error: GravitonMQ.AMQP10.Error.t() | nil,
            encoded_error: binary() | nil
          }
  end

  defmodule Modified do
    @moduledoc """
    Represents AMQP Modified flags and exact message-annotation data.

    When annotations are present, `encoded_message_annotations` retains their
    original bytes for lossless passage through the protocol-neutral core.
    """

    defstruct delivery_failed?: false,
              undeliverable_here?: false,
              message_annotations: nil,
              encoded_message_annotations: nil

    @type t :: %__MODULE__{
            delivery_failed?: boolean(),
            undeliverable_here?: boolean(),
            message_annotations: GravitonMQ.AMQP10.Value.map_value() | nil,
            encoded_message_annotations: binary() | nil
          }
  end

  @type t :: Accepted.t() | Released.t() | Rejected.t() | Modified.t()
end
