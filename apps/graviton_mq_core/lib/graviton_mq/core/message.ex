defmodule GravitonMQ.Core.Message do
  @moduledoc """
  Protocol-independent message data owned by the broker core.

  The original encoded content is authoritative for forwarding. `index` is a
  derived view containing only broker routing, policy, expiration, durability,
  and management data. The core neither imports protocol message structs nor
  treats a decoded `body: term()` as the message of record.
  """

  defmodule Format do
    @moduledoc """
    Opaque protocol-family and format identifier attached to encoded content.
    """

    @enforce_keys [:family, :identifier]
    defstruct [:family, :identifier]

    @type t :: %__MODULE__{family: binary(), identifier: non_neg_integer()}
  end

  defmodule Index do
    @moduledoc """
    Parsed protocol-neutral fields needed by broker policy and management.
    """

    defstruct routing: %{},
              policy: %{},
              expires_at: nil,
              durability: %{},
              management: %{}

    @type value ::
            nil
            | boolean()
            | integer()
            | float()
            | binary()
            | [value()]
            | %{optional(binary()) => value()}

    @type t :: %__MODULE__{
            routing: %{optional(binary()) => value()},
            policy: %{optional(binary()) => value()},
            expires_at: integer() | nil,
            durability: %{optional(binary()) => value()},
            management: %{optional(binary()) => value()}
          }
  end

  defmodule Mutation do
    @moduledoc """
    Opaque serializable mutation data carried across a protocol-neutral core.

    A protocol adapter owns the format and can later interpret or re-encode
    the bytes; core queue policy can retain them without importing that
    adapter's value structs.
    """

    @enforce_keys [:format, :encoded_data]
    defstruct [:format, :encoded_data]

    @type t :: %__MODULE__{format: binary(), encoded_data: binary()}
  end

  @enforce_keys [:id, :encoded_content, :wire_format, :index]
  defstruct [:id, :encoded_content, :wire_format, :index, durable?: false]

  @type id :: GravitonMQ.Core.MessageId.t()
  @type t :: %__MODULE__{
          id: id(),
          encoded_content: binary(),
          wire_format: Format.t(),
          index: Index.t(),
          durable?: boolean()
        }
end
