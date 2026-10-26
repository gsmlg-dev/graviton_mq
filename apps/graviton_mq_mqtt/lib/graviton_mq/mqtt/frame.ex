defmodule GravitonMQ.MQTT.Frame do
  @moduledoc """
  MQTT fixed header and opaque packet body.

  `header_size` records the decoded header's byte size, including Remaining
  Length. Encoding chooses the canonical header independently of this metadata.
  """

  @enforce_keys [:type, :body]
  defstruct [:type, :body, :header_size, flags: 0]

  @type t :: %__MODULE__{
          type: 1..15,
          flags: 0..15,
          body: binary(),
          header_size: pos_integer() | nil
        }
end
