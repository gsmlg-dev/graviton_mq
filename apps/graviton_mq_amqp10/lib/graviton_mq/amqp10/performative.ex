defmodule GravitonMQ.AMQP10.Performative do
  @moduledoc """
  Defines typed AMQP 1.0 performative data independently of protocol execution.

  Only the Open and Begin schemas are modeled by the bounded codec foundation.
  The structs carry exact tagged AMQP field values; they do not negotiate a
  connection, transition session state, or perform broker work.
  """

  alias GravitonMQ.AMQP10.Performative.Begin
  alias GravitonMQ.AMQP10.Performative.Open

  @type name ::
          :open
          | :begin
          | :attach
          | :flow
          | :transfer
          | :disposition
          | :detach
          | :end
          | :close

  @type t :: Open.t() | Begin.t()
end
