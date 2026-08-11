defmodule GravitonMQ.AMQP10.Performative do
  @moduledoc """
  Defines typed AMQP 1.0 performative data independently of protocol execution.

  The Open, Begin, End, and Close schemas are modeled by the bounded codec
  foundation. The structs carry exact tagged AMQP field values; none executes
  protocol transitions, negotiates a connection, or performs broker work.
  """

  alias GravitonMQ.AMQP10.Performative.Begin
  alias GravitonMQ.AMQP10.Performative.Close
  alias GravitonMQ.AMQP10.Performative.End
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

  @type t :: Open.t() | Begin.t() | End.t() | Close.t()
end
