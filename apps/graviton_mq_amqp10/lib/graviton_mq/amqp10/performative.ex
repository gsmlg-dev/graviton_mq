defmodule GravitonMQ.AMQP10.Performative do
  @moduledoc """
  Defines typed AMQP 1.0 performative data independently of protocol execution.

  The nine transport schemas are modeled by the bounded G3 codec. The structs
  carry exact tagged AMQP field values and typed nested protocol data; none executes
  protocol transitions, negotiates a connection, or performs broker work.
  """

  alias GravitonMQ.AMQP10.Performative.Begin
  alias GravitonMQ.AMQP10.Performative.Attach
  alias GravitonMQ.AMQP10.Performative.Close
  alias GravitonMQ.AMQP10.Performative.Detach
  alias GravitonMQ.AMQP10.Performative.Disposition
  alias GravitonMQ.AMQP10.Performative.End
  alias GravitonMQ.AMQP10.Performative.Flow
  alias GravitonMQ.AMQP10.Performative.Open
  alias GravitonMQ.AMQP10.Performative.Transfer

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

  @type t ::
          Open.t()
          | Begin.t()
          | Attach.t()
          | Flow.t()
          | Transfer.t()
          | Disposition.t()
          | Detach.t()
          | End.t()
          | Close.t()
end
