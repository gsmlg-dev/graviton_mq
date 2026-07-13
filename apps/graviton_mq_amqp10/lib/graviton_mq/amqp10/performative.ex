defmodule GravitonMQ.AMQP10.Performative do
  @moduledoc """
  Names AMQP 1.0 performative data without parsing or protocol execution.

  Connection, session, link, flow, transfer, and disposition performatives are
  protocol-layer concepts. They are not broker-core queue commands.
  """

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

  @type t :: {name(), map()}
end
