defmodule GravitonMQ.AMQP10.Settlement do
  @moduledoc """
  Names AMQP 1.0 link and delivery settlement modes.

  Settlement execution and disposition handling are excluded from Milestone 0.
  """

  @type sender_mode :: :unsettled | :settled | :mixed
  @type receiver_mode :: :first | :second
end
