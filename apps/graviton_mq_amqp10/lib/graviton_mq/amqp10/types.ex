defmodule GravitonMQ.AMQP10.Types do
  @moduledoc """
  Names AMQP 1.0 scalar concepts independently of binary encoding and I/O.

  These types do not implement the AMQP type system or codec.
  """

  @type uint :: 0..4_294_967_295
  @type ushort :: 0..65_535
  @type handle :: uint()
  @type delivery_number :: uint()
  @type symbol :: binary()
end
