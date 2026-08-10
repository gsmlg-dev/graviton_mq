defmodule GravitonMQ.AMQP10.Types do
  @moduledoc """
  Names bounded AMQP 1.0 field domains independently of binary encoding.

  Decoded general values use `GravitonMQ.AMQP10.Value`; these aliases are for
  protocol fields whose semantic AMQP type is fixed by the specification.
  """

  @type ubyte :: 0..255
  @type uint :: 0..4_294_967_295
  @type ushort :: 0..65_535
  @type ulong :: 0..18_446_744_073_709_551_615
  @type signed_byte :: -128..127
  @type signed_short :: -32_768..32_767
  @type signed_int :: -2_147_483_648..2_147_483_647
  @type signed_long :: -9_223_372_036_854_775_808..9_223_372_036_854_775_807
  @type handle :: uint()
  @type delivery_number :: uint()
  @type symbol :: GravitonMQ.AMQP10.Value.symbol_value()
end
