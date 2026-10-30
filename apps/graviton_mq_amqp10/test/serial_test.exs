defmodule GravitonMQ.AMQP10.SerialTest do
  use ExUnit.Case, async: true
  alias GravitonMQ.AMQP10.Serial

  test "32 bit serial arithmetic wraps without expanding ranges" do
    assert Code.ensure_loaded?(Serial)
    assert Serial.add(0xFFFF_FFFF, 1) == 0
    assert Serial.distance(0xFFFF_FFFE, 1) == 3
    assert Serial.compare(0xFFFF_FFFF, 0) == :lt
    assert Serial.compare(0, 0xFFFF_FFFF) == :gt
    assert Serial.compare(7, 7) == :eq
    assert Serial.compare(0, 0x8000_0000) == :ambiguous
    assert Serial.span(0xFFFF_FFFE, 1) == {:ok, 4}
    assert Serial.member?(0, 0xFFFF_FFFE, 1)
    refute Serial.member?(2, 0xFFFF_FFFE, 1)
    assert Serial.span(0, 0x8000_0000) == {:error, :ambiguous_range}
  end
end
