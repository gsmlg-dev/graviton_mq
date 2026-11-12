defmodule GravitonMQ.AMQP10.SASL.AnonymousTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.SASL.Anonymous

  test "absent, empty and valid traces are accepted; traces are informational" do
    assert Anonymous.parse(nil) == {:ok, nil}
    assert Anonymous.parse(<<>>) == {:ok, nil}
    assert Anonymous.parse("sirhc") == {:ok, "sirhc"}
    assert Anonymous.parse("user@example.com") == {:ok, "user@example.com"}
    assert {:ok, _} = Anonymous.parse(String.duplicate("é", 255))
  end

  test "invalid UTF-8, more than 255 characters and non-binaries are rejected" do
    for trace <- [<<0xFF>>, String.duplicate("a", 256), String.duplicate("é", 256), 7] do
      assert Anonymous.parse(trace) == {:error, :invalid_anonymous_message}
    end
  end
end
