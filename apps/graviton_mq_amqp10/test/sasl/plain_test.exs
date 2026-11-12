defmodule GravitonMQ.AMQP10.SASL.PlainTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.SASL.Plain

  test "parses RFC 4616 messages with and without an authorization identity" do
    assert {:ok, %Plain{authzid: nil, username: "tim", password: "tanstaaftanstaaf"}} =
             Plain.parse(<<0, "tim", 0, "tanstaaftanstaaf">>)

    assert {:ok, %Plain{authzid: "Ursel", username: "Kurt", password: "xipj3plmq"}} =
             Plain.parse(<<"Ursel", 0, "Kurt", 0, "xipj3plmq">>)

    assert {:ok, %Plain{username: "ü"}} = Plain.parse(<<0, "ü", 0, "p">>)
  end

  test "rejects structure, lengths, NUL and invalid UTF-8 without detail" do
    long = String.duplicate("a", 256)

    for message <- [
          <<>>,
          <<"user">>,
          <<0, "user">>,
          <<0, 0, "pass">>,
          <<0, "user", 0>>,
          <<0, "u", 0, "p", 0, "extra">>,
          <<0, long::binary, 0, "p">>,
          <<0, "u", 0, long::binary>>,
          <<long::binary, 0, "u", 0, "p">>,
          <<0, 255, 0, "p">>,
          <<0, "u", 0, 0xC3>>
        ] do
      assert Plain.parse(message) == {:error, :invalid_plain_message}
    end

    assert Plain.parse(:not_binary) == {:error, :invalid_plain_message}
  end

  test "encode and parse round-trip for generated valid credentials" do
    :rand.seed(:exsss, {2026, 11, 12})
    alphabet = Enum.concat([?a..?z, ?A..?Z, ?0..?9, [?é, ?中, ?-]])

    for _ <- 1..300 do
      field = fn minimum ->
        length = Enum.random(minimum..40)
        for(_ <- 1..length//1, into: "", do: <<Enum.random(alphabet)::utf8>>)
      end

      authzid = if :rand.uniform(2) == 1, do: nil, else: field.(1)
      {:ok, plain} = Plain.new(field.(1), field.(1), authzid)
      assert {:ok, ^plain} = plain |> Plain.encode() |> Plain.parse()
    end
  end

  test "new/3 validates fields and normalizes an empty authzid" do
    assert {:ok, %Plain{authzid: nil}} = Plain.new("u", "p", "")
    assert {:error, :invalid_plain_message} = Plain.new("", "p")
    assert {:error, :invalid_plain_message} = Plain.new("u", "")
    assert {:error, :invalid_plain_message} = Plain.new("u", "p\0q")
    assert {:error, :invalid_plain_message} = Plain.new(:u, "p")
  end

  test "inspect never prints any credential field" do
    {:ok, plain} = Plain.new("visible-user", "visible-password", "visible-authz")
    rendered = inspect(plain)
    refute rendered =~ "visible"
    assert rendered == "#GravitonMQ.AMQP10.SASL.Plain<redacted>"
  end
end
