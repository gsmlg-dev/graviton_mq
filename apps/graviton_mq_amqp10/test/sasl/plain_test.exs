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
      assert {:ok, message} = Plain.encode(plain)
      assert {:ok, ^plain} = Plain.parse(message)
    end
  end

  test "new/3 validates fields and normalizes an empty authzid" do
    assert {:ok, %Plain{authzid: nil}} = Plain.new("u", "p", "")
    assert {:error, :invalid_plain_message} = Plain.new("", "p")
    assert {:error, :invalid_plain_message} = Plain.new("u", "")
    assert {:error, :invalid_plain_message} = Plain.new("u", "p\0q")
    assert {:error, :invalid_plain_message} = Plain.new(:u, "p")
  end

  test "fields of exactly 255 octets are accepted and 256 octets are refused" do
    max = String.duplicate("a", 255)
    over = String.duplicate("a", 256)

    assert {:ok, %Plain{username: ^max, password: ^max, authzid: ^max} = plain} =
             Plain.new(max, max, max)

    assert {:ok, message} = Plain.encode(plain)
    assert byte_size(message) == 767
    assert {:ok, ^plain} = Plain.parse(message)

    for {username, password, authzid} <- [{over, "p", nil}, {"u", over, nil}, {"u", "p", over}] do
      assert Plain.new(username, password, authzid) == {:error, :invalid_plain_message}

      assert Plain.parse((authzid || "") <> <<0>> <> username <> <<0>> <> password) ==
               {:error, :invalid_plain_message}
    end
  end

  test "encode re-validates fields and never raises or emits ambiguous bytes" do
    for plain <- [
          %Plain{username: "a\0b", password: "p"},
          %Plain{username: "u", password: "p\0q"},
          %Plain{username: "u", password: "p", authzid: "z\0"},
          %Plain{username: "", password: "p"},
          %Plain{username: "u", password: ""},
          %Plain{username: String.duplicate("u", 256), password: "p"},
          %Plain{username: "u", password: <<0xFF>>},
          %Plain{username: :user, password: "sentinel-password"},
          %Plain{username: "u", password: ~c"sentinel-password"},
          %Plain{username: "u", password: "p", authzid: :admin}
        ] do
      assert Plain.encode(plain) == {:error, :invalid_plain_message}
    end

    assert Plain.encode(%Plain{username: "u", password: "p", authzid: ""}) ==
             {:ok, <<0, "u", 0, "p">>}

    assert Plain.encode(:not_plain) == {:error, :invalid_plain_message}
  end

  test "inspect never prints any credential field" do
    {:ok, plain} = Plain.new("visible-user", "visible-password", "visible-authz")
    rendered = inspect(plain)
    refute rendered =~ "visible"
    assert rendered == "#GravitonMQ.AMQP10.SASL.Plain<redacted>"
  end
end
