defmodule GravitonMQ.AMQP10Client.SASLTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10Client, as: Client
  alias GravitonMQ.AMQP10.Value
  alias GravitonMQ.AMQP10.Codec.{Frame, Limits, Performative}
  alias GravitonMQ.AMQP10.Codec.SASL, as: SASLCodec
  alias GravitonMQ.AMQP10.Frame, as: Envelope
  alias GravitonMQ.AMQP10.Performative.Open
  alias GravitonMQ.AMQP10.SASL.{Challenge, Init, Mechanisms, Outcome, Plain}
  alias GravitonMQ.Client.AMQP10.Config

  @amqp <<"AMQP", 0, 1, 0, 0>>
  @sasl <<"AMQP", 3, 1, 0, 0>>

  test "SASL configuration is validated without echoing values" do
    long = String.duplicate("a", 255)

    for options <- [
          [sasl: :scram],
          [sasl: {:plain, "u", "p"}],
          [sasl: {:plain, "", "p"}, allow_insecure_plain: true],
          [sasl: {:plain, "u", "p\0"}, allow_insecure_plain: true],
          [sasl: {:plain, "u", "p", authzid: <<255>>}, allow_insecure_plain: true],
          [sasl: {:plain, "u", "p", other: 1}, allow_insecure_plain: true],
          [sasl: {:plain, long, long, authzid: long}, allow_insecure_plain: true],
          [sasl: {:anonymous, <<0xFF>>}],
          [sasl: :anonymous, sasl_timeout: 0],
          [sasl: :anonymous, sasl_timeout: 10_001],
          [allow_insecure_plain: :yes]
        ] do
      assert {:error, :invalid_options} =
               Config.build(Keyword.merge([container_id: "c", observer: self()], options))
    end

    assert {:ok, %{sasl: :none, sasl_timeout: 5_000}} =
             Config.build(container_id: "c", observer: self())

    assert {:ok, %{sasl: {:anonymous, nil}}} =
             Config.build(container_id: "c", observer: self(), sasl: :anonymous)

    assert {:ok, %{sasl: {:anonymous, "trace"}}} =
             Config.build(container_id: "c", observer: self(), sasl: {:anonymous, "trace"})

    assert {:ok, %{sasl: {:plain, "u", "p", "z"}}} =
             Config.build(
               container_id: "c",
               observer: self(),
               transport: :tls,
               sasl: {:plain, "u", "p", authzid: "z"}
             )

    provider = fn -> "p" end

    assert {:ok, %{sasl: {:plain, "u", ^provider, nil}}} =
             Config.build(
               container_id: "c",
               observer: self(),
               sasl: {:plain, "u", provider},
               allow_insecure_plain: true
             )
  end
end
