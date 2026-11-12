defmodule GravitonMQ.AMQP10SASLTest.Auth do
  @moduledoc false
  @behaviour GravitonMQ.Runtime.AMQP10.Auth

  @impl true
  def authenticate(credentials, options) do
    test = Keyword.fetch!(options, :test)
    send(test, {:authenticating, self(), Map.delete(credentials, :password)})

    case Keyword.get(options, :mode, :check) do
      :check ->
        if credentials.username == "graviton-user" and
             credentials.password == "graviton-password",
           do: {:ok, {:user, "graviton-user"}},
           else: {:error, {:denied, credentials.password}}

      :sleep ->
        Process.sleep(:infinity)

      :raise ->
        raise ArgumentError, "leaked #{credentials.password}"

      :invalid ->
        {:weird, credentials.password}

      :anonymous ->
        {:ok, {:anonymous_peer, credentials.peer.port}}
    end
  end
end

defmodule GravitonMQ.AMQP10SASLTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.AMQP10.Value
  alias GravitonMQ.AMQP10.Codec.{Frame, Limits, Performative}
  alias GravitonMQ.AMQP10.Codec.SASL, as: SASLCodec
  alias GravitonMQ.AMQP10.Frame, as: Envelope
  alias GravitonMQ.AMQP10.Performative.{Attach, Begin, Open}
  alias GravitonMQ.AMQP10.SASL.{Init, Mechanisms, Outcome, Plain}
  alias GravitonMQ.AMQP10.Terminus.{Source, Target}
  alias GravitonMQ.AMQP10SASLTest.Auth

  @moduletag timeout: 15_000
  @amqp <<"AMQP", 0, 1, 0, 0>>
  @sasl <<"AMQP", 3, 1, 0, 0>>

  test "a PLAIN principal reaches authorize/3 before any Link admission" do
    test = self()

    authorize = fn principal, action, address ->
      send(test, {:authorized, principal, action, address})
      :allow
    end

    broker = plain_broker!(authorize: authorize)
    socket = connect!(broker)

    assert %Outcome{code: %Value{value: 0}} =
             negotiate!(socket, plain_init("graviton-user", "graviton-password"))

    assert_receive {:authenticating, _task,
                    %{mechanism: :plain, username: "graviton-user"} = credentials}

    refute Map.has_key?(credentials, :password)

    assert %{
             peer: %{
               transport: :tcp,
               ip: {127, 0, 0, 1},
               certificate: nil,
               certificate_verified: false
             }
           } = credentials

    attach_sender!(socket)
    assert_receive {:authorized, {:user, "graviton-user"}, :send, "orders"}, 2_000
  end

  defp broker!(options) do
    config =
      Keyword.merge(
        [
          container_id: "sasl-#{System.unique_integer([:positive])}",
          auth: :anonymous,
          queues: ["orders"],
          endpoint_options: [max_frame_size: 512],
          listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
        ],
        options
      )

    start_supervised!({GravitonMQ, name: nil, runtime_supervisor_name: nil, amqp10: config},
      id: make_ref()
    )
  end

  defp plain_broker!(options \\ []) do
    {auth_options, options} = Keyword.pop(options, :auth_options, [])

    broker!(
      Keyword.merge(
        [auth: {Auth, [test: self()] ++ auth_options}, allow_insecure_plain: true],
        options
      )
    )
  end

  defp connect!(broker) do
    assert {:ok, %{listeners: [%{port: port}]}} = GravitonMQ.amqp10_status(broker)
    {:ok, socket} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 1_000)
    on_exit(fn -> :gen_tcp.close(socket) end)
    socket
  end

  defp negotiate!(socket, init) do
    :ok = :gen_tcp.send(socket, @sasl)
    assert {:ok, @sasl} = :gen_tcp.recv(socket, 8, 1_000)
    assert %Mechanisms{} = read_sasl!(socket)
    :ok = :gen_tcp.send(socket, sasl_frame(init))
    read_sasl!(socket)
  end

  defp attach_sender!(socket) do
    :ok = :gen_tcp.send(socket, @amqp <> wire(%Open{container_id: Value.string("raw-peer")}))
    assert {:ok, @amqp} = :gen_tcp.recv(socket, 8, 1_000)
    assert %Open{} = read_pdu!(socket)

    :ok =
      :gen_tcp.send(
        socket,
        wire(
          %Begin{
            next_outgoing_id: Value.uint(0),
            incoming_window: Value.uint(32),
            outgoing_window: Value.uint(32)
          },
          1
        )
      )

    assert %Begin{} = read_pdu!(socket)

    attach = %Attach{
      name: Value.string("sasl-sender"),
      handle: Value.uint(0),
      role: Value.boolean(false),
      snd_settle_mode: Value.ubyte(0),
      rcv_settle_mode: Value.ubyte(0),
      source: %Source{address: Value.string("raw")},
      target: %Target{address: Value.string("orders")},
      initial_delivery_count: Value.uint(0)
    }

    :ok = :gen_tcp.send(socket, wire(attach, 1))
  end

  defp plain_init(username, password) do
    {:ok, message} = Plain.encode(%Plain{username: username, password: password})

    %Init{
      mechanism: Value.symbol("PLAIN"),
      initial_response: Value.binary(message)
    }
  end

  defp sasl_frame(body) do
    {:ok, bytes} = SASLCodec.encode_frame(body)
    bytes
  end

  defp read_sasl!(socket) do
    {:ok, <<size::32, _::binary>> = header} = :gen_tcp.recv(socket, 8, 1_000)
    {:ok, body} = :gen_tcp.recv(socket, size - 8, 1_000)

    {:ok, %Envelope{type: :sasl} = frame, <<>>} =
      Frame.decode(header <> body, Limits.default(), types: [:sasl])

    {:ok, decoded} = SASLCodec.decode(frame.body)
    decoded
  end

  defp wire(pdu, channel \\ 0) do
    {:ok, body} = Performative.encode(pdu)

    {:ok, bytes} =
      Frame.encode(%Envelope{
        declared_size: 8 + byte_size(body),
        data_offset_words: 2,
        type: :amqp,
        channel: channel,
        extended_header: <<>>,
        body: body
      })

    bytes
  end

  defp read_pdu!(socket) do
    {:ok, <<size::32, _::binary>> = header} = :gen_tcp.recv(socket, 8, 1_000)
    body = if size > 8, do: elem(:gen_tcp.recv(socket, size - 8, 1_000), 1), else: <<>>
    {:ok, frame, <<>>} = Frame.decode(header <> body)
    {:ok, pdu, _payload} = Performative.decode(frame.body)
    pdu
  end
end
