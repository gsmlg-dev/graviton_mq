defmodule GravitonMQ.AMQP10.MessageProtonTest do
  use ExUnit.Case, async: false

  @moduletag :proton_interop
  @moduletag timeout: 30_000

  setup do
    broker =
      start_supervised!(
        {GravitonMQ,
         name: nil,
         runtime_supervisor_name: nil,
         amqp10: [
           container_id: "message-corpus-#{System.unique_integer([:positive])}",
           auth: :anonymous,
           queues: ["messages"],
           endpoint_options: [max_frame_size: 512],
           listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
         ]}
      )

    assert {:ok, %{listeners: [%{port: port}]}} = GravitonMQ.amqp10_status(broker)
    %{broker: broker, url: "amqp://127.0.0.1:#{port}"}
  end

  test "Proton release and acquisition by a different Link cannot retain first-acquirer true",
       context do
    output = peer!(context.url, "first-acquirer")
    assert output =~ "\"reacquired_first_acquirer\": false"

    assert {:ok, %{queues: %{"messages" => %{messages: 0}}}} =
             GravitonMQ.amqp10_status(context.broker)
  end

  test "Proton preserves exact noncanonical bare sections for all body and message ID kinds",
       context do
    output = peer!(context.url, "forwarding")
    assert output =~ "\"fixtures\": 12"
    assert output =~ "\"body_kinds\": [\"data\", \"sequence\", \"value\"]"
    assert output =~ "\"message_id_types\": [\"ulong\", \"uuid\", \"binary\", \"string\"]"

    assert {:ok, %{queues: %{"messages" => %{messages: 0}}}} =
             GravitonMQ.amqp10_status(context.broker)
  end

  defp peer!(url, mode) do
    {output, status} =
      System.cmd(
        System.fetch_env!("GRAVITON_PROTON_PYTHON"),
        [Path.expand("../support/amqp10_message_peer.py", __DIR__), url, mode],
        stderr_to_stdout: true
      )

    assert status == 0, output
    assert output =~ "PASS "
    output
  end
end
