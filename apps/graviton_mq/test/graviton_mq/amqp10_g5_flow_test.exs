defmodule GravitonMQ.AMQP10.G5FlowTest do
  use ExUnit.Case, async: false

  @moduletag :proton_interop
  @moduletag timeout: 30_000

  test "Proton drain consumes queued fragmented messages before returning unused credit" do
    broker =
      start_supervised!(
        Supervisor.child_spec(
          {GravitonMQ,
           name: nil,
           runtime_supervisor_name: nil,
           amqp10: [
             container_id: "g5-drain-#{System.unique_integer([:positive])}",
             auth: :anonymous,
             queues: ["orders"],
             endpoint_options: [max_frame_size: 512],
             listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
           ]},
          id: make_ref()
        )
      )

    assert {:ok, %{listeners: [%{port: port}]}} = GravitonMQ.amqp10_status(broker)

    script = """
    import hashlib, json, sys
    import proton
    from proton import Message
    from proton.reactor import AtLeastOnce
    from proton.utils import BlockingConnection
    assert proton.VERSION == (0, 40, 0), proton.VERSION
    connection = BlockingConnection(sys.argv[1], timeout=5, sasl_enabled=False)
    try:
        sender = connection.create_sender('orders', options=AtLeastOnce())
        bodies = [b'a' * 4096, b'b' * 3000]
        for body in bodies:
            sender.send(Message(body=body), timeout=5)
        receiver = connection.create_receiver('orders', credit=0, options=AtLeastOnce())
        receiver.link.drain(4)
        received = []
        while True:
            connection.wait(lambda: receiver.fetcher.has_message or not receiver.link.draining(), timeout=5)
            while receiver.fetcher.has_message:
                message = receiver.fetcher.pop()
                received.append(bytes(message.body))
                receiver.accept()
            if not receiver.link.draining():
                break
        assert received == bodies, {'received': [(len(body), hashlib.sha256(body).hexdigest()) for body in received],
                                    'expected': [(len(body), hashlib.sha256(body).hexdigest()) for body in bodies]}
        assert receiver.link.credit == 0, receiver.link.credit
        assert receiver.link.drained() == 2
        receiver.link.drain(3)
        connection.wait(lambda: not receiver.link.draining(), timeout=5)
        assert receiver.link.drained() == 3
        assert not receiver.fetcher.has_message
        receiver.link.drain_mode = False
        sender.send(Message(body=b'after-drain'), timeout=5)
        receiver.link.flow(1)
        message = receiver.receive(timeout=5)
        assert bytes(message.body) == b'after-drain'
        receiver.accept()
        print(json.dumps({'queued': 2, 'unused_credit': 2, 'empty_credit': 3, 'fresh_delivery': True}))
    finally:
        connection.close()
    """

    {output, status} =
      System.cmd(
        System.fetch_env!("GRAVITON_PROTON_PYTHON"),
        ["-c", script, "amqp://127.0.0.1:#{port}"],
        stderr_to_stdout: true
      )

    assert status == 0, output
    assert output =~ "\"fresh_delivery\": true"
    assert {:ok, %{queues: %{"orders" => %{messages: 0}}}} = GravitonMQ.amqp10_status(broker)
  end
end
