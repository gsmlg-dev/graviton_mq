defmodule GravitonMQ.AMQP10.G5AnnotationsTest do
  use ExUnit.Case, async: false

  @moduletag :proton_interop
  @moduletag timeout: 30_000

  test "Proton mandatory annotations detach before queue admission and optional annotations survive" do
    broker =
      start_supervised!(
        Supervisor.child_spec(
          {GravitonMQ,
           name: nil,
           runtime_supervisor_name: nil,
           amqp10: [
             container_id: "g5-annotations-#{System.unique_integer([:positive])}",
             auth: :anonymous,
             queues: ["orders"],
             listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
           ]},
          id: make_ref()
        )
      )

    assert {:ok, %{listeners: [%{port: port}]}} = GravitonMQ.amqp10_status(broker)

    script = """
    import json, sys
    import proton
    from proton import Message, symbol, ulong
    from proton.reactor import AtLeastOnce
    from proton.utils import BlockingConnection, LinkDetached
    assert proton.VERSION == (0, 40, 0), proton.VERSION
    mode = sys.argv[2]
    connection = BlockingConnection(sys.argv[1], timeout=5, sasl_enabled=False)
    try:
        sender = connection.create_sender('orders', options=AtLeastOnce())
        key = {'symbol': symbol('must-understand'), 'ulong': ulong(7), 'optional': symbol('x-opt-meta')}[mode]
        message = Message(body=b'opaque-body', annotations={key: True})
        if mode == 'optional':
            sender.send(message, timeout=5)
            receiver = connection.create_receiver('orders', credit=1, options=AtLeastOnce())
            delivered = receiver.receive(timeout=5)
            assert bytes(delivered.body) == b'opaque-body'
            assert delivered.annotations == {symbol('x-opt-meta'): True}
            receiver.accept()
            print(json.dumps({'optional_preserved': True}))
        else:
            try:
                sender.send(message, timeout=5)
            except LinkDetached:
                assert sender.link.remote_condition.name == 'amqp:not-implemented', sender.link.remote_condition
                print(json.dumps({'mandatory_detached': mode}))
            else:
                raise AssertionError('mandatory annotation was accepted')
    finally:
        connection.close()
    """

    for mode <- ["symbol", "ulong", "optional"] do
      {output, status} =
        System.cmd(
          System.fetch_env!("GRAVITON_PROTON_PYTHON"),
          ["-c", script, "amqp://127.0.0.1:#{port}", mode],
          stderr_to_stdout: true
        )

      assert status == 0, output
      assert {:ok, %{queues: %{"orders" => %{messages: 0}}}} = GravitonMQ.amqp10_status(broker)
    end
  end
end
