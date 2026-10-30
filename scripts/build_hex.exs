[staging] = System.argv()
staging = Path.expand(staging)
packages = staging |> Path.join("packages.txt") |> File.read!() |> String.split("\n", trim: true)
artifacts = Path.join(staging, "artifacts")
unpacked = Path.join(staging, "unpacked")
File.mkdir!(artifacts)
File.mkdir!(unpacked)

run! = fn args, directory ->
  {_, status} =
    System.cmd("mix", args,
      cd: directory,
      into: IO.stream(),
      env: [{"MIX_ENV", "prod"}, {"MIX_BUILD_PATH", Path.join(directory, "_build")}]
    )

  if status != 0, do: raise("mix #{Enum.join(args, " ")} failed in #{directory}")
end

for package <- packages do
  archive = Path.join(artifacts, package <> ".tar")
  run!.(["hex.build", "--output", archive], Path.join(staging, package))

  {:ok, files} = :erl_tar.extract(String.to_charlist(archive), [:memory])
  {~c"contents.tar.gz", contents} = List.keyfind(files, ~c"contents.tar.gz", 0)
  target = Path.join(unpacked, package)
  File.mkdir!(target)
  :ok = :erl_tar.extract({:binary, contents}, [:compressed, {:cwd, String.to_charlist(target)}])
end

consumer = Path.join(staging, "consumer")
File.mkdir!(consumer)

deps =
  Enum.map(packages, fn package ->
    {String.to_atom(package), [path: Path.join(unpacked, package), override: true]}
  end)

project =
  quote do
    defmodule GravitonMQ.HexSmoke.MixProject do
      use Mix.Project

      def project do
        [app: :graviton_mq_hex_smoke, version: "0.0.0", deps: unquote(Macro.escape(deps))]
      end
    end
  end

File.write!(Path.join(consumer, "mix.exs"), Macro.to_string(project))
File.mkdir!(Path.join(consumer, "config"))

File.write!(Path.join(consumer, "config/config.exs"), """
import Config
config :graviton_mq, :default_instance,
  mqtt: [auth: :anonymous, host_principal: :archive_host,
    listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]],
  amqp10: [container_id: "archive-standalone", auth: :anonymous, queues: ["archive-amqp"],
    listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]]
""")

run!.(["deps.get"], consumer)
run!.(["compile", "--warnings-as-errors"], consumer)

version = Path.expand("../VERSION", __DIR__) |> File.read!() |> String.trim()

broker_smoke =
  quote do
    fn instance ->
      {:ok, %{owner: owner, profile: :memory, listeners: [%{port: port}]}} =
        GravitonMQ.status(instance)

      {:ok, operation} = GravitonMQ.subscribe(instance, "graviton/archive/#", qos: 1)

      subscription =
        receive do
          {:graviton_mq, ^owner, {:operation, ^operation, {:ok, subscription}}} -> subscription
        after
          5_000 -> raise "archive broker host subscription did not complete"
        end

      :ok = GravitonMQ.request(instance, subscription, 1)
      {:ok, publication} = GravitonMQ.publish(instance, "graviton/archive/host", "host", qos: 1)

      receive do
        {:graviton_mq, ^owner, {:operation, ^publication, {:ok, :accepted_in_memory}}} -> :ok
      after
        5_000 -> raise "archive broker host publication did not complete"
      end

      receive do
        {:graviton_mq, ^owner, {:delivery, ^subscription, delivery, %{payload: "host"}}} ->
          :ok = GravitonMQ.ack(instance, delivery)
      after
        5_000 -> raise "archive broker host delivery did not arrive"
      end

      if binary = System.get_env("GRAVITON_MOSQUITTO_BIN") do
        {version, 0} = System.cmd(binary, ["-h"])
        true = String.contains?(version, "mosquitto version 2.1.2")
        publisher = Path.join(Path.dirname(binary), "mosquitto_pub")
        subscriber = Path.join(Path.dirname(binary), "mosquitto_sub")
        arguments = ["-V", "mqttv5", "-h", "127.0.0.1", "-p", to_string(port)]
        :ok = GravitonMQ.request(instance, subscription, 1)

        {_, 0} =
          System.cmd(
            "timeout",
            ["5s", publisher] ++
              arguments ++
              ["-t", "graviton/archive/ingress", "-m", "independent", "-q", "1"],
            stderr_to_stdout: true
          )

        receive do
          {:graviton_mq, ^owner,
           {:delivery, ^subscription, delivery,
            %{topic: "graviton/archive/ingress", payload: "independent", qos: 1}}} ->
            :ok = GravitonMQ.ack(instance, delivery)
        after
          5_000 -> raise "archive broker independent publisher did not reach host"
        end

        {:ok, retained} =
          GravitonMQ.publish(instance, "graviton/archive/egress", "retained",
            qos: 1,
            retain: true
          )

        receive do
          {:graviton_mq, ^owner, {:operation, ^retained, {:ok, :accepted_in_memory}}} -> :ok
        after
          5_000 -> raise "archive broker retained publication did not complete"
        end

        {output, 0} =
          System.cmd(
            "timeout",
            ["5s", subscriber] ++
              arguments ++
              [
                "-t",
                "graviton/archive/egress",
                "-q",
                "1",
                "-C",
                "1",
                "-W",
                "3",
                "-F",
                "%t|%q|%r|%p"
              ],
            stderr_to_stdout: true
          )

        true = String.contains?(output, "graviton/archive/egress|1|1|retained")
        IO.puts("Broker archive host/network paths and independent-peer exchange PASS")
      else
        IO.puts(
          "Broker archive host path PASS; independent-peer exchange NOT RUN (set GRAVITON_MOSQUITTO_BIN)"
        )
      end

      :ok
    end
  end

amqp_broker_smoke =
  quote do
    fn instance ->
      {:ok,
       %{
         profile: :memory,
         listeners: [%{port: port}],
         queues: %{"archive-amqp" => %{messages: 0}}
       }} =
        GravitonMQ.amqp10_status(instance)

      if python = System.get_env("GRAVITON_PROTON_PYTHON") do
        code = """
        import hashlib, json, sys, proton
        from proton import Message
        from proton.reactor import AtLeastOnce
        from proton.utils import BlockingConnection
        assert proton.VERSION == (0, 40, 0)
        body = bytes(range(256)) * 256
        connection = BlockingConnection(sys.argv[1], timeout=5, sasl_enabled=False)
        try:
            sender = connection.create_sender('archive-amqp', options=AtLeastOnce())
            sender.send(Message(body=body), timeout=5)
            receiver = connection.create_receiver('archive-amqp', options=AtLeastOnce(), credit=1)
            received = receiver.receive(timeout=5)
            assert bytes(received.body) == body
            receiver.accept()
            print(json.dumps({'bytes': len(body), 'sha256': hashlib.sha256(body).hexdigest()}))
        finally:
            connection.close()
        """

        {output, 0} =
          System.cmd(python, ["-c", code, "amqp://127.0.0.1:#{port}"], stderr_to_stdout: true)

        true = String.contains?(output, "\"bytes\": 65536")
        IO.puts("Broker AMQP archive installation and independent Proton exchange PASS")
      else
        IO.puts("Broker AMQP archive exchange NOT RUN (set GRAVITON_PROTON_PYTHON)")
      end

      :ok
    end
  end

smoke =
  quote do
    for package <- unquote(packages) do
      app = String.to_existing_atom(package)
      unquote(version) = app |> Application.spec(:vsn) |> to_string()
    end

    true = is_pid(Process.whereis(GravitonMQ.Supervisor))
    check_broker = unquote(broker_smoke)
    :ok = check_broker.(GravitonMQ.Supervisor)
    check_amqp = unquote(amqp_broker_smoke)
    :ok = check_amqp.(GravitonMQ.Supervisor)
    true = Code.ensure_loaded?(GravitonMQ.MQTT.Codec.Packet)

    {:ok, %GravitonMQ.MQTT.Packet.Connect{version: :v5, client_id: "c"}, <<>>} =
      GravitonMQ.MQTT.Codec.Packet.decode(<<0x10, 0x0E, 0, 4, "MQTT", 5, 2, 0, 60, 0, 0, 1, "c">>)

    for app <- [
          :graviton_mq_core,
          :graviton_mq_storage,
          :graviton_mq_amqp10,
          :graviton_mq_mqtt,
          :graviton_mq_client,
          :graviton_mq_runtime
        ] do
      [] = Application.spec(app, :mod)
    end
  end

run!.(["run", "--no-compile", "-e", Macro.to_string(smoke)], consumer)
durable_broker_script = Path.expand("interop/installed_broker_durable.exs", __DIR__)
run!.(["run", "--no-compile", durable_broker_script], consumer)

# A separate host project proves runtime: false prevents the product's default
# instance from starting, while the host can supervise the same public API.
embedding_consumer = Path.join(staging, "embedding-consumer")
File.mkdir!(embedding_consumer)

embedding_deps =
  deps
  |> Enum.reject(fn {app, _} -> app == :graviton_mq_client end)
  |> Enum.map(fn
    {:graviton_mq, options} -> {:graviton_mq, Keyword.put(options, :runtime, false)}
    dependency -> dependency
  end)

embedding_project =
  quote do
    defmodule GravitonMQ.HexEmbeddingSmoke.MixProject do
      use Mix.Project

      def project do
        [
          app: :graviton_mq_hex_embedding_smoke,
          version: "0.0.0",
          deps: unquote(Macro.escape(embedding_deps))
        ]
      end
    end
  end

File.write!(Path.join(embedding_consumer, "mix.exs"), Macro.to_string(embedding_project))
run!.(["deps.get"], embedding_consumer)
run!.(["compile", "--warnings-as-errors"], embedding_consumer)

embedding_smoke =
  quote do
    nil = Process.whereis(GravitonMQ.Supervisor)
    nil = Process.whereis(GravitonMQ.Runtime.Supervisor)

    false =
      Enum.any?(Application.started_applications(), fn {app, _, _} -> app == :graviton_mq end)

    {:ok, host} =
      Supervisor.start_link(
        [
          {GravitonMQ,
           name: :hex_embedded_graviton_mq,
           runtime_supervisor_name: :hex_embedded_graviton_mq_runtime}
        ],
        strategy: :one_for_one
      )

    embedded = Process.whereis(:hex_embedded_graviton_mq)
    runtime = Process.whereis(:hex_embedded_graviton_mq_runtime)
    true = is_pid(embedded)
    true = is_pid(runtime)
    [] = Supervisor.which_children(runtime)
    nil = Process.whereis(GravitonMQ.Supervisor)

    {:ok, %GravitonMQ.MQTT.Packet.Connect{version: :v5, client_id: "c"}, <<>>} =
      GravitonMQ.MQTT.Codec.Packet.decode(<<0x10, 0x0E, 0, 4, "MQTT", 5, 2, 0, 60, 0, 0, 1, "c">>)

    :ok = Supervisor.stop(host)
    false = Process.alive?(embedded)
    false = Process.alive?(runtime)

    false = Code.ensure_loaded?(GravitonMQ.Client.MQTT)
    nil = Application.spec(:graviton_mq_client, :vsn)

    {:ok, broker_host} =
      Supervisor.start_link(
        [
          {GravitonMQ,
           name: :hex_embedded_mqtt,
           runtime_supervisor_name: :hex_embedded_mqtt_runtime,
           mqtt: [
             auth: :anonymous,
             host_principal: :archive_host,
             listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
           ],
           amqp10: [
             container_id: "archive-embedded",
             auth: :anonymous,
             queues: ["archive-amqp"],
             listeners: [[transport: :tcp, ip: {127, 0, 0, 1}, port: 0]]
           ]}
        ],
        strategy: :one_for_one
      )

    embedded_broker = Process.whereis(:hex_embedded_mqtt)
    check_broker = unquote(broker_smoke)
    :ok = check_broker.(embedded_broker)
    check_amqp = unquote(amqp_broker_smoke)
    :ok = check_amqp.(embedded_broker)
    nil = Process.whereis(GravitonMQ.Supervisor)
    :ok = Supervisor.stop(broker_host)
    false = Process.alive?(embedded_broker)
  end

run!.(["run", "--no-compile", "-e", Macro.to_string(embedding_smoke)], embedding_consumer)
run!.(["run", "--no-compile", durable_broker_script], embedding_consumer)

# The client is installed from its own archive and its actual dependency closure.
# No broker runtime/storage/composition package is available to this consumer.
client_consumer = Path.join(staging, "client-consumer")
File.mkdir!(client_consumer)

client_deps =
  Enum.filter(deps, fn {app, _} ->
    app in [:graviton_mq_core, :graviton_mq_amqp10, :graviton_mq_mqtt, :graviton_mq_client]
  end)

client_project =
  quote do
    defmodule GravitonMQ.HexClientSmoke.MixProject do
      use Mix.Project

      def project,
        do: [
          app: :graviton_mq_hex_client_smoke,
          version: "0.0.0",
          deps: unquote(Macro.escape(client_deps))
        ]
    end
  end

File.write!(Path.join(client_consumer, "mix.exs"), Macro.to_string(client_project))
run!.(["deps.get"], client_consumer)
run!.(["compile", "--warnings-as-errors"], client_consumer)

client_smoke =
  quote do
    for app <- [:graviton_mq, :graviton_mq_runtime, :graviton_mq_storage] do
      nil = Application.spec(app, :vsn)
      false = Enum.any?(Application.started_applications(), fn {name, _, _} -> name == app end)
    end

    false = Code.ensure_loaded?(GravitonMQ.Runtime.Supervisor)
    nil = Process.whereis(GravitonMQ.Supervisor)

    if port = System.get_env("GRAVITON_MQTT_TEST_PORT") do
      {:ok, host} =
        Supervisor.start_link(
          [
            {GravitonMQ.MQTTClient,
             host: "127.0.0.1",
             port: String.to_integer(port),
             client_id: "archive-client",
             observer: self()}
          ],
          strategy: :one_for_one
        )

      [{_, client, _, _}] = Supervisor.which_children(host)

      receive do
        {:graviton_mq, ^client, {:connected, _}} -> :ok
      after
        5_000 -> raise "archive client did not connect to independent peer"
      end

      {:ok, subscription} = GravitonMQ.MQTTClient.subscribe(client, "graviton/archive", qos: 1)

      receive do
        {:graviton_mq, ^client, {:operation, ^subscription, {:ok, [1]}}} -> :ok
      after
        5_000 -> raise "archive client did not receive SUBACK"
      end

      :ok = GravitonMQ.MQTTClient.request(client, 1)

      {:ok, operation} =
        GravitonMQ.MQTTClient.publish(client, "graviton/archive", "archive", qos: 1)

      receive do
        {:graviton_mq, ^client, {:operation, ^operation, {:ok, :puback}}} -> :ok
      after
        5_000 -> raise "archive client did not receive PUBACK"
      end

      receive do
        {:graviton_mq, ^client,
         {:delivery, delivery, %{topic: "graviton/archive", payload: "archive"}}} ->
          :ok = GravitonMQ.MQTTClient.ack(client, delivery)
      after
        5_000 -> raise "archive client did not receive peer publication"
      end

      :ok = Supervisor.stop(host)
      false = Process.alive?(client)
      IO.puts("Client-only archive installation and independent-peer exchange PASS")
    else
      IO.puts(
        "Client-only archive dependency closure PASS; network smoke NOT RUN (set GRAVITON_MQTT_TEST_PORT)"
      )
    end

    if port = System.get_env("ARTEMIS_PORT") do
      alias GravitonMQ.AMQP10.{Outcome, Value}
      alias GravitonMQ.AMQP10Client, as: AMQPClient

      await = fn client, operation ->
        receive do
          {:graviton_mq, ^client, {:operation, ^operation, {:ok, result}}} ->
            result

          {:graviton_mq, ^client, {:operation, ^operation, result}} ->
            raise "archive AMQP operation failed: #{inspect(result)}"
        after
          5_000 -> raise "archive AMQP operation did not finish"
        end
      end

      {:ok, host} =
        Supervisor.start_link(
          [
            {AMQPClient,
             host: System.get_env("ARTEMIS_HOST", "127.0.0.1"),
             port: String.to_integer(port),
             container_id: "archive-amqp-client",
             observer: self()}
          ],
          strategy: :one_for_one
        )

      [{_, client, _, _}] = Supervisor.which_children(host)

      receive do
        {:graviton_mq, ^client, {:connected, _}} -> :ok
      after
        5_000 -> raise "archive AMQP client did not connect to Artemis"
      end

      {:ok, open} = AMQPClient.open_session(client, 1)
      1 = await.(client, open)
      address = Value.string("graviton-archive-amqp-#{System.unique_integer([:positive])}")
      receiver = {Value.string("archive-receiver"), :receiver}
      sender = {Value.string("archive-sender"), :sender}

      {:ok, attach} =
        AMQPClient.attach(client, 1, "archive-receiver", :receiver, address: address)

      ^receiver = await.(client, attach)
      {:ok, attach} = AMQPClient.attach(client, 1, "archive-sender", :sender, address: address)
      ^sender = await.(client, attach)
      {:ok, demand} = AMQPClient.request(client, 1, receiver, 1, 1_048_576)
      1 = await.(client, demand)

      {:ok, raw} =
        GravitonMQ.AMQP10.Codec.Message.encode_sections([{:data, Value.binary("archive-amqp")}])

      {:ok, publish} = AMQPClient.send_message(client, 1, sender, raw)
      %Outcome.Accepted{} = await.(client, publish)

      delivery =
        receive do
          {:graviton_mq, ^client, {:delivery, delivery, 1, ^receiver, received, _}} ->
            {:ok, message, <<>>} = GravitonMQ.AMQP10.Codec.Message.decode(received)
            true = {:data, Value.binary("archive-amqp")} in message.sections
            delivery
        after
          5_000 -> raise "archive AMQP client did not receive Artemis delivery"
        end

      {:ok, settle} = AMQPClient.settle(client, delivery, %Outcome.Accepted{})
      %Outcome.Accepted{} = await.(client, settle)
      {:ok, close} = AMQPClient.close(client)
      :closed = await.(client, close)
      :ok = Supervisor.stop(host)
      false = Process.alive?(client)
      IO.puts("Client-only AMQP archive installation and Artemis exchange PASS")
    else
      IO.puts("Client-only AMQP network smoke NOT RUN (set ARTEMIS_HOST/ARTEMIS_PORT)")
    end
  end

run!.(["run", "--no-compile", "-e", Macro.to_string(client_smoke)], client_consumer)

run!.(
  ["run", "--no-compile", Path.expand("interop/installed_client_durable.exs", __DIR__)],
  client_consumer
)
