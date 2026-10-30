alias GravitonMQ.MQTTClient, as: Client

for app <- [:graviton_mq, :graviton_mq_runtime, :graviton_mq_storage] do
  nil = Application.spec(app, :vsn)
end

if port = System.get_env("GRAVITON_MQTT_TEST_PORT") do
  directory = Path.join(File.cwd!(), "installed-client-durability")

  options = [
    host: "127.0.0.1",
    port: String.to_integer(port),
    client_id: "archive-durable-client",
    observer: self(),
    persistence: [directory: directory]
  ]

  start = fn ->
    {:ok, supervisor} = Supervisor.start_link([{Client, options}], strategy: :one_for_one)
    [{_, client, _, _}] = Supervisor.which_children(supervisor)
    {supervisor, client}
  end

  await_connected = fn client ->
    receive do
      {:graviton_mq, ^client, {:connected, _}} -> :ok
    after
      5_000 -> raise "installed persistent client did not connect"
    end
  end

  await_result = fn client, operation ->
    receive do
      {:graviton_mq, ^client, {:operation, ^operation, {:ok, result}}} ->
        result

      {:graviton_mq, ^client, {:operation, ^operation, result}} ->
        raise "installed persistent operation failed: #{inspect(result)}"
    after
      5_000 -> raise "installed persistent client operation did not complete"
    end
  end

  {host, client} = start.()
  :ok = await_connected.(client)
  {:ok, operation} = Client.subscribe(client, "installed/durable", qos: 2)
  [2] = await_result.(client, operation)
  :ok = Client.request(client, 1)
  {:ok, operation} = Client.publish(client, "installed/durable", "owned", qos: 2)
  :pubcomp = await_result.(client, operation)

  receive do
    {:graviton_mq, ^client, {:delivery, _, %{payload: "owned", qos: 2}}} -> :ok
  after
    5_000 -> raise "installed client did not receive the independent broker's publication"
  end

  :ok = Supervisor.stop(host)
  {host, restored} = start.()

  delivery =
    receive do
      {:graviton_mq, ^restored,
       {:recovered_delivery, delivery, %{payload: "owned", qos: 2}, :await_host}} ->
        delivery
    after
      5_000 -> raise "installed client lost its durable incoming ownership"
    end

  :ok = await_connected.(restored)
  :ok = Client.ack(restored, delivery)

  wait = fn wait, remaining ->
    cond do
      Client.status(restored).deliveries == 0 ->
        :ok

      remaining == 0 ->
        raise "installed client did not finish recovered QoS2"

      true ->
        Process.sleep(10)
        wait.(wait, remaining - 1)
    end
  end

  :ok = wait.(wait, 500)
  :ok = Supervisor.stop(host)
  IO.puts("Client-only installed persistence/host-owned QoS2 recovery against Mosquitto PASS")
else
  IO.puts("Client-only installed persistence exchange NOT RUN (set GRAVITON_MQTT_TEST_PORT)")
end
