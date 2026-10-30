defmodule GravitonMQ.MQTTHostTest do
  use ExUnit.Case, async: true

  defp broker(options \\ []) do
    start_supervised!(
      {GravitonMQ,
       [
         name: nil,
         runtime_supervisor_name: nil,
         mqtt: Keyword.merge([auth: :anonymous, host_principal: :test_host], options)
       ]},
      id: make_ref()
    )
  end

  defp subscribe(instance, filter, options) do
    assert {:ok, reference} = GravitonMQ.subscribe(instance, filter, options)
    assert_receive {:graviton_mq, owner, {:operation, ^reference, {:ok, subscription}}}, 1_000
    {owner, subscription}
  end

  defp publish(instance, topic, payload, options) do
    assert {:ok, reference} = GravitonMQ.publish(instance, topic, payload, options)

    assert_receive {:graviton_mq, _owner, {:operation, ^reference, {:ok, :accepted_in_memory}}},
                   1_000
  end

  test "host routing has explicit admission, demand, acknowledgement and cancellation" do
    instance = broker()
    {owner, subscription} = subscribe(instance, "sensors/+", qos: 1)
    publish(instance, "sensors/one", "one", qos: 1)
    refute_receive {:graviton_mq, ^owner, {:delivery, _, _, _}}, 20
    assert :ok = GravitonMQ.request(instance, subscription, 1)

    assert_receive {:graviton_mq, ^owner,
                    {:delivery, ^subscription, delivery,
                     %{topic: "sensors/one", payload: "one", qos: 1}}}

    assert :ok = GravitonMQ.ack(instance, delivery)
    assert {:error, :not_owner} = GravitonMQ.ack(instance, delivery)
    assert :ok = GravitonMQ.unsubscribe(instance, subscription)
    assert {:error, :not_owner} = GravitonMQ.request(instance, subscription, 1)
  end

  test "two real host brokers isolate retained state and reject cross-instance references" do
    first = broker()
    second = broker()
    publish(first, "state/value", "first", retain: true, qos: 1)
    {first_owner, first_sub} = subscribe(first, "state/#", qos: 1)
    {second_owner, second_sub} = subscribe(second, "state/#", qos: 1)
    assert :ok = GravitonMQ.request(first, first_sub, 1)
    assert :ok = GravitonMQ.request(second, second_sub, 1)

    assert_receive {:graviton_mq, ^first_owner,
                    {:delivery, ^first_sub, delivery, %{payload: "first", retain: true}}}

    refute_receive {:graviton_mq, ^second_owner, {:delivery, _, _, _}}, 20
    assert {:error, :not_owner} = GravitonMQ.request(second, first_sub, 1)
    assert {:error, :not_owner} = GravitonMQ.ack(second, delivery)
    publish(second, "state/value", "second", qos: 1)

    assert_receive {:graviton_mq, ^second_owner,
                    {:delivery, ^second_sub, _, %{payload: "second"}}}
  end

  test "host resources are finite and a publication cannot partially mutate retained state" do
    instance = broker(max_pending_deliveries: 1, max_host_outstanding: 1, max_demand: 1)
    {owner, subscription} = subscribe(instance, "a/#", qos: 1)
    assert {:error, :demand_limit} = GravitonMQ.request(instance, subscription, 2)
    publish(instance, "a/one", "one", qos: 1)
    assert {:ok, reference} = GravitonMQ.publish(instance, "a/two", "two", qos: 1, retain: true)
    assert_receive {:graviton_mq, ^owner, {:operation, ^reference, {:error, _reason}}}
    assert {:ok, %{retained: 0, pending_deliveries: 1}} = GravitonMQ.status(instance)
  end

  test "caller lifetime owns subscriptions and host APIs need an explicit principal" do
    instance = broker()
    parent = self()

    subscriber =
      spawn(fn ->
        {owner, subscription} = subscribe(instance, "a/#", qos: 1)
        send(parent, {:subscribed, owner, subscription})

        receive do
          :stop -> :ok
        end
      end)

    on_exit(fn -> if Process.alive?(subscriber), do: Process.exit(subscriber, :kill) end)
    assert_receive {:subscribed, _owner, subscription}
    assert {:error, :not_owner} = GravitonMQ.request(instance, subscription, 1)
    send(subscriber, :stop)
    monitor = Process.monitor(subscriber)
    assert_receive {:DOWN, ^monitor, :process, ^subscriber, _reason}
    # Status is serialized after the observed subscriber's DOWN processing.
    assert eventually(fn ->
             match?({:ok, %{host_subscriptions: 0}}, GravitonMQ.status(instance))
           end)

    disabled = broker(host_principal: nil)
    assert {:error, :host_access_disabled} = GravitonMQ.publish(disabled, "a", "x")
  end

  defp eventually(predicate, attempts \\ 50)
  defp eventually(_predicate, 0), do: false

  defp eventually(predicate, attempts) do
    if predicate.(),
      do: true,
      else:
        (
          Process.sleep(2)
          eventually(predicate, attempts - 1)
        )
  end
end
