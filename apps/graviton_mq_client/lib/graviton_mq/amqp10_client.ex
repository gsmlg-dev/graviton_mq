defmodule GravitonMQ.AMQP10Client do
  @moduledoc """
  Independently supervised AMQP 1.0 network client.

  The initial profile uses AMQP without SASL, non-dynamic volatile termini,
  format-zero messages, unsettled senders and receiver-first settlement.
  TCP and verified TLS are available. Reconnect and session resumption are not
  implemented: a new owner starts a new connection without implicit replay.

  Commands return local admission `{:ok, reference}`. The caller then receives
  `{:graviton_mq, client, {:operation, reference, result}}`. Publication completes
  with a peer outcome, never merely a successful socket write. An interrupted
  possible send reports `{:unknown, reason}`;
  a known outcome followed by interrupted settlement retains that outcome.
  No memory outcome promises disk durability or consumer business processing.

  The observer receives connection/session/link events and
  `{:delivery, reference, session_id, link_identity, encoded_message, metadata}`
  in the same `{:graviton_mq, client, event}` envelope. Credit is granted only
  by `request/5`, bounded by both slots and worst-case message bytes. Settle
  each delivery explicitly. The observer's death closes the owned transport.
  Installing or starting this library does not start a broker.
  """

  alias GravitonMQ.AMQP10.Value
  alias GravitonMQ.Client.AMQP10

  @doc false
  def start_link(options) when is_function(options, 0), do: start_link(options.())

  def start_link(options) when is_list(options) do
    if Keyword.keyword?(options),
      do: AMQP10.start_link(Keyword.put_new(options, :observer, self())),
      else: {:error, :invalid_options}
  end

  def start_link(_options), do: {:error, :invalid_options}

  def child_spec(options) do
    options = Keyword.put_new(options, :observer, self())

    %{
      id: Keyword.get(options, :id, {__MODULE__, Keyword.get(options, :container_id)}),
      start: {__MODULE__, :start_link, [fn -> options end]},
      restart: :transient,
      type: :worker
    }
  end

  def open_session(client, identity, options \\ []),
    do: command(client, {:open_session, identity, options})

  def attach(client, session, name, role, options \\ [])

  def attach(client, session, name, role, options)
      when is_binary(name) and role in [:sender, :receiver],
      do: command(client, {:session, session, {:attach, {Value.string(name), role}, options}})

  def attach(_client, _session, _name, _role, _options), do: {:error, :invalid_link_identity}

  @doc "Send authoritative, already encoded format-zero message bytes."
  def send_message(client, session, link_identity, encoded_message, options \\ []),
    do: command(client, {:session, session, {:send, link_identity, encoded_message, options}})

  def request(client, session, link_identity, slots, bytes),
    do: command(client, {:session, session, {:demand, link_identity, slots, bytes}})

  def settle(client, reference, outcome),
    do: GenServer.call(client, {:settle_delivery, reference, outcome})

  def detach(client, session, link_identity),
    do: command(client, {:session, session, {:detach, link_identity}})

  def end_session(client, session), do: command(client, {:end_session, session})
  def close(client), do: command(client, :close)
  def status(client), do: GenServer.call(client, :status)

  defp command(client, command), do: GenServer.call(client, {:command, command})
end
