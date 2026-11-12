defmodule GravitonMQ.Client.AMQP10.Reconnect do
  @moduledoc false

  # Pure reconnect policy of the AMQP 1.0 client shell (S3 §3.2–3.3). The
  # backoff is the MQTT client's formula, without jitter (owner Q3). `cause` is
  # the shell's record of why the Endpoint began a local Close (D1): the
  # Endpoint reports such a close as :normal, :close_timeout or :closed.

  alias GravitonMQ.AMQP10.{Error, Value}

  @transient_connect ~w(econnrefused econnreset econnaborted ehostunreach enetunreach nxdomain timeout etimedout closed)a

  def delay(%{delay: delay, max_delay: maximum}, attempt)
      when is_integer(attempt) and attempt >= 1,
      do: min(maximum, delay * Integer.pow(2, min(attempt - 1, 20)))

  def classify(_reason, :host_close), do: :stop
  def classify(_reason, cause) when cause in [:idle_timeout, :open_timeout], do: :retry
  def classify(_reason, :protocol), do: :stop
  def classify(reason, nil), do: transient(reason)

  defp transient(:closed), do: :retry
  defp transient({:transport, _reason}), do: :retry
  defp transient({:write_failed, {:error, {:encode, _class, _reason}}}), do: :stop
  defp transient({:write_failed, _result}), do: :retry
  defp transient({:connect, reason}) when reason in @transient_connect, do: :retry
  defp transient(reason) when reason in [:idle_timeout, :open_timeout, :sasl_timeout], do: :retry
  defp transient({:sasl, :sys_temp}), do: :retry
  # An unrequested peer Close without error: a broker shutdown or restart (Q4).
  defp transient(:normal), do: :retry

  defp transient(
         {:peer_close, %Error{condition: %Value{type: :symbol, value: "amqp:connection:forced"}}}
       ),
       do: :retry

  defp transient(_reason), do: :stop
end
