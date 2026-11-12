defmodule GravitonMQ.Runtime.AMQP10.Auth do
  @moduledoc """
  Host authentication policy for AMQP 1.0 SASL on the broker.

  Configure `amqp10: [auth: {module, options}]`. The broker invokes
  `c:authenticate/2` in a temporary supervised task, outside protocol
  transitions, bounded by `max_sasl_jobs` and `auth_timeout`. Credentials are

      %{mechanism: :plain | :anonymous, username, password, authzid, trace,
        hostname, peer: %{ip, port, transport, certificate, certificate_verified}}

  (`nil` where a mechanism has no such field). The callback owns credential
  verification — use a constant-time comparison — and the `authzid` policy.
  The returned principal becomes the first argument of `authorize/3`.
  `{:error, _}` refuses with sasl-outcome `auth`; an invalid return,
  exception or exit refuses with `sys`; a missed deadline or exhausted
  concurrency refuses with `sys-temp`. Callback reasons are never logged,
  reported to the peer or returned.

  The principal is retained by the broker for the connection's lifetime and
  passed to every `authorize/3` call, so it must not contain secrets
  (passwords, tokens or the policy options): return an identity such as
  `{:user, username}`. Until a Connection reports its principal, Link
  admission is denied without calling `authorize/3`; the atom
  `:unauthenticated` is reserved for that state and is always denied.

  The broker waits for the peer's protocol header under every `sasl` mode,
  bounded by `sasl_timeout` (default 5_000 ms); `sasl: :disabled` uses the
  same deadline for that header wait before refusing a SASL header.
  """

  @callback authenticate(credentials :: map(), options :: keyword()) ::
              {:ok, principal :: term()} | {:error, term()}

  @doc false
  @spec invoke(module(), keyword(), map()) :: {:ok, term()} | {:error, :auth | :sys}
  def invoke(module, options, credentials) do
    case module.authenticate(credentials, options) do
      {:ok, principal} -> {:ok, principal}
      {:error, _reason} -> {:error, :auth}
      _invalid -> {:error, :sys}
    end
  catch
    _kind, _reason -> {:error, :sys}
  end

  @doc false
  # Supervises the actual callback so a slot cannot outlive its deadline or
  # its Connection, even if the callback traps ordinary exits.
  @spec watch(pid(), pid(), integer()) :: :ok | true
  def watch(callback, caller, deadline) do
    callback_monitor = Process.monitor(callback)
    caller_monitor = Process.monitor(caller)

    receive do
      {:DOWN, ^callback_monitor, :process, ^callback, _reason} -> :ok
      {:DOWN, ^caller_monitor, :process, ^caller, _reason} -> Process.exit(callback, :kill)
    after
      max(deadline - System.monotonic_time(:millisecond), 0) -> Process.exit(callback, :kill)
    end
  end
end
