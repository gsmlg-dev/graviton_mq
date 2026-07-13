defmodule GravitonMQ.Core.Storage do
  @moduledoc """
  Behaviour boundary for protocol-independent durable state storage.

  The callbacks describe the direction of the future abstraction. They are
  optional in Milestone 0 so concrete storage namespaces can declare their
  role without exposing non-functional operations.
  """

  @type state :: term()
  @type record :: term()
  @type reason :: term()

  @callback append(state(), record()) :: {:ok, state()} | {:error, reason()}
  @callback sync(state()) :: {:ok, state()} | {:error, reason()}
  @callback recover(state()) :: {:ok, [record()], state()} | {:error, reason()}

  @optional_callbacks append: 2, sync: 1, recover: 1
end
