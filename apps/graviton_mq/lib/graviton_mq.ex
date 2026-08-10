defmodule GravitonMQ do
  @moduledoc """
  Public namespace for the embeddable GravitonMQ application.

  `start_link/1` and `child_spec/1` are the common standalone and embedded
  lifecycle. Milestone 0 exposes no broker operations: there is no listener,
  publish path, consumer path, queue, or persistence engine.
  """

  @doc """
  Starts one empty GravitonMQ instance supervision tree.

  `:name` and `:runtime_supervisor_name` are exact OTP registration names
  supplied by the caller. They are never converted into atoms.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(options \\ []) do
    GravitonMQ.Supervisor.start_link(options)
  end

  @doc """
  Returns a child specification for host-supervised embedding.
  """
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(options) do
    name = Keyword.get(options, :name, GravitonMQ.Supervisor)

    %{
      id: {__MODULE__, name},
      start: {__MODULE__, :start_link, [options]},
      type: :supervisor
    }
  end
end
