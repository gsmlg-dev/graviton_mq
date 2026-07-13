defmodule GravitonMQ.Application do
  @moduledoc """
  Public OTP Application callback and composition root for GravitonMQ.

  Lower-level umbrella applications remain library-style applications and do
  not own top-level supervisors.
  """

  use Application

  @impl true
  def start(_type, _arguments) do
    GravitonMQ.Supervisor.start_link()
  end
end
