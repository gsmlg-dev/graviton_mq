defmodule GravitonMQ.Application do
  @moduledoc """
  Public OTP Application callback and composition root for GravitonMQ.

  Lower-level umbrella applications remain library-style applications and do
  not own top-level supervisors.
  """

  use Application

  @impl true
  def start(_type, _arguments) do
    options = Application.get_env(:graviton_mq, :default_instance, [])
    GravitonMQ.start_link(options)
  end
end
