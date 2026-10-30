defmodule GravitonMQ.Runtime.AMQP10.Store do
  @moduledoc false

  alias GravitonMQ.Storage.WAL

  def start_link(initializer) do
    config = initializer.()

    WAL.start_link(
      path: config.directory,
      stream_id: "amqp-broker-store/v1:" <> config.stream_id,
      mode: :opaque,
      max_record_bytes: 1024,
      max_log_bytes: 4096
    )
  end

  def child_spec(config),
    do: %{id: __MODULE__, start: {__MODULE__, :start_link, [fn -> config end]}}
end
