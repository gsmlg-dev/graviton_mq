defmodule GravitonMQ.Client.MQTT.Store.IO do
  @moduledoc false
  def open(path), do: :file.open(String.to_charlist(path), [:raw, :binary, :write, :exclusive])
  def write(file, bytes), do: :file.write(file, bytes)
  def sync(file), do: :file.sync(file)
  def close(file), do: :file.close(file)
  def rename(source, destination), do: File.rename(source, destination)

  def sync_directory(path) do
    with {:ok, file} <- :file.open(String.to_charlist(path), [:raw, :read, :directory]) do
      result = :file.sync(file)
      closed = :file.close(file)
      if result == :ok, do: closed, else: result
    end
  end
end
