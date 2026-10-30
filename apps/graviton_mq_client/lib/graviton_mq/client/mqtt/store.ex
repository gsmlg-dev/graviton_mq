defmodule GravitonMQ.Client.MQTT.Store do
  @moduledoc false
  alias GravitonMQ.Core.CheckpointCodec
  alias __MODULE__.IO, as: LocalIO
  @magic <<"GMQC", 1>>
  @overhead 65
  defstruct [:directory, :lock, :id, :io, sequence: 0, max_bytes: 8_388_608, poisoned: false]

  def open(options) do
    directory = Keyword.get(options, :directory)
    maximum = Keyword.get(options, :max_bytes, 8_388_608)
    io = Keyword.get(options, :io, LocalIO)

    with true <- :os.type() == {:unix, :linux},
         true <- is_binary(directory) and Path.type(directory) == :absolute,
         true <- is_integer(maximum) and maximum in 256..67_108_864,
         :ok <- prepare(directory, io),
         {:ok, lock} <- lock(directory) do
      store = %__MODULE__{directory: directory, lock: lock, io: io, max_bytes: maximum}

      case recover(store) do
        {:ok, store, snapshot} ->
          {:ok, store, snapshot}

        {:error, reason} ->
          close(store)
          {:error, reason}
      end
    else
      false -> {:error, :unsupported_store_options}
      {:error, _} = error -> error
    end
  end

  def commit(%{poisoned: true} = store, _), do: {:error, :poisoned, store}

  def commit(store, snapshot) do
    publish(%{store | sequence: store.sequence + 1}, snapshot)
  end

  def close(%{lock: lock}) do
    :socket.close(lock)
    :ok
  end

  defp prepare(directory, io) do
    case File.mkdir(directory) do
      :ok ->
        io.sync_directory(Path.dirname(directory))

      {:error, :eexist} ->
        if File.dir?(directory),
          do: io.sync_directory(Path.dirname(directory)),
          else: {:error, :not_directory}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp lock(directory) do
    with {:ok, stat} <- File.stat(directory),
         {:ok, socket} <- :socket.open(:local, :stream, :default) do
      # Shared filesystem-profile address also excludes a broker WAL owner.
      # This is an OS lock convention, with no storage/runtime dependency.
      name =
        <<0, "graviton-wal-v1:", Integer.to_string(stat.major_device)::binary, ":",
          Integer.to_string(stat.minor_device)::binary, ":",
          Integer.to_string(stat.inode)::binary>>

      case :socket.bind(socket, %{family: :local, path: name}) do
        :ok ->
          {:ok, socket}

        {:error, reason} ->
          :socket.close(socket)
          {:error, if(reason == :eaddrinuse, do: :locked, else: reason)}
      end
    end
  end

  defp recover(store) do
    path = Path.join(store.directory, "checkpoint")

    case File.stat(path) do
      {:error, :enoent} ->
        store = %{store | id: :crypto.strong_rand_bytes(16)}

        case publish(store, nil) do
          {:ok, store} -> {:ok, store, nil}
          {:error, reason, _} -> {:error, reason}
        end

      {:ok, %{type: :regular, size: size}} when size <= store.max_bytes + @overhead ->
        with {:ok, wire} <- read_bounded(path, store.max_bytes + @overhead),
             <<@magic, id::binary-size(16), sequence::64, length::32, rest::binary>> <- wire,
             <<payload::binary-size(length), checksum::binary-size(32)>> <- rest,
             true <-
               :crypto.hash(:sha256, binary_part(wire, 0, byte_size(wire) - 32)) == checksum,
             {:ok, snapshot} <- CheckpointCodec.decode(payload, max_bytes: store.max_bytes) do
          # A prior rename/directory-sync completion can have been uncertain.
          # Re-establish the full boundary before exposing any recovered host
          # event, including a terminal success from the persisted outbox.
          case publish(%{store | id: id, sequence: sequence}, snapshot) do
            {:ok, synchronized} -> {:ok, synchronized, snapshot}
            {:error, reason, _poisoned} -> {:error, reason}
          end
        else
          _ -> {:error, :corrupt_checkpoint}
        end

      {:ok, _} ->
        {:error, :corrupt_checkpoint}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp read_bounded(path, maximum) do
    with {:ok, file} <- :file.open(String.to_charlist(path), [:raw, :binary, :read]) do
      result = :file.read(file, maximum + 1)
      closed = :file.close(file)

      case {result, closed} do
        {{:ok, bytes}, :ok} when byte_size(bytes) <= maximum -> {:ok, bytes}
        _ -> {:error, :corrupt_checkpoint}
      end
    end
  end

  defp publish(store, snapshot) do
    with {:ok, payload} <- CheckpointCodec.encode(snapshot, max_bytes: store.max_bytes) do
      prefix =
        <<@magic, store.id::binary-size(16), store.sequence::64, byte_size(payload)::32,
          payload::binary>>

      wire = prefix <> :crypto.hash(:sha256, prefix)
      temporary = Path.join(store.directory, ".checkpoint.pending")
      destination = Path.join(store.directory, "checkpoint")
      # Only the inode owner removes its own unpublished temporary file.
      case File.rm(temporary) do
        result when result == :ok or result == {:error, :enoent} ->
          write_checkpoint(store, temporary, destination, wire)

        {:error, reason} ->
          failure(store, :failed, reason)
      end
    else
      {:error, reason} -> failure(store, :failed, reason)
    end
  end

  defp write_checkpoint(store, temporary, destination, wire) do
    case store.io.open(temporary) do
      {:ok, file} ->
        result =
          with :ok <- File.chmod(temporary, 0o600),
               :ok <- store.io.write(file, wire),
               :ok <- store.io.sync(file),
               do: :ok

        closed = store.io.close(file)

        case {result, closed} do
          {:ok, :ok} ->
            with :ok <- store.io.rename(temporary, destination),
                 :ok <- store.io.sync_directory(store.directory) do
              {:ok, store}
            else
              {:error, reason} -> failure(store, :unknown, reason)
            end

          {{:error, reason}, _} ->
            failure(store, :failed, reason)

          {_, {:error, reason}} ->
            failure(store, :failed, reason)
        end

      {:error, reason} ->
        failure(store, :failed, reason)
    end
  end

  defp failure(store, class, reason), do: {:error, {class, reason}, %{store | poisoned: true}}
end
