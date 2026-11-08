defmodule GravitonMQ.Storage.WAL do
  @moduledoc """
  Serialized local-Linux write-ahead log with explicit durability boundaries.

  `start_link/1` requires an absolute `:path` with an existing parent and a
  stable binary `:stream_id`. It creates only the leaf directory. A kernel
  abstract Unix socket locks that directory's device/inode across BEAMs and
  path aliases until the owner dies. Other filesystem/platform profiles are
  unsupported. Do not rename or replace files in an open WAL directory.

  `:mode` selects `:events` (the default Core.Storage implementation) or
  `:opaque` (binary type/payload records). APIs cannot cross modes. Positions
  start at one, are inclusive and ordered within their stream only. Append
  preserves order but does not establish durability or atomicity for a batch.
  A complete unacknowledged prefix may survive a crash.

  Synchronization flushes the WAL, writes and flushes a checksummed checkpoint,
  replaces it atomically and synchronizes its directory. Any failed/unknown
  write or synchronization poisons the writer until validated reopen. Recovery
  checks every record. Durable bytes must be intact; an incomplete or invalid
  tail starting at or beyond the last confirmed boundary was never acknowledged
  and is truncated. Surviving complete tail records become durable through a
  fresh synchronization before open succeeds. A log without a checkpoint that
  holds at most a (partial) header for this stream never acknowledged anything
  and is reinitialized; any other log without a checkpoint is an error.

  `compact/2` and `compact_records/2` replace the whole durable log with
  caller-supplied base items. They require every appended record to be durable
  and run serialized with append/sync. The base is written to a new generation
  file (`events.<n>.wal`, header version 2 with its base position and size),
  flushed with its directory, and committed by atomically replacing the
  checkpoint, which names the generation. A crash before that rename recovers
  the old generation, after it the new one. Base items take fresh positions
  after the current position, so positions and durable-through references never
  go backwards. Success is reported only after the new checkpoint's directory
  is synchronized; the old generation file is then removed, and stale
  generation files are removed after every successful open. A never-compacted
  log keeps the original file name and version 1 header/checkpoint formats.
  `usage/1` reports the current log, base and limit sizes in bytes.

  Record size (default 1 MiB, maximum 64 MiB) includes framing. Log size
  defaults to 1 GiB. Append batches are bounded to 1024 records and 64 MiB.
  Fold reads one record at a time and runs the reducer in the serialized owner;
  reducers must not call this owner recursively. `close/1` does not sync.

  An optional `:fault_injector` function `(stage, detail)` supports deterministic
  I/O qualification. Return `:ok`, `{:error, reason}`, `{:unknown, reason}`,
  or `{:partial, byte_count}` for writes. Injection never converts a failed
  filesystem operation into success. It is transient and never stored.
  """

  use GenServer
  @behaviour GravitonMQ.Core.Storage

  alias GravitonMQ.Core.CommitRef
  alias GravitonMQ.Storage.WAL.Codec

  @max_record 67_108_864
  @max_counter 18_446_744_073_709_551_615
  @options [:path, :stream_id, :mode, :max_record_bytes, :max_log_bytes, :fault_injector]

  def start_link(options), do: GenServer.start_link(__MODULE__, options)
  def close(owner), do: GenServer.stop(owner, :normal, :infinity)
  @impl GravitonMQ.Core.Storage
  def append(owner, events), do: GenServer.call(owner, {:append, :events, events}, :infinity)

  def append_records(owner, records),
    do: GenServer.call(owner, {:append, :opaque, records}, :infinity)

  @impl GravitonMQ.Core.Storage
  def sync(owner, reference), do: GenServer.call(owner, {:sync, reference}, :infinity)
  @impl GravitonMQ.Core.Storage
  def durable_through(owner), do: GenServer.call(owner, :durable_through, :infinity)
  @impl GravitonMQ.Core.Storage
  def fold(owner, cursor, acc, reducer),
    do: GenServer.call(owner, {:fold, :events, cursor, acc, reducer}, :infinity)

  def fold_records(owner, cursor, acc, reducer),
    do: GenServer.call(owner, {:fold, :opaque, cursor, acc, reducer}, :infinity)

  def compact(owner, events), do: GenServer.call(owner, {:compact, :events, events}, :infinity)

  def compact_records(owner, records),
    do: GenServer.call(owner, {:compact, :opaque, records}, :infinity)

  def usage(owner), do: GenServer.call(owner, :usage, :infinity)

  @impl GenServer
  def init(options) do
    with {:ok, state} <- options(options),
         :ok <- platform(),
         :ok <- directory(state.path),
         {:ok, lock} <- lock(state.path) do
      state = Map.put(state, :lock, lock)

      case open(state) do
        {:ok, state} ->
          {:ok, state}

        {:error, reason} ->
          :socket.close(lock)
          {:stop, reason}
      end
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl GenServer
  def handle_call(_request, _from, %{poison: reason} = state) when not is_nil(reason),
    do: {:reply, {:error, {:poisoned, reason}}, state}

  def handle_call({:append, mode, _items}, _from, %{mode: actual} = state) when mode != actual,
    do: {:reply, {:error, :stream_mode}, state}

  def handle_call({:append, _mode, items}, _from, state) do
    with {:ok, size, count} <- measure_batch(items, state),
         :ok <- capacity(size, count, state) do
      case write_batch(items, state, :append_write) do
        {:ok, state} -> {:reply, {:ok, reference(state, state.position)}, state}
        {:error, reason} -> {:reply, {:error, reason}, %{state | poison: reason}}
      end
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:sync, ref}, _from, state) do
    if valid_reference?(ref, state) and ref.position > 0 do
      case synchronize(state) do
        {:ok, state} -> {:reply, {:ok, reference(state, state.durable)}, state}
        {:error, reason} -> {:reply, {:error, reason}, %{state | poison: reason}}
      end
    else
      {:reply, {:error, :invalid_reference}, state}
    end
  end

  def handle_call(:durable_through, _from, state) do
    ref = if state.durable == 0, do: :none, else: reference(state, state.durable)
    {:reply, {:ok, ref}, state}
  end

  def handle_call(:usage, _from, state),
    do:
      {:reply, {:ok, %{bytes: state.offset, base_bytes: state.base_end, limit: state.max_log}},
       state}

  def handle_call({:compact, mode, _items}, _from, %{mode: actual} = state) when mode != actual,
    do: {:reply, {:error, :stream_mode}, state}

  def handle_call({:compact, _mode, items}, _from, state) do
    with :ok <- compactable(state),
         {:ok, base_end} <- measure_base(items, state) do
      case compact(items, base_end, state) do
        {:ok, state} -> {:reply, {:ok, reference(state, state.durable)}, state}
        {:error, reason} -> {:reply, {:error, reason}, %{state | poison: reason}}
      end
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:fold, mode, _cursor, _acc, _reducer}, _from, %{mode: actual} = state)
      when mode != actual,
      do: {:reply, {:error, :stream_mode}, state}

  def handle_call({:fold, _mode, cursor, acc, reducer}, _from, state) do
    if (cursor == :origin or valid_reference?(cursor, state)) and
         is_function(reducer, reducer_arity(state.mode)) do
      after_position = if cursor == :origin, do: 0, else: cursor.position
      first = state.base_position + 1
      result = fold_stream(state, state.header_size, first, after_position, acc, reducer)
      {:reply, result, state}
    else
      {:reply, {:error, :invalid_reference}, state}
    end
  end

  @impl GenServer
  def terminate(_reason, state) do
    if Map.has_key?(state, :file), do: :file.close(state.file)
    if Map.has_key?(state, :lock), do: :socket.close(state.lock)
    :ok
  end

  @impl GenServer
  def format_status(status),
    do: Map.merge(status, %{state: :redacted, message: :redacted, reason: :redacted, log: []})

  defp options(opts) when is_list(opts) do
    if Keyword.keyword?(opts) and
         length(Keyword.keys(opts)) == length(Enum.uniq(Keyword.keys(opts))) and
         Enum.all?(Keyword.keys(opts), &(&1 in @options)) do
      path = Keyword.get(opts, :path)
      stream = Keyword.get(opts, :stream_id)
      mode = Keyword.get(opts, :mode, :events)
      record = Keyword.get(opts, :max_record_bytes, 1_048_576)
      log = Keyword.get(opts, :max_log_bytes, 1_073_741_824)
      fault = Keyword.get(opts, :fault_injector)

      if is_binary(path) and Path.type(path) == :absolute and is_binary(stream) and
           byte_size(stream) in 1..1024 and mode in [:events, :opaque] and
           is_integer(record) and record in 64..@max_record and is_integer(log) and
           log >= record and log >= byte_size(stream) + 12 and log <= 9_223_372_036_854_775_807 and
           (is_nil(fault) or is_function(fault, 2)) do
        {:ok,
         %{
           path: path,
           stream: stream,
           mode: mode,
           max_record: record,
           max_log: log,
           fault: fault,
           position: 0,
           offset: 0,
           generation: 0,
           base_position: 0,
           base_end: 0,
           header_size: 0,
           durable: 0,
           durable_offset: 0,
           poison: nil
         }}
      else
        {:error, :invalid_options}
      end
    else
      {:error, :invalid_options}
    end
  end

  defp options(_), do: {:error, :invalid_options}

  defp platform do
    if :os.type() == {:unix, :linux}, do: :ok, else: {:error, :unsupported_platform}
  end

  defp directory(path) do
    case File.mkdir(path) do
      :ok ->
        sync_directory(Path.dirname(path))

      {:error, :eexist} ->
        case File.stat(path) do
          {:ok, %{type: :directory}} -> sync_directory(Path.dirname(path))
          _ -> {:error, :not_directory}
        end

      {:error, reason} ->
        {:error, {:directory, reason}}
    end
  end

  defp lock(path) do
    with {:ok, info} <- File.stat(path),
         {:ok, socket} <- :socket.open(:local, :stream, :default) do
      address =
        <<0, "graviton-wal-v1:", Integer.to_string(info.major_device)::binary, ":",
          Integer.to_string(info.minor_device)::binary, ":",
          Integer.to_string(info.inode)::binary>>

      case :socket.bind(socket, %{family: :local, path: address}) do
        :ok ->
          {:ok, socket}

        {:error, reason} ->
          :socket.close(socket)
          if reason == :eaddrinuse, do: {:error, :already_open}, else: {:error, {:lock, reason}}
      end
    else
      {:error, reason} -> {:error, {:lock, reason}}
    end
  end

  # The checkpoint is the single source of truth: it names the generation file
  # holding the log. Without one, only the legacy generation 0 file can exist.
  defp open(state) do
    case read_checkpoint(state) do
      :missing ->
        open_uncheckpointed(state)

      {:ok, checkpoint} ->
        with {:ok, generation} <- Codec.checkpoint_generation(checkpoint) do
          open_checkpointed(state, generation, checkpoint)
        end

      {:error, _} = error ->
        error
    end
  end

  defp open_uncheckpointed(state) do
    with {:ok, names} <- list_directory(state.path),
         false <-
           Enum.any?(names, &match?({:ok, generation} when generation > 0, generation_file(&1))),
         {:ok, state} <- open_file(state, log_path(state.path, 0)) do
      initialize_uncheckpointed(state)
    else
      true -> {:error, :checkpoint_corrupt}
      {:error, _} = error -> error
    end
  end

  defp open_checkpointed(state, generation, checkpoint) do
    path = log_path(state.path, generation)

    with true <- File.regular?(path),
         {:ok, state} <- open_file(state, path) do
      initialize_existing(state, checkpoint)
    else
      false -> {:error, :durable_prefix_truncated}
      {:error, _} = error -> error
    end
  end

  defp open_file(state, path) do
    case :file.open(String.to_charlist(path), [:raw, :binary, :read, :write]) do
      {:ok, fd} -> {:ok, Map.put(state, :file, fd)}
      {:error, reason} -> {:error, {:open, reason}}
    end
  end

  # Ownership of the descriptor transfers to state on success only.
  defp initialize_uncheckpointed(state) do
    result =
      with {:ok, file_size} <- :file.position(state.file, :eof) do
        cond do
          file_size == 0 ->
            initialize_new(state)

          header_prefix?(state, file_size) ->
            # A crash between writing the header and the first checkpoint left a
            # log that provably never acknowledged anything. Reinitialize it.
            with {:ok, 0} <- :file.position(state.file, 0),
                 :ok <- :file.truncate(state.file) do
              initialize_new(state)
            end

          true ->
            with {:ok, _info} <- read_header(state), do: {:error, :checkpoint_corrupt}
        end
      end

    close_on_error(result, state.file)
  end

  defp header_prefix?(state, file_size) do
    expected = Codec.header(state.stream, state.mode)

    file_size <= byte_size(expected) and
      read_exact(state.file, 0, file_size) == {:ok, binary_part(expected, 0, file_size)}
  end

  defp initialize_new(state) do
    bytes = Codec.header(state.stream, state.mode)
    size = byte_size(bytes)
    state = Map.merge(state, %{header_size: size, base_end: size, offset: size})

    with :ok <- write(state, :header_write, 0, bytes),
         :ok <- sync_directory(state.path),
         {:ok, state} <- synchronize(state) do
      {:ok, state}
    end
  end

  defp initialize_existing(state, checkpoint) do
    result =
      with {:ok, file_size} <- :file.position(state.file, :eof),
           true <- file_size > 0 || {:error, :durable_prefix_truncated},
           {:ok, info} <- read_header(state),
           {:ok, generation, position, offset} <-
             Codec.decode_checkpoint(checkpoint, state.stream, state.mode),
           true <- generation == info.generation || {:error, :checkpoint_corrupt},
           true <-
             offset >= info.base_end and offset <= file_size and file_size <= state.max_log and
               position >= info.base_position,
           state =
             Map.merge(state, %{
               header_size: info.header_size,
               generation: generation,
               base_position: info.base_position,
               base_end: info.base_end,
               durable: position,
               durable_offset: offset
             }),
           {:ok, state, _repaired} <-
             recover(state, info.header_size, info.base_position, file_size),
           # A checkpoint visible after a process crash may be the outcome of an
           # interrupted directory sync. Re-establish the full boundary on every open.
           {:ok, state} <- synchronize(state) do
        remove_stale_generations(state)
        {:ok, state}
      else
        false -> {:error, :durable_prefix_or_log_limit}
        {:error, _} = error -> error
      end

    close_on_error(result, state.file)
  end

  defp close_on_error({:ok, _} = result, _fd), do: result

  defp close_on_error(error, fd) do
    :file.close(fd)
    error
  end

  defp read_header(state) do
    with {:ok, prefix} <- read_exact(state.file, 0, 8),
         {:ok, size} <- Codec.header_size(prefix),
         {:ok, header} <- read_exact(state.file, 0, size) do
      Codec.decode_header(header, state.stream, state.mode)
    end
  end

  defp read_checkpoint(state) do
    path = Path.join(state.path, "durable.checkpoint")

    with {:ok, %{size: size, type: :regular}} when size <= 1060 <- File.stat(path),
         {:ok, bytes} <- File.read(path) do
      {:ok, bytes}
    else
      {:error, :enoent} -> :missing
      _ -> {:error, :checkpoint_corrupt}
    end
  end

  defp list_directory(path) do
    case File.ls(path) do
      {:ok, names} -> {:ok, names}
      {:error, reason} -> {:error, {:directory, reason}}
    end
  end

  defp log_path(path, 0), do: Path.join(path, "events.wal")
  defp log_path(path, generation), do: Path.join(path, "events.#{generation}.wal")

  defp generation_file("events.wal"), do: {:ok, 0}

  defp generation_file("events." <> rest) do
    with [digits, "wal"] <- String.split(rest, ".", parts: 2),
         true <- digits =~ ~r/\A[1-9][0-9]{0,19}\z/ do
      {:ok, String.to_integer(digits)}
    else
      _ -> :error
    end
  end

  defp generation_file(_), do: :error

  # The checkpoint names the only live generation. Others are leftovers of an
  # interrupted or completed compaction and are garbage once open succeeds.
  defp remove_stale_generations(state) do
    with {:ok, names} <- list_directory(state.path) do
      stale =
        for name <- names,
            {:ok, generation} <- [generation_file(name)],
            generation != state.generation,
            do: Path.join(state.path, name)

      Enum.each(stale, &File.rm/1)
      if stale != [], do: sync_directory(state.path)
    end

    :ok
  end

  defp recover(state, offset, position, file_size) when offset == file_size do
    if checkpoint_aligned?(state, position, offset) and position >= state.durable do
      {:ok, %{state | position: position, offset: offset}, false}
    else
      {:error, :checkpoint_alignment}
    end
  end

  defp recover(state, offset, position, file_size) do
    case read_frame(state, offset, position + 1, file_size) do
      {:ok, _item, next} ->
        if checkpoint_aligned?(state, position + 1, next) do
          recover(state, next, position + 1, file_size)
        else
          {:error, :checkpoint_alignment}
        end

      invalid when offset >= state.durable_offset and position >= state.durable ->
        # Bytes beyond the durable checkpoint were never acknowledged. An
        # incomplete or invalid frame there is an interrupted write: truncate.
        if invalid == :incomplete or invalid_tail?(invalid) do
          with {:ok, ^offset} <- :file.position(state.file, offset),
               :ok <- :file.truncate(state.file) do
            {:ok, %{state | position: position, offset: offset}, true}
          end
        else
          invalid
        end

      :incomplete ->
        {:error, :durable_prefix_truncated}

      {:error, _} = error ->
        error
    end
  end

  # Data-validity failures only; read I/O errors are never treated as a tail.
  defp invalid_tail?({:error, reason}),
    do:
      reason in [
        :record_header,
        :record_header_checksum,
        :record_position,
        :record_length,
        :record_checksum_or_payload,
        :event_payload,
        :event_record_type
      ]

  defp invalid_tail?(_), do: false

  defp checkpoint_aligned?(state, position, offset) do
    cond do
      position == state.durable -> offset == state.durable_offset
      position < state.durable -> offset < state.durable_offset
      true -> offset > state.durable_offset
    end
  end

  defp read_frame(state, offset, position, end_offset) do
    available = end_offset - offset

    with {:ok, prefix} <- read_exact(state.file, offset, min(20, available)) do
      cond do
        available < 20 ->
          if Codec.incomplete_prefix?(prefix), do: :incomplete, else: {:error, :record_header}

        true ->
          with {:ok, size} <- Codec.frame_header(prefix, position, state.max_record) do
            if size + 24 > available do
              :incomplete
            else
              with {:ok, bytes} <- read_exact(state.file, offset + 20, size + 4),
                   {:ok, item} <- Codec.decode_frame(bytes, state.mode) do
                {:ok, item, offset + size + 24}
              end
            end
          end
      end
    end
  end

  defp measure_batch(items, state), do: measure_batch(items, state, 0, 0)
  defp measure_batch([], _state, _size, 0), do: {:error, :empty_batch}
  defp measure_batch([], _state, size, count), do: {:ok, size, count}

  defp measure_batch([head | tail], state, size, count) when count < 1024 do
    with {:ok, bytes} <- Codec.measure(head, state.mode, state.max_record),
         true <- size + bytes <= @max_record do
      measure_batch(tail, state, size + bytes, count + 1)
    else
      false -> {:error, :batch_limit}
      error -> error
    end
  end

  defp measure_batch(_, _, _, _), do: {:error, :batch_limit}

  defp capacity(size, count, state) do
    cond do
      state.offset + size > state.max_log -> {:error, :log_limit}
      state.position > @max_counter - count -> {:error, :position_exhausted}
      true -> :ok
    end
  end

  defp write_batch([], state, _stage), do: {:ok, state}

  defp write_batch([item | rest], state, stage) do
    frame = Codec.frame(item, state.mode, state.position + 1)

    with :ok <- write(state, stage, state.offset, frame) do
      state = %{
        state
        | position: state.position + 1,
          offset: state.offset + IO.iodata_length(frame)
      }

      write_batch(rest, state, stage)
    end
  end

  defp compactable(state) do
    cond do
      state.position != state.durable -> {:error, :unsynced_records}
      state.generation >= @max_counter -> {:error, :generation_exhausted}
      true -> :ok
    end
  end

  # Measures the complete base before any file is created. Returns the exact
  # byte offset at which the base ends in the new generation file.
  defp measure_base(items, state), do: measure_base(items, state, 0, 0)
  defp measure_base([], _state, _size, 0), do: {:error, :empty_batch}

  defp measure_base([], state, size, count) do
    base_end = Codec.compacted_header_size(state.stream) + size

    cond do
      base_end > state.max_log -> {:error, :log_limit}
      state.position > @max_counter - count -> {:error, :position_exhausted}
      true -> {:ok, base_end}
    end
  end

  defp measure_base([head | tail], state, size, count) do
    with {:ok, bytes} <- Codec.measure(head, state.mode, state.max_record),
         do: measure_base(tail, state, size + bytes, count + 1)
  end

  defp measure_base(_, _, _, _), do: {:error, :batch_limit}

  # Single commit point: the checkpoint rename inside synchronize/1. Before it,
  # the old checkpoint names the intact old generation; after it, the new
  # checkpoint names a generation file that was already flushed with its
  # directory entry. Any failure is returned for the caller to poison the owner.
  defp compact(items, base_end, state) do
    generation = state.generation + 1
    path = log_path(state.path, generation)
    header = Codec.header(state.stream, state.mode, generation, state.position, base_end)

    with {:ok, fd} <- create_generation(path) do
      next = %{
        state
        | file: fd,
          generation: generation,
          header_size: byte_size(header),
          base_position: state.position,
          base_end: base_end,
          offset: byte_size(header)
      }

      result =
        with :ok <- write(next, :compact_write, 0, header),
             {:ok, next} <- write_batch(items, next, :compact_write),
             true <- next.offset == base_end || {:error, {:io, :compact_write, :size_mismatch}},
             :ok <- operation(next, :compact_sync, generation, fn -> :file.sync(fd) end),
             :ok <-
               operation(next, :compact_directory_sync, generation, fn ->
                 sync_directory(state.path)
               end),
             {:ok, next} <- synchronize(next) do
          {:ok, next}
        end

      case result do
        {:ok, next} ->
          :file.close(state.file)
          retire_generation(state, next)
          {:ok, next}

        {:error, _} = error ->
          :file.close(fd)
          error
      end
    end
  end

  # A leftover file of an uncommitted generation is never named by the
  # checkpoint, so it may be replaced.
  defp create_generation(path) do
    with :ok <- remove_file(path),
         {:ok, fd} <-
           :file.open(String.to_charlist(path), [:raw, :binary, :read, :write, :exclusive]) do
      {:ok, fd}
    else
      {:error, {:io, _, _}} = error -> error
      {:error, reason} -> {:error, {:io, :compact_open, reason}}
    end
  end

  # Runs after the commit point. The new generation is already durable and
  # named by the checkpoint; removal is garbage collection and is retried by
  # every later open, so its failure cannot affect the committed result.
  defp retire_generation(state, next) do
    _ =
      operation(next, :compact_remove, state.generation, fn ->
        remove_file(log_path(state.path, state.generation))
      end)

    :ok
  end

  defp remove_file(path) do
    case File.rm(path) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      {:error, reason} -> {:error, {:io, :compact_open, reason}}
    end
  end

  defp synchronize(state) do
    bytes =
      Codec.checkpoint(state.stream, state.mode, state.generation, state.position, state.offset)

    temp = Path.join(state.path, "durable.checkpoint.next")
    final = Path.join(state.path, "durable.checkpoint")

    with :ok <- operation(state, :wal_sync, state.offset, fn -> :file.sync(state.file) end),
         :ok <- checkpoint_file(state, temp, bytes),
         :ok <-
           operation(state, :checkpoint_rename, state.position, fn -> File.rename(temp, final) end),
         :ok <-
           operation(state, :directory_sync, state.position, fn -> sync_directory(state.path) end) do
      {:ok, %{state | durable: state.position, durable_offset: state.offset}}
    end
  end

  defp checkpoint_file(state, path, bytes) do
    case :file.open(String.to_charlist(path), [:raw, :binary, :write]) do
      {:ok, fd} ->
        try do
          with :ok <- write(%{state | file: fd}, :checkpoint_write, 0, bytes),
               :ok <- operation(state, :checkpoint_sync, state.position, fn -> :file.sync(fd) end) do
            :ok
          end
        after
          :file.close(fd)
        end

      {:error, reason} ->
        {:error, {:io, :checkpoint_open, reason}}
    end
  end

  defp write(state, stage, offset, bytes) do
    case fault(state, stage, %{offset: offset, bytes: IO.iodata_length(bytes)}) do
      :ok ->
        io_result(stage, :file.pwrite(state.file, offset, bytes))

      {:partial, count} when is_integer(count) and count >= 0 ->
        binary = IO.iodata_to_binary(bytes)
        prefix = binary_part(binary, 0, min(count, byte_size(binary)))

        case :file.pwrite(state.file, offset, prefix) do
          :ok -> {:error, {:io, stage, :partial_write}}
          {:error, reason} -> {:error, {:io, stage, reason}}
        end

      {:error, reason} ->
        {:error, {:io, stage, reason}}

      {:unknown, reason} ->
        {:error, {:unknown_outcome, stage, reason}}

      _ ->
        {:error, {:io, stage, :invalid_fault_result}}
    end
  end

  defp operation(state, stage, detail, fun) do
    case fault(state, stage, detail) do
      :ok ->
        io_result(stage, fun.())

      {:error, reason} ->
        {:error, {:io, stage, reason}}

      {:unknown, reason} ->
        case fun.() do
          :ok -> {:error, {:unknown_outcome, stage, reason}}
          {:error, actual} -> {:error, {:io, stage, actual}}
        end

      _ ->
        {:error, {:io, stage, :invalid_fault_result}}
    end
  end

  defp fault(%{fault: nil}, _, _), do: :ok

  defp fault(state, stage, detail) do
    state.fault.(stage, detail)
  catch
    _, _ -> {:error, :fault_callback_failed}
  end

  defp io_result(_, :ok), do: :ok
  defp io_result(stage, {:error, reason}), do: {:error, {:io, stage, reason}}

  defp sync_directory(path) do
    case :file.open(String.to_charlist(path), [:raw, :read, :directory]) do
      {:ok, fd} ->
        try do
          :file.sync(fd)
        after
          :file.close(fd)
        end

      {:error, reason} ->
        {:error, {:directory_sync, reason}}
    end
  end

  defp fold_stream(state, offset, _position, _after, acc, _reducer) when offset == state.offset,
    do: {:ok, acc}

  defp fold_stream(state, offset, position, after_position, acc, reducer) do
    case read_frame(state, offset, position, state.offset) do
      {:ok, _item, next} when position <= after_position ->
        fold_stream(state, next, position + 1, after_position, acc, reducer)

      {:ok, item, next} ->
        ref = reference(state, position)
        result = reduce(state.mode, reducer, ref, item, acc)

        case result do
          {:cont, acc} -> fold_stream(state, next, position + 1, after_position, acc, reducer)
          {:halt, acc} -> {:ok, acc}
          {:error, _} = error -> error
          _ -> {:error, :invalid_reducer_result}
        end

      :incomplete ->
        {:error, :record_truncated}

      {:error, _} = error ->
        error
    end
  end

  defp reduce(mode, reducer, ref, item, acc) do
    try do
      case mode do
        :events ->
          reducer.(ref, item, acc)

        :opaque ->
          {type, payload} = item
          reducer.(ref, type, payload, acc)
      end
    rescue
      _ -> {:error, :reducer_failed}
    catch
      _, _ -> {:error, :reducer_failed}
    end
  end

  defp read_exact(_fd, _offset, 0), do: {:ok, <<>>}

  defp read_exact(fd, offset, size) do
    case :file.pread(fd, offset, size) do
      {:ok, bytes} when byte_size(bytes) == size -> {:ok, bytes}
      {:ok, _} -> {:error, :unexpected_eof}
      :eof -> {:error, :unexpected_eof}
      {:error, reason} -> {:error, {:read, reason}}
    end
  end

  defp valid_reference?(%CommitRef{stream_id: stream, position: pos} = ref, state),
    do:
      map_size(ref) == 3 and stream == state.stream and is_integer(pos) and pos >= 0 and
        pos <= state.position

  defp valid_reference?(_, _), do: false
  defp reference(state, position), do: CommitRef.new(state.stream, position)
  defp reducer_arity(:events), do: 3
  defp reducer_arity(:opaque), do: 4
end
