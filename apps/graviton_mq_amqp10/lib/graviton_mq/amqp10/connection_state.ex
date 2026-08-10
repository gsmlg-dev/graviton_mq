defmodule GravitonMQ.AMQP10.ConnectionState do
  @moduledoc """
  Data owned by a future AMQP 1.0 connection process.

  Sessions have a connection-local stable identity and separate local and
  remote channel indexes. Milestone 0 defines no connection transitions.
  """

  defstruct [
    :local_container_id,
    :remote_container_id,
    sessions_by_identity: %{},
    local_channel_to_session: %{},
    remote_channel_to_session: %{}
  ]

  @type build_error ::
          {:duplicate_session_identity, GravitonMQ.AMQP10.SessionState.id()}
          | {:duplicate_local_channel, GravitonMQ.AMQP10.Types.ushort()}
          | {:duplicate_remote_channel, GravitonMQ.AMQP10.Types.ushort()}

  @type t :: %__MODULE__{
          local_container_id: GravitonMQ.AMQP10.Value.string_value() | nil,
          remote_container_id: GravitonMQ.AMQP10.Value.string_value() | nil,
          sessions_by_identity: %{
            optional(GravitonMQ.AMQP10.SessionState.id()) => GravitonMQ.AMQP10.SessionState.t()
          },
          local_channel_to_session: %{
            optional(GravitonMQ.AMQP10.Types.ushort()) => GravitonMQ.AMQP10.SessionState.id()
          },
          remote_channel_to_session: %{
            optional(GravitonMQ.AMQP10.Types.ushort()) => GravitonMQ.AMQP10.SessionState.id()
          }
        }

  @spec new(keyword()) :: {:ok, t()} | {:error, build_error()}
  def new(options \\ []) do
    state = %__MODULE__{
      local_container_id: Keyword.get(options, :local_container_id),
      remote_container_id: Keyword.get(options, :remote_container_id)
    }

    Enum.reduce_while(Keyword.get(options, :sessions, []), {:ok, state}, &index_session/2)
  end

  @spec fetch_outgoing_session(t(), GravitonMQ.AMQP10.Types.ushort()) ::
          {:ok, GravitonMQ.AMQP10.SessionState.t()} | :error
  def fetch_outgoing_session(%__MODULE__{} = state, local_channel) do
    fetch_indexed_session(state, state.local_channel_to_session, local_channel)
  end

  @spec fetch_incoming_session(t(), GravitonMQ.AMQP10.Types.ushort()) ::
          {:ok, GravitonMQ.AMQP10.SessionState.t()} | :error
  def fetch_incoming_session(%__MODULE__{} = state, remote_channel) do
    fetch_indexed_session(state, state.remote_channel_to_session, remote_channel)
  end

  defp index_session(session, {:ok, state}) do
    with :ok <- available_identity(state.sessions_by_identity, session.id),
         {:ok, local_index} <-
           put_channel(state.local_channel_to_session, session.local_channel, session.id, :local),
         {:ok, remote_index} <-
           put_channel(
             state.remote_channel_to_session,
             session.remote_channel,
             session.id,
             :remote
           ) do
      updated = %__MODULE__{
        state
        | sessions_by_identity: Map.put(state.sessions_by_identity, session.id, session),
          local_channel_to_session: local_index,
          remote_channel_to_session: remote_index
      }

      {:cont, {:ok, updated}}
    else
      {:error, reason} -> {:halt, {:error, reason}}
    end
  end

  defp available_identity(sessions, id) do
    if Map.has_key?(sessions, id), do: {:error, {:duplicate_session_identity, id}}, else: :ok
  end

  defp put_channel(index, nil, _id, _direction), do: {:ok, index}

  defp put_channel(index, channel, id, direction) do
    if Map.has_key?(index, channel) do
      {:error, {duplicate_channel_error(direction), channel}}
    else
      {:ok, Map.put(index, channel, id)}
    end
  end

  defp duplicate_channel_error(:local), do: :duplicate_local_channel
  defp duplicate_channel_error(:remote), do: :duplicate_remote_channel

  defp fetch_indexed_session(state, index, channel) do
    with {:ok, id} <- Map.fetch(index, channel) do
      Map.fetch(state.sessions_by_identity, id)
    end
  end
end
