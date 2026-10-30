defmodule GravitonMQ.AMQP10.SessionState do
  @moduledoc """
  Pure data for one AMQP 1.0 session endpoint.

  Links remain data owned by the Session. The canonical map uses
  `{link_name, local_role}` identities, while separate handle indexes reflect
  the peer's independent handle allocation.
  """

  @enforce_keys [:id]
  defstruct [
    :id,
    :local_channel,
    :remote_channel,
    :begin_operation,
    :last_now_ms,
    :last_remote_delivery_id,
    incarnation: 0,
    phase: :idle,
    options: %{},
    remote_handle_max: 4_294_967_295,
    next_outgoing_id: 0,
    next_incoming_id: nil,
    remote_next_incoming_id: nil,
    remote_incoming_window: 0,
    remote_outgoing_window: 0,
    outgoing_window: 0,
    incoming_window: 0,
    next_delivery_id: 0,
    next_token: 0,
    next_receipt: 0,
    operations: %{},
    deadlines: %{},
    timer_effects: [],
    writes: %{},
    control_queue: {[], []},
    pending_flows: %{},
    outgoing_by_id: %{},
    incoming_by_id: %{},
    incoming_token_to_id: %{},
    send_queue: {[], []},
    outgoing_bytes: 0,
    incoming_bytes: 0,
    incoming_reserved_bytes: 0,
    incoming_reserved_slots: 0,
    links_by_identity: %{},
    local_handle_to_link: %{},
    remote_handle_to_link: %{}
  ]

  @type id :: GravitonMQ.AMQP10.Types.uint()
  @type build_error ::
          :missing_session_identity
          | {:duplicate_link_identity, GravitonMQ.AMQP10.Link.identity()}
          | {:duplicate_local_handle, GravitonMQ.AMQP10.Types.handle()}
          | {:duplicate_remote_handle, GravitonMQ.AMQP10.Types.handle()}

  @type t :: %__MODULE__{
          id: id(),
          local_channel: GravitonMQ.AMQP10.Types.ushort() | nil,
          remote_channel: GravitonMQ.AMQP10.Types.ushort() | nil,
          links_by_identity: %{
            optional(GravitonMQ.AMQP10.Link.identity()) => GravitonMQ.AMQP10.Link.t()
          },
          local_handle_to_link: %{
            optional(GravitonMQ.AMQP10.Types.handle()) => GravitonMQ.AMQP10.Link.identity()
          },
          remote_handle_to_link: %{
            optional(GravitonMQ.AMQP10.Types.handle()) => GravitonMQ.AMQP10.Link.identity()
          }
        }

  @spec new(keyword()) :: {:ok, t()} | {:error, build_error()}
  def new(options) do
    case Keyword.fetch(options, :id) do
      {:ok, id} ->
        state = %__MODULE__{
          id: id,
          local_channel: Keyword.get(options, :local_channel),
          remote_channel: Keyword.get(options, :remote_channel)
        }

        Enum.reduce_while(Keyword.get(options, :links, []), {:ok, state}, &index_link/2)

      :error ->
        {:error, :missing_session_identity}
    end
  end

  @spec fetch_outgoing_link(t(), GravitonMQ.AMQP10.Types.handle()) ::
          {:ok, GravitonMQ.AMQP10.Link.t()} | :error
  def fetch_outgoing_link(%__MODULE__{} = state, local_handle) do
    fetch_indexed_link(state, state.local_handle_to_link, local_handle)
  end

  @spec fetch_incoming_link(t(), GravitonMQ.AMQP10.Types.handle()) ::
          {:ok, GravitonMQ.AMQP10.Link.t()} | :error
  def fetch_incoming_link(%__MODULE__{} = state, remote_handle) do
    fetch_indexed_link(state, state.remote_handle_to_link, remote_handle)
  end

  @doc "Adds or replaces canonical Link data and atomically updates its two handle indexes."
  @spec put_link(t(), GravitonMQ.AMQP10.Link.t()) :: {:ok, t()} | {:error, build_error()}
  def put_link(%__MODULE__{} = state, %GravitonMQ.AMQP10.Link{} = link) do
    identity = GravitonMQ.AMQP10.Link.identity(link)
    without = remove_link(state, identity)

    case index_link(link, {:ok, without}) do
      {:cont, {:ok, updated}} -> {:ok, updated}
      {:halt, {:error, reason}} -> {:error, reason}
    end
  end

  @doc "Removes one Link and its handle indexes; other Link data is unchanged."
  @spec remove_link(t(), GravitonMQ.AMQP10.Link.identity()) :: t()
  def remove_link(%__MODULE__{} = state, identity) do
    case Map.pop(state.links_by_identity, identity) do
      {nil, _} ->
        state

      {link, links} ->
        %{
          state
          | links_by_identity: links,
            local_handle_to_link: Map.delete(state.local_handle_to_link, link.local_handle),
            remote_handle_to_link: Map.delete(state.remote_handle_to_link, link.remote_handle)
        }
    end
  end

  defp index_link(link, {:ok, state}) do
    identity = GravitonMQ.AMQP10.Link.identity(link)

    with :ok <- available_identity(state.links_by_identity, identity),
         {:ok, local_index} <-
           put_handle(state.local_handle_to_link, link.local_handle, identity, :local),
         {:ok, remote_index} <-
           put_handle(state.remote_handle_to_link, link.remote_handle, identity, :remote) do
      updated = %__MODULE__{
        state
        | links_by_identity: Map.put(state.links_by_identity, identity, link),
          local_handle_to_link: local_index,
          remote_handle_to_link: remote_index
      }

      {:cont, {:ok, updated}}
    else
      {:error, reason} -> {:halt, {:error, reason}}
    end
  end

  defp available_identity(links, identity) do
    if Map.has_key?(links, identity),
      do: {:error, {:duplicate_link_identity, identity}},
      else: :ok
  end

  defp put_handle(index, nil, _identity, _direction), do: {:ok, index}

  defp put_handle(index, handle, identity, direction) do
    if Map.has_key?(index, handle) do
      {:error, {duplicate_handle_error(direction), handle}}
    else
      {:ok, Map.put(index, handle, identity)}
    end
  end

  defp duplicate_handle_error(:local), do: :duplicate_local_handle
  defp duplicate_handle_error(:remote), do: :duplicate_remote_handle

  defp fetch_indexed_link(state, index, handle) do
    with {:ok, identity} <- Map.fetch(index, handle) do
      Map.fetch(state.links_by_identity, identity)
    end
  end
end
