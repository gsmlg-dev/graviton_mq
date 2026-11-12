defmodule GravitonMQ.Client.AMQP10.Intent do
  @moduledoc false

  # Pure, bounded record of the Sessions and Links the host successfully
  # declared (S3 owner Q1), and of the progress restoring them on a new
  # Connection. No process and no clock. It stores identities and declaration
  # options only: never message bytes, credit, deliveries or credentials.
  # An entry is added only for a declaration the live Endpoint completed with
  # {:ok, _}, so it is bounded by the Endpoint's max_sessions x max_links.

  defstruct sessions: %{}, queue: [], outstanding: 0, active: false, restored: 0, failed: 0

  def new, do: %__MODULE__{}

  def put_session(intent, sid, options),
    do: put(intent, sid, %{options: options, status: :live, links: []})

  def put_link(intent, sid, identity, options) do
    case Map.fetch(intent.sessions, sid) do
      {:ok, session} ->
        links = List.keystore(session.links, identity, 0, {identity, options, :live})
        put(intent, sid, %{session | links: links})

      :error ->
        intent
    end
  end

  def drop_session(intent, sid), do: %{intent | sessions: Map.delete(intent.sessions, sid)}

  def drop_link(intent, sid, identity) do
    case Map.fetch(intent.sessions, sid) do
      {:ok, session} ->
        put(intent, sid, %{session | links: List.keydelete(session.links, identity, 0)})

      :error ->
        intent
    end
  end

  def counts(intent) do
    %{
      sessions: map_size(intent.sessions),
      links: intent.sessions |> Map.values() |> Enum.map(&length(&1.links)) |> Enum.sum()
    }
  end

  def restoring(intent) do
    Enum.reduce(intent.sessions, 0, fn {_sid, session}, total ->
      total + if(session.status == :pending, do: 1, else: 0) +
        Enum.count(session.links, &match?({_, _, :pending}, &1))
    end)
  end

  # After a new Open every entry waits again; Sessions go first, ascending.
  def start(intent) do
    sessions =
      Map.new(intent.sessions, fn {sid, session} ->
        links =
          Enum.map(session.links, fn {identity, options, _} -> {identity, options, :pending} end)

        {sid, %{session | status: :pending, links: links}}
      end)

    queue = sessions |> Map.keys() |> Enum.sort() |> Enum.map(&{:session, &1})

    %{
      intent
      | sessions: sessions,
        queue: queue,
        outstanding: 0,
        active: true,
        restored: 0,
        failed: 0
    }
  end

  # Another loss interrupted restoration; entries stay declared (D13).
  def stop(intent), do: %{intent | queue: [], outstanding: 0, active: false}

  def pending?(intent, sid, identity) do
    case Map.fetch(intent.sessions, sid) do
      {:ok, %{status: :pending}} ->
        true

      {:ok, session} ->
        identity != nil and match?({_, _, :pending}, List.keyfind(session.links, identity, 0))

      :error ->
        false
    end
  end

  def waiting?(intent), do: intent.active and intent.queue != []
  def finished?(intent), do: intent.active and intent.queue == [] and intent.outstanding == 0

  def finish(intent),
    do: {%{restored: intent.restored, failed: intent.failed}, %{intent | active: false}}

  def take(intent, count) when count <= 0, do: {[], intent}

  def take(intent, count) do
    {keys, rest} = Enum.split(intent.queue, count)
    commands = Enum.flat_map(keys, &command(intent, &1))
    {commands, %{intent | queue: rest, outstanding: intent.outstanding + length(commands)}}
  end

  def interrupt(intent), do: %{intent | outstanding: max(intent.outstanding - 1, 0)}

  # Returns the reports `{key, result}` the shell sends as `{:restored, key, result}`.
  def complete(intent, key, result) do
    intent = interrupt(intent)

    case {key, result} do
      {{:session, sid}, {:ok, _}} ->
        case Map.fetch(intent.sessions, sid) do
          {:ok, session} ->
            links = Enum.map(session.links, fn {identity, _, _} -> {:link, sid, identity} end)
            intent = put(intent, sid, %{session | status: :live})

            {%{intent | queue: intent.queue ++ links, restored: intent.restored + 1},
             [{key, result}]}

          :error ->
            {%{intent | restored: intent.restored + 1}, [{key, result}]}
        end

      {{:session, sid}, _failure} ->
        links =
          case Map.fetch(intent.sessions, sid) do
            {:ok, session} -> session.links
            :error -> []
          end

        reports = [
          {key, result}
          | Enum.map(links, fn {identity, _, _} ->
              {{:link, sid, identity}, {:error, :session_not_restored}}
            end)
        ]

        intent = drop_session(intent, sid)
        {%{intent | failed: intent.failed + length(reports)}, reports}

      {{:link, sid, identity}, {:ok, _}} ->
        intent =
          case Map.fetch(intent.sessions, sid) do
            {:ok, session} ->
              links =
                Enum.map(session.links, fn
                  {^identity, options, _} -> {identity, options, :live}
                  link -> link
                end)

              put(intent, sid, %{session | links: links})

            :error ->
              intent
          end

        {%{intent | restored: intent.restored + 1}, [{key, result}]}

      {{:link, sid, identity}, _failure} ->
        intent = drop_link(intent, sid, identity)
        {%{intent | failed: intent.failed + 1}, [{key, result}]}
    end
  end

  defp command(intent, {:session, sid} = key) do
    case Map.fetch(intent.sessions, sid) do
      {:ok, %{status: :pending, options: options}} -> [{key, {:open_session, sid, options}}]
      _ -> []
    end
  end

  defp command(intent, {:link, sid, identity} = key) do
    with {:ok, session} <- Map.fetch(intent.sessions, sid),
         {^identity, options, :pending} <- List.keyfind(session.links, identity, 0) do
      [{key, {:session, sid, {:attach, identity, options}}}]
    else
      _ -> []
    end
  end

  defp put(intent, sid, session), do: %{intent | sessions: Map.put(intent.sessions, sid, session)}
end
