defmodule GravitonMQ.Storage.BoundariesTest do
  use ExUnit.Case, async: true

  @modules [
    GravitonMQ.Storage,
    GravitonMQ.Storage.Memory,
    GravitonMQ.Storage.WAL,
    GravitonMQ.Storage.Record,
    GravitonMQ.Storage.Segment,
    GravitonMQ.Storage.Recovery
  ]

  test "storage boundary modules load" do
    Enum.each(@modules, fn module ->
      assert Code.ensure_loaded?(module), "expected #{inspect(module)} to load"
    end)
  end

  test "unimplemented storage boundaries do not claim the core storage behaviour" do
    for module <- [GravitonMQ.Storage.Memory, GravitonMQ.Storage.WAL] do
      behaviours =
        module.__info__(:attributes) |> Keyword.get_values(:behaviour) |> List.flatten()

      refute GravitonMQ.Core.Storage in behaviours
    end
  end
end
