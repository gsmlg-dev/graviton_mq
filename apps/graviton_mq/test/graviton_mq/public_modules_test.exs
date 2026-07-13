defmodule GravitonMQ.PublicModulesTest do
  use ExUnit.Case, async: true

  test "the intentionally small public module surface loads" do
    for module <- [GravitonMQ, GravitonMQ.Application, GravitonMQ.Supervisor, GravitonMQ.Config] do
      assert Code.ensure_loaded?(module), "expected #{inspect(module)} to load"
    end
  end

  test "no unimplemented broker operations are exposed" do
    for {function, arity} <- [publish: 3, consume: 3, declare_queue: 2, ack: 2] do
      refute function_exported?(GravitonMQ, function, arity)
    end
  end
end
