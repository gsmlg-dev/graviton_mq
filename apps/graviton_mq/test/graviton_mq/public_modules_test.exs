defmodule GravitonMQ.PublicModulesTest do
  use ExUnit.Case, async: true

  test "the intentionally small public module surface loads" do
    for module <- [
          GravitonMQ,
          GravitonMQ.Application,
          GravitonMQ.Supervisor,
          GravitonMQ.Config,
          GravitonMQ.Architecture
        ] do
      assert Code.ensure_loaded?(module), "expected #{inspect(module)} to load"
    end
  end

  test "no unimplemented broker operations are exposed" do
    for {function, arity} <- [publish: 3, consume: 3, declare_queue: 2, ack: 2] do
      refute function_exported?(GravitonMQ, function, arity)
    end
  end

  test "the public surface exposes only the common instance lifecycle" do
    assert function_exported?(GravitonMQ, :start_link, 1)
    assert function_exported?(GravitonMQ, :child_spec, 1)
  end
end
