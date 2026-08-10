defmodule GravitonMQ.Core.BoundariesTest do
  use ExUnit.Case, async: true

  @modules [
    GravitonMQ.Core,
    GravitonMQ.Core.Message,
    GravitonMQ.Core.MessageId,
    GravitonMQ.Core.NodeId,
    GravitonMQ.Core.DeliveryRef,
    GravitonMQ.Core.CommitRef,
    GravitonMQ.Core.Delivery,
    GravitonMQ.Core.Outcome,
    GravitonMQ.Core.Address,
    GravitonMQ.Core.NodeType,
    GravitonMQ.Core.Storage,
    GravitonMQ.Queue.Machine,
    GravitonMQ.Queue.Command,
    GravitonMQ.Queue.Effect,
    GravitonMQ.Queue.Event,
    GravitonMQ.Queue.EventId,
    GravitonMQ.Queue.State
  ]

  test "core boundary modules load" do
    Enum.each(@modules, fn module ->
      assert Code.ensure_loaded?(module), "expected #{inspect(module)} to load"
    end)
  end

  test "the queue transition function is not implemented in Milestone 0" do
    refute function_exported?(GravitonMQ.Queue.Machine, :apply, 2)
  end
end
