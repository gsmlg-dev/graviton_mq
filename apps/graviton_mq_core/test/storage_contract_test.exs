defmodule GravitonMQ.Core.StorageContractTest do
  use ExUnit.Case, async: true

  test "append, sync-through, durable-through, and streaming fold are mandatory" do
    callbacks = GravitonMQ.Core.Storage.behaviour_info(:callbacks)

    assert MapSet.new(callbacks) ==
             MapSet.new(append: 2, sync: 2, durable_through: 1, fold: 4)

    assert [] == GravitonMQ.Core.Storage.behaviour_info(:optional_callbacks)
  end
end
