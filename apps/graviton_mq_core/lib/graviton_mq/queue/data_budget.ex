defmodule GravitonMQ.Queue.DataBudget do
  @moduledoc false

  # Count before retaining or serializing child values. A unit per container or
  # scalar bounds even zero-byte values. Nesting and integer widths are finite.
  @max_integer 18_446_744_073_709_551_615
  @min_integer -9_223_372_036_854_775_808

  def measure(value, limit) do
    case take(value, limit, 0) do
      {:ok, remaining} -> {:ok, limit - remaining}
      error -> error
    end
  end

  defp take(_value, remaining, _depth) when remaining < 1, do: {:error, :limit}
  defp take(_value, _remaining, depth) when depth > 32, do: {:error, :invalid}

  defp take(value, remaining, _depth) when is_binary(value) do
    charge(remaining, 1 + byte_size(value))
  end

  defp take(value, remaining, _depth)
       when is_integer(value) and value >= @min_integer and value <= @max_integer,
       do: charge(remaining, 9)

  defp take(value, remaining, _depth) when is_float(value), do: charge(remaining, 9)

  defp take(value, remaining, _depth) when is_boolean(value) or is_nil(value),
    do: {:ok, remaining - 1}

  defp take(value, remaining, depth) when is_list(value),
    do: list(value, remaining - 1, depth + 1)

  defp take(value, remaining, depth) when is_map(value) and not is_struct(value) do
    Enum.reduce_while(value, {:ok, remaining - 1}, fn
      {key, item}, {:ok, remaining} when is_binary(key) ->
        with {:ok, remaining} <- take(key, remaining, depth + 1),
             {:ok, remaining} <- take(item, remaining, depth + 1) do
          {:cont, {:ok, remaining}}
        else
          error -> {:halt, error}
        end

      _, _ ->
        {:halt, {:error, :invalid}}
    end)
  end

  defp take(_value, _remaining, _depth), do: {:error, :invalid}
  defp list([], remaining, _depth), do: {:ok, remaining}

  defp list([head | tail], remaining, depth) do
    with {:ok, remaining} <- take(head, remaining, depth) do
      list(tail, remaining, depth)
    end
  end

  defp list(_, _, _), do: {:error, :invalid}
  defp charge(remaining, cost) when cost <= remaining, do: {:ok, remaining - cost}
  defp charge(_, _), do: {:error, :limit}
end
