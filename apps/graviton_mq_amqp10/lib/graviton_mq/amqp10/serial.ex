defmodule GravitonMQ.AMQP10.Serial do
  @moduledoc "Bounded uint serial arithmetic; ranges are compared without expansion."
  import Bitwise, only: [band: 2]
  @half 0x8000_0000
  @mask 0xFFFF_FFFF

  @spec add(non_neg_integer(), non_neg_integer()) :: non_neg_integer()
  def add(number, increment), do: band(number + increment, @mask)
  @spec distance(non_neg_integer(), non_neg_integer()) :: non_neg_integer()
  def distance(first, last), do: band(last - first, @mask)
  @spec compare(non_neg_integer(), non_neg_integer()) :: :eq | :lt | :gt | :ambiguous
  def compare(first, last) do
    case distance(first, last) do
      0 -> :eq
      @half -> :ambiguous
      distance when distance < @half -> :lt
      _ -> :gt
    end
  end

  @spec span(non_neg_integer(), non_neg_integer()) ::
          {:ok, pos_integer()} | {:error, :ambiguous_range}
  def span(first, last) do
    case distance(first, last) do
      distance when distance < @half -> {:ok, distance + 1}
      _ -> {:error, :ambiguous_range}
    end
  end

  @spec member?(non_neg_integer(), non_neg_integer(), non_neg_integer()) :: boolean()
  def member?(number, first, last) do
    distance(first, last) < @half and distance(first, number) <= distance(first, last)
  end
end
