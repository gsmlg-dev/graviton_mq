[version] = System.argv()

case Version.parse(version) do
  {:ok, %Version{build: nil} = parsed} ->
    if Version.to_string(parsed) != version do
      raise ArgumentError, "version must use canonical SemVer, for example 1.2.3 or 1.2.3-rc.1"
    end

  _ ->
    raise ArgumentError, "version must use SemVer without a v prefix or build metadata"
end

File.write!(Path.expand("../VERSION", __DIR__), version <> "\n")
