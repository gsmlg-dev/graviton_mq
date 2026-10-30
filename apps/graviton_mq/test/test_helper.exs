ExUnit.start(
  exclude:
    if(System.get_env("GRAVITON_MOSQUITTO_BIN"), do: [], else: [:interop]) ++
      if(System.get_env("GRAVITON_PAHO_PYTHON"), do: [], else: [:paho_interop]) ++
      if(System.get_env("GRAVITON_PROTON_PYTHON"), do: [], else: [:proton_interop])
)
