excluded = if System.get_env("GRAVITON_MOSQUITTO_BIN"), do: [], else: [:interop]

excluded =
  if System.get_env("GRAVITON_PAHO_PYTHON"), do: excluded, else: [:paho_interop | excluded]

excluded =
  if System.get_env("ARTEMIS_HOST") && System.get_env("GRAVITON_PROTON_PYTHON"),
    do: excluded,
    else: [:artemis_interop | excluded]

excluded =
  if System.get_env("ARTEMIS_TLS_PORT") && System.get_env("ARTEMIS_TLS_CA") &&
       System.get_env("ARTEMIS_TLS_UNTRUSTED_CA"),
     do: excluded,
     else: [:artemis_tls_interop | excluded]

ExUnit.start(exclude: excluded)
