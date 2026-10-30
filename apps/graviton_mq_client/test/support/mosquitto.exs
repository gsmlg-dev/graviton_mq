Code.require_file("peer_process.exs", __DIR__)

defmodule GravitonMQ.Client.Test.Mosquitto do
  @moduledoc false
  import ExUnit.Assertions
  import ExUnit.Callbacks, only: [start_supervised!: 2]

  alias GravitonMQ.Client.Test.PeerProcess

  def executables! do
    binary = System.fetch_env!("GRAVITON_MOSQUITTO_BIN")

    assert File.regular?(binary),
           "GRAVITON_MOSQUITTO_BIN must name the pinned Mosquitto executable"

    {version, 0} = System.cmd(binary, ["-h"], stderr_to_stdout: true)
    assert version =~ ~r/mosquitto version 2\.1\.2(?:\s|$)/

    binaries = %{
      broker: binary,
      publisher: Path.join(Path.dirname(binary), "mosquitto_pub"),
      subscriber: Path.join(Path.dirname(binary), "mosquitto_sub"),
      password: Path.join(Path.dirname(binary), "mosquitto_passwd"),
      openssl: System.find_executable("openssl")
    }

    for name <- [:publisher, :subscriber, :password, :openssl] do
      assert is_binary(binaries[name]) and File.regular?(binaries[name]),
             "missing interoperability executable: #{name}"
    end

    binaries
  end

  def directory! do
    directory =
      Path.join(
        System.tmp_dir!(),
        "graviton-mosquitto-#{System.pid()}-#{System.unique_integer([:positive])}"
      )

    File.mkdir!(directory)
    directory
  end

  def broker!(executables, directory, options \\ []) do
    {:ok, reservation} = :gen_tcp.listen(0, ip: {127, 0, 0, 1}, active: false)
    {:ok, {{127, 0, 0, 1}, port}} = :inet.sockname(reservation)
    :ok = :gen_tcp.close(reservation)
    config = Path.join(directory, "mosquitto.conf")

    File.write!(config, """
    listener #{port} 127.0.0.1
    allow_anonymous #{Keyword.get(options, :anonymous, true)}
    persistence false
    max_qos #{Keyword.get(options, :max_qos, 1)}
    log_dest stdout
    log_type all
    #{Keyword.get(options, :configuration, "")}
    """)

    owner = process!(executables.broker, ["-c", config, "-v"])
    wait_ready!(owner, port)
    %{owner: owner, port: port, directory: directory}
  end

  def process!(executable, args) do
    start_supervised!({PeerProcess, executable: executable, args: args}, id: make_ref())
  end

  def command!(executable, args) do
    owner = process!(executable, args)
    eventually!(fn -> snapshot(owner).exit_status != nil end, fn -> snapshot(owner) end)
    %{output: output, exit_status: status} = snapshot(owner)
    assert status == 0, "independent command exited #{status}: #{output}"
    output
  end

  def snapshot(owner), do: PeerProcess.snapshot(owner)

  def restart!(broker) do
    :ok = PeerProcess.restart(broker.owner)
    wait_ready!(broker.owner, broker.port)
  end

  def cli_args(broker, options \\ []) do
    [
      "-V",
      "mqttv5",
      "-h",
      Keyword.get(options, :host, "127.0.0.1"),
      "-p",
      Integer.to_string(broker.port)
    ] ++ Keyword.get(options, :extra, [])
  end

  def subscriber!(executables, broker, topic, options \\ []) do
    client_id = "independent-sub-#{System.unique_integer([:positive])}"

    owner =
      process!(
        executables.subscriber,
        cli_args(broker, options) ++
          [
            "-i",
            client_id,
            "-t",
            topic,
            "-q",
            Integer.to_string(Keyword.get(options, :qos, 1)),
            "-C",
            "1",
            "-W",
            "5",
            "-d",
            "-F",
            "%t|%q|%p"
          ]
      )

    eventually!(
      fn -> snapshot(broker.owner).output =~ "Sending SUBACK to #{client_id}" end,
      fn -> %{subscriber: snapshot(owner), broker: snapshot(broker.owner)} end
    )

    owner
  end

  def publish!(executables, broker, topic, payload, qos, options \\ []) do
    command!(
      executables.publisher,
      cli_args(broker, options) ++ ["-t", topic, "-m", payload, "-q", Integer.to_string(qos)]
    )
  end

  def certificates!(executables, directory) do
    certificate = Path.join(directory, "localhost.pem")
    private_key = Path.join(directory, "localhost.key")
    ca_certificate = Path.join(directory, "ca.pem")
    ca_key = Path.join(directory, "ca.key")
    untrusted = Path.join(directory, "untrusted.pem")

    for {name, cert, key} <- [
          {"Graviton Test CA", ca_certificate, ca_key},
          {"untrusted", untrusted, Path.join(directory, "untrusted.key")}
        ] do
      command!(executables.openssl, [
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-days",
        "1",
        "-subj",
        "/CN=#{name}",
        "-addext",
        "basicConstraints=critical,CA:TRUE",
        "-addext",
        "keyUsage=critical,keyCertSign,cRLSign",
        "-keyout",
        key,
        "-out",
        cert
      ])
    end

    request = Path.join(directory, "localhost.csr")
    extensions = Path.join(directory, "localhost.ext")

    File.write!(extensions, """
    basicConstraints=critical,CA:FALSE
    subjectAltName=DNS:localhost
    keyUsage=critical,digitalSignature,keyEncipherment
    extendedKeyUsage=serverAuth
    """)

    command!(executables.openssl, [
      "req",
      "-new",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-subj",
      "/CN=localhost",
      "-keyout",
      private_key,
      "-out",
      request
    ])

    command!(executables.openssl, [
      "x509",
      "-req",
      "-in",
      request,
      "-CA",
      ca_certificate,
      "-CAkey",
      ca_key,
      "-set_serial",
      "1",
      "-days",
      "1",
      "-extfile",
      extensions,
      "-out",
      certificate
    ])

    %{
      certificate: certificate,
      ca_certificate: ca_certificate,
      ca_private_key: ca_key,
      private_key: private_key,
      untrusted: untrusted
    }
  end

  def eventually!(predicate, diagnostic \\ fn -> :unavailable end, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    eventually_until!(predicate, diagnostic, deadline)
  end

  defp eventually_until!(predicate, diagnostic, deadline) do
    if predicate.() do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        flunk("interoperability condition timed out: #{inspect(diagnostic.())}")
      end

      Process.sleep(10)
      eventually_until!(predicate, diagnostic, deadline)
    end
  end

  defp wait_ready!(owner, port) do
    eventually!(
      fn ->
        case :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false], 100) do
          {:ok, socket} ->
            :gen_tcp.close(socket)
            true

          {:error, _reason} ->
            false
        end
      end,
      fn -> snapshot(owner) end
    )
  end
end
