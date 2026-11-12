defmodule GravitonMQ.Runtime.TLSOptionsTest do
  use ExUnit.Case, async: true

  alias GravitonMQ.Runtime.TLSOptions

  @identity [certfile: "server.pem", keyfile: "server.key"]
  @in_memory_key {:RSAPrivateKey, <<48, 1, 0>>}

  test "accepts the allowlisted options with structurally valid values" do
    assert :ok = TLSOptions.validate(@identity)
    assert :ok = TLSOptions.validate(certfile: ~c"server.pem", keyfile: ~c"server.key")
    assert :ok = TLSOptions.validate(cert: <<48, 1, 0>>, keyfile: "server.key")

    assert :ok =
             TLSOptions.validate(
               certs_keys: [
                 %{certfile: "a.pem", keyfile: "a.key", password: "p"},
                 %{cert: <<48>>, keyfile: "b.key"}
               ]
             )

    assert :ok =
             TLSOptions.validate(
               @identity ++
                 [
                   password: "secret",
                   cacertfile: "ca.pem",
                   verify: :verify_peer,
                   fail_if_no_peer_cert: true,
                   depth: 3,
                   versions: [:"tlsv1.3"],
                   alpn_preferred_protocols: ["mqtt", "amqp"],
                   sni_hosts: [{~c"example.test", [certfile: "e.pem", keyfile: "e.key"]}]
                 ]
             )

    assert :ok =
             TLSOptions.validate(
               @identity ++
                 [
                   password: fn -> "secret" end,
                   cacerts: [<<48, 1>>],
                   verify: :verify_peer,
                   sni_fun: fn _name -> :undefined end
                 ]
             )

    assert :ok =
             TLSOptions.validate(
               @identity ++ [ciphers: [:tls_aes_128_gcm_sha256, "TLS_AES_256_GCM_SHA384", 0x1303]]
             )

    assert :ok =
             TLSOptions.validate(
               @identity ++
                 [
                   ciphers: [
                     %{key_exchange: :any, cipher: :aes_128_gcm, mac: :aead, prf: :sha256}
                   ]
                 ]
             )
  end

  # gsmlg-dev/http_fetch#168 is fixed in hell 1.0.0-rc.17: Hell redacts key,
  # password, sni_fun and nested identity secrets in listener telemetry.
  test "in-memory private keys are accepted wherever an identity is" do
    for options <- [
          [cert: <<48>>, key: @in_memory_key],
          [certfile: "server.pem", key: {:ECPrivateKey, <<48>>}],
          [cert: <<48>>, key: {:PrivateKeyInfo, <<48>>}, password: "secret"],
          [certs_keys: [%{cert: <<48>>, key: @in_memory_key}]],
          @identity ++ [sni_hosts: [{~c"h.test", [cert: <<48>>, key: @in_memory_key]}]],
          @identity ++
            [sni_hosts: [{~c"h.test", [certs_keys: [%{cert: <<48>>, key: @in_memory_key}]]}]]
        ] do
      assert :ok = TLSOptions.validate(options), inspect(options)
    end
  end

  test "OTP-only, session, socket and runtime-owned options are unsupported by key" do
    for key <-
          ~w(verify_fun partial_chain crl_check honor_cipher_order secure_renegotiate
             reuse_sessions session_tickets active mode packet header deliver packet_size
             send_timeout send_timeout_close buffer ip active_chunk_size)a do
      assert {:error, {:unsupported_tls_option, ^key}} =
               TLSOptions.validate(@identity ++ [{key, :anything}])
    end

    assert {:error, {:unsupported_tls_option, :active_chunk_size}} =
             TLSOptions.validate(@identity ++ [active_chunk_size: 16_384])
  end

  test "only TLS 1.3 may be configured" do
    for versions <- [[:"tlsv1.2"], [:"tlsv1.3", :"tlsv1.2"], [], :"tlsv1.3"] do
      assert {:error, {:unsupported_tls_option, :versions}} =
               TLSOptions.validate(@identity ++ [versions: versions])
    end
  end

  test "malformed values and missing prerequisites report only the key" do
    for {options, key} <- [
          {[certfile: "", keyfile: "k"], :certfile},
          {[certfile: "c", keyfile: 1], :keyfile},
          {[cert: "", keyfile: "k"], :cert},
          {[cert: <<48>>, key: <<48>>], :key},
          {[cert: <<48>>, key: {:DSAPrivateKey, <<48>>}], :key},
          {[cert: <<48>>, key: {:RSAPrivateKey, <<>>}], :key},
          {@identity ++ [password: 42], :password},
          {@identity ++ [cacertfile: ""], :cacertfile},
          {@identity ++ [cacerts: []], :cacerts},
          {@identity ++ [cacerts: [:not_der]], :cacerts},
          {@identity ++ [verify: :verify_maybe], :verify},
          {@identity ++ [verify: :verify_peer], :verify},
          {@identity ++ [fail_if_no_peer_cert: true], :fail_if_no_peer_cert},
          {@identity ++ [cacertfile: "ca", verify: :verify_peer, fail_if_no_peer_cert: :yes],
           :fail_if_no_peer_cert},
          {@identity ++ [depth: 0], :depth},
          {@identity ++ [depth: 101], :depth},
          {@identity ++ [ciphers: []], :ciphers},
          {@identity ++ [ciphers: [:tls_ecdhe_rsa_with_aes_128_gcm_sha256]], :ciphers},
          {@identity ++ [ciphers: [0x1301, :tls_aes_128_gcm_sha256]], :ciphers},
          {@identity ++ [alpn_preferred_protocols: []], :alpn_preferred_protocols},
          {@identity ++ [alpn_preferred_protocols: ["a", "a"]], :alpn_preferred_protocols},
          {@identity ++ [alpn_preferred_protocols: [""]], :alpn_preferred_protocols},
          {@identity ++ [sni_fun: fn -> nil end], :sni_fun},
          {@identity ++ [sni_hosts: []], :sni_hosts},
          {@identity ++ [sni_hosts: [{"binary-name", @identity}]], :sni_hosts},
          {@identity ++ [sni_hosts: [{~c"h.test", [certfile: "c"]}]], :sni_hosts},
          {@identity ++ [sni_hosts: [{~c"h.test", [cert: <<48>>, key: :bad]}]], :sni_hosts},
          {@identity ++ [sni_hosts: [{~c"H.test", @identity}, {~c"h.test", @identity}]],
           :sni_hosts},
          {@identity ++
             [sni_hosts: [{~c"h.test", @identity}], sni_fun: fn _name -> :undefined end],
           :sni_fun},
          {[certs_keys: []], :certs_keys},
          {[certs_keys: [%{certfile: "c"}]], :certs_keys},
          {[certs_keys: [%{cert: <<48>>, key: :bad}]], :certs_keys},
          {[certs_keys: [%{certfile: "c", keyfile: "k", verify: :verify_peer}]], :certs_keys},
          {[certs_keys: List.duplicate(%{certfile: "c", keyfile: "k"}, 17)], :certs_keys},
          {[certfile: "c"], :keyfile},
          {[cert: <<48>>], :keyfile},
          {[keyfile: "k"], :certfile},
          {[key: @in_memory_key], :certfile},
          {[], :certfile},
          {@identity ++ [certfile: "again"], :certfile}
        ] do
      assert {:error, {:invalid_tls_option, ^key}} = TLSOptions.validate(options),
             inspect(options)
    end

    assert {:error, {:invalid_tls_option, :tls_options}} = TLSOptions.validate(:not_a_list)

    assert {:error, {:invalid_tls_option, :tls_options}} =
             TLSOptions.validate([:binary | @identity])
  end

  # Defence in depth (hell rc.17 also redacts): a closure prints as
  # #Function<...>, so a password never appears in inspected Hell state.
  test "passwords reach Hell only inside closures" do
    options =
      TLSOptions.hell_options(
        @identity ++
          [
            password: "first-secret",
            certs_keys: [%{certfile: "a", keyfile: "b", password: ~c"second-secret"}],
            sni_hosts: [{~c"h.test", [certfile: "c", keyfile: "k", password: "third-secret"]}]
          ]
      )

    printed = inspect(options, limit: :infinity, printable_limit: :infinity)
    for secret <- ["first-secret", "second-secret", "third-secret"], do: refute(printed =~ secret)
    assert options[:password].() == "first-secret"
    assert [%{password: entry}] = options[:certs_keys]
    assert entry.() == ~c"second-secret"
    assert [{~c"h.test", identity}] = options[:sni_hosts]
    assert identity[:password].() == "third-secret"
    assert Keyword.take(options, [:certfile, :keyfile]) == @identity

    callback = fn -> "provided" end
    assert TLSOptions.hell_options(password: callback)[:password] == callback

    assert TLSOptions.hell_options(cert: <<48>>, key: @in_memory_key) ==
             [cert: <<48>>, key: @in_memory_key]
  end

  test "verify_peer?/1 reflects the listener's client-certificate request" do
    assert TLSOptions.verify_peer?(verify: :verify_peer)
    refute TLSOptions.verify_peer?(verify: :verify_none)
    refute TLSOptions.verify_peer?([])
  end
end
