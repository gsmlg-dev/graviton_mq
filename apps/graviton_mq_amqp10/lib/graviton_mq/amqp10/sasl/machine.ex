defmodule GravitonMQ.AMQP10.SASL.Machine do
  @moduledoc """
  Pure AMQP 1.0 SASL negotiation (Part 5 §5.3) for ANONYMOUS and PLAIN.

  Same contract as `GravitonMQ.AMQP10.Endpoint.step/2`: generation-fenced
  facts `%{generation, now_ms, event}` return `{state, effects}`. The machine
  owns no socket, process or clock and runs before the Endpoint; a shell steps
  the Endpoint `:start` only after `{:start_endpoint, header_or_nil}`.

  Events: `:start`, `{:header, ProtocolHeader.t()}`, `{:frame, Frame.t()}`,
  `{:decode_error, Codec.Error.t()}`, `{:write_result, {:sasl, n}, result}`,
  `{:auth_result, ref, :ok | {:error, :auth | :sys | :sys_perm | :sys_temp}}`,
  `{:timeout, :sasl_negotiation, token}` and `{:transport_down, reason}`.

  Effects: `{:send_bytes, generation, {:sasl, n}, bytes}`,
  `{:wake_at, generation, :sasl_negotiation, token, at}`,
  `{:cancel_wake, generation, :sasl_negotiation, token}`, `{:expect_header, [0]}`,
  `{:authenticate, ref, credentials}`, `{:cancel_authentication, ref}`,
  `{:outcome, code_name}`, `{:established, :no_sasl | {:sasl, mechanism}}`,
  `{:start_endpoint, ProtocolHeader.t() | nil}` and `{:close, reason}`.

  A server never stores credentials: they leave only inside
  `{:authenticate, ...}`, and the principal stays in the shell. A client drops
  its credentials once sasl-init is emitted. Final refusal bytes (a protocol
  header or a failing sasl-outcome) are flushed before `{:close, reason}`;
  protocol violations close without an outcome. ANONYMOUS and PLAIN are
  single-step, so a challenge or response is a protocol error.
  """

  alias GravitonMQ.AMQP10.{Frame, SASL, Value}
  alias GravitonMQ.AMQP10.Codec.{Error, ProtocolHeader}
  alias GravitonMQ.AMQP10.Codec.SASL, as: SASLCodec
  alias GravitonMQ.AMQP10.SASL.{Anonymous, Challenge, Init, Mechanisms, Outcome, Plain}

  @sasl_header <<"AMQP", 3, 1, 0, 0>>
  @amqp_header <<"AMQP", 0, 1, 0, 0>>
  @deadline :sasl_negotiation
  @failure_codes [:auth, :sys, :sys_perm, :sys_temp]

  defstruct [
    :role,
    :mode,
    :mechanisms,
    :authenticate,
    :credentials,
    :hostname,
    :timeout_ms,
    generation: 0,
    phase: :new,
    now_ms: nil,
    next_write: 0,
    next_token: 0,
    next_auth: 0,
    deadline: nil,
    closing: nil,
    auth: nil,
    mechanism: nil,
    early_header: nil
  ]

  @type t :: %__MODULE__{}

  @spec new(keyword()) :: {:ok, t()} | {:error, :invalid_options}
  def new(options) when is_list(options) do
    if Keyword.keyword?(options), do: build(Map.new(options)), else: {:error, :invalid_options}
  end

  def new(_options), do: {:error, :invalid_options}

  @doc "True until the AMQP layer owns input (`:done`) or negotiation closed."
  @spec active?(t()) :: boolean()
  def active?(%__MODULE__{phase: phase}), do: phase not in [:done, :closed]

  @doc "Timer key used for the negotiation deadline in wake effects."
  @spec deadline_key() :: :sasl_negotiation
  def deadline_key, do: @deadline

  defp build(%{role: :server} = options) do
    state = %__MODULE__{
      role: :server,
      mode: Map.get(options, :mode, :optional),
      mechanisms: Map.get(options, :mechanisms, [SASL.anonymous()]),
      authenticate: Map.get(options, :authenticate, false),
      timeout_ms: Map.get(options, :timeout_ms),
      generation: Map.get(options, :generation, 0)
    }

    cond do
      not common_valid?(state) -> {:error, :invalid_options}
      state.mode not in [:optional, :required, :disabled] -> {:error, :invalid_options}
      not is_boolean(state.authenticate) -> {:error, :invalid_options}
      state.mode != :disabled and not mechanisms_valid?(state) -> {:error, :invalid_options}
      true -> {:ok, state}
    end
  end

  defp build(%{role: :client} = options) do
    state = %__MODULE__{
      role: :client,
      mode: :required,
      credentials: Map.get(options, :credentials),
      hostname: Map.get(options, :hostname),
      timeout_ms: Map.get(options, :timeout_ms),
      generation: Map.get(options, :generation, 0)
    }

    with true <- common_valid?(state),
         {:ok, init} <- client_init(state),
         {:ok, _frame} <- SASLCodec.encode_frame(init) do
      {:ok, state}
    else
      _invalid -> {:error, :invalid_options}
    end
  end

  defp build(_options), do: {:error, :invalid_options}

  defp common_valid?(state),
    do:
      is_integer(state.timeout_ms) and state.timeout_ms > 0 and is_integer(state.generation) and
        state.generation >= 0

  defp mechanisms_valid?(%{mechanisms: mechanisms, authenticate: authenticate}) do
    is_list(mechanisms) and mechanisms != [] and Enum.uniq(mechanisms) == mechanisms and
      Enum.all?(mechanisms, &(&1 in SASL.supported_mechanisms())) and
      (authenticate or SASL.plain() not in mechanisms)
  end

  @spec step(t(), map()) :: {t(), list()}
  def step(
        %__MODULE__{generation: generation} = state,
        %{generation: generation, now_ms: now, event: event}
      )
      when is_integer(now) do
    if is_integer(state.now_ms) and now < state.now_ms,
      do: {state, []},
      else: dispatch(%{state | now_ms: now}, event)
  end

  def step(%__MODULE__{} = state, _facts), do: {state, []}

  # Common fences.
  defp dispatch(%{phase: phase} = state, _event) when phase in [:done, :closed], do: {state, []}
  defp dispatch(state, {:transport_down, reason}), do: close(state, reason)

  defp dispatch(%{deadline: {token, _at}} = state, {:timeout, @deadline, token}),
    do: close(%{state | deadline: nil}, :sasl_timeout)

  defp dispatch(state, {:timeout, _key, _token}), do: {state, []}

  defp dispatch(%{closing: {id, reason}} = state, {:write_result, id, _result}),
    do: close(state, reason)

  defp dispatch(%{closing: {_id, _reason}} = state, _event), do: {state, []}
  defp dispatch(state, {:write_result, _id, :ok}), do: {state, []}
  defp dispatch(state, {:write_result, _id, {:error, reason}}), do: close(state, reason)

  # Server role.
  defp dispatch(%{role: :server, phase: :new} = state, :start) do
    {state, wake} = set_deadline(state)
    {%{state | phase: :await_header}, wake}
  end

  defp dispatch(
         %{role: :server, phase: :await_header} = state,
         {:header, %ProtocolHeader{protocol_id: 3}}
       ) do
    if state.mode == :disabled do
      fail(state, :sasl_not_supported, @amqp_header)
    else
      {:ok, frame} = SASLCodec.encode_frame(%Mechanisms{server_mechanisms: mechanism_list(state)})
      {state, _id, sent} = send_bytes(state, @sasl_header <> frame)
      {%{state | phase: :await_init}, sent}
    end
  end

  defp dispatch(
         %{role: :server, phase: :await_header} = state,
         {:header, %ProtocolHeader{protocol_id: 0} = header}
       ) do
    if state.mode == :required do
      fail(state, :sasl_required, @sasl_header)
    else
      {state, cancel} = cancel_deadline(state)
      {%{state | phase: :done}, cancel ++ [{:established, :no_sasl}, {:start_endpoint, header}]}
    end
  end

  defp dispatch(
         %{role: :server, phase: :await_header} = state,
         {:decode_error, %Error{operation: :protocol_header}}
       ),
       do: fail(state, :unsupported_protocol_header, preferred_header(state))

  defp dispatch(
         %{role: :server, phase: :await_init} = state,
         {:frame, %Frame{type: :sasl, body: body}}
       ) do
    case SASLCodec.decode(body) do
      {:ok, %Init{} = init} -> init(state, init)
      {:ok, _other} -> close(state, :sasl_protocol_error)
      {:error, %Error{reason: reason}} -> close(state, {:decode_error, reason})
    end
  end

  defp dispatch(
         %{role: :server, phase: :authenticating, auth: ref} = state,
         {:auth_result, ref, :ok}
       ),
       do: succeed(%{state | auth: nil}, [])

  defp dispatch(
         %{role: :server, phase: :authenticating, auth: ref} = state,
         {:auth_result, ref, {:error, code}}
       )
       when code in @failure_codes,
       do: outcome_failure(%{state | auth: nil}, code)

  defp dispatch(
         %{role: :server, phase: :authenticating, early_header: nil} = state,
         {:header, %ProtocolHeader{protocol_id: 0} = header}
       ),
       do: {%{state | early_header: header}, []}

  defp dispatch(
         %{role: :server, phase: :await_amqp_header} = state,
         {:header, %ProtocolHeader{protocol_id: 0} = header}
       ) do
    {state, cancel} = cancel_deadline(state)
    {%{state | phase: :done}, cancel ++ [{:start_endpoint, header}]}
  end

  # Client role.
  defp dispatch(%{role: :client, phase: :new} = state, :start) do
    {state, wake} = set_deadline(state)
    {state, _id, sent} = send_bytes(state, @sasl_header)
    {%{state | phase: :await_header}, wake ++ sent}
  end

  defp dispatch(
         %{role: :client, phase: :await_header} = state,
         {:header, %ProtocolHeader{protocol_id: 3}}
       ),
       do: {%{state | phase: :await_mechanisms}, []}

  defp dispatch(
         %{role: :client, phase: :await_header} = state,
         {:header, %ProtocolHeader{protocol_id: 0}}
       ),
       do: close(state, :sasl_not_offered)

  defp dispatch(
         %{role: :client, phase: :await_header} = state,
         {:decode_error, %Error{operation: :protocol_header}}
       ),
       do: close(state, :unsupported_protocol_header)

  defp dispatch(%{role: :client, phase: phase} = state, {:frame, %Frame{type: :sasl, body: body}})
       when phase in [:await_mechanisms, :await_outcome] do
    case {phase, SASLCodec.decode(body)} do
      {:await_mechanisms, {:ok, %Mechanisms{} = mechanisms}} -> choose(state, mechanisms)
      {:await_outcome, {:ok, %Outcome{code: %Value{value: code}}}} -> outcome(state, code)
      {_phase, {:ok, %Challenge{}}} -> close(state, :sasl_protocol_error)
      {_phase, {:ok, _other}} -> close(state, :sasl_protocol_error)
      {_phase, {:error, %Error{reason: reason}}} -> close(state, {:decode_error, reason})
    end
  end

  # Fallbacks shared by both roles.
  defp dispatch(state, :start), do: {state, []}
  defp dispatch(state, {:auth_result, _ref, _result}), do: {state, []}
  defp dispatch(%{phase: :authenticating} = state, _event), do: close(state, :sasl_protocol_error)

  defp dispatch(%{role: :server, phase: :await_amqp_header} = state, _event),
    do: fail(state, :unsupported_protocol_header, @amqp_header)

  defp dispatch(state, {:decode_error, %Error{reason: reason}}),
    do: close(state, {:decode_error, reason})

  defp dispatch(state, _event), do: close(state, :sasl_protocol_error)

  defp choose(state, mechanisms) do
    {:ok, %Init{mechanism: %Value{value: name}} = init} = client_init(state)

    if name in Mechanisms.names(mechanisms) do
      case SASLCodec.encode_frame(init) do
        {:ok, frame} ->
          {state, _id, sent} =
            send_bytes(%{state | credentials: nil, phase: :await_outcome}, frame)

          {state, sent}

        {:error, _error} ->
          close(state, :invalid_sasl_credentials)
      end
    else
      close(state, :sasl_mechanism_not_offered)
    end
  end

  defp outcome(state, 0) do
    {state, cancel} = cancel_deadline(state)
    {%{state | phase: :done}, cancel ++ [{:expect_header, [0]}, {:start_endpoint, nil}]}
  end

  defp outcome(state, code), do: close(state, {:sasl, SASL.code_name(code)})

  defp client_init(%{credentials: credentials, hostname: hostname}) do
    with true <-
           is_nil(hostname) or
             (is_binary(hostname) and byte_size(hostname) in 1..255 and String.valid?(hostname)),
         {:ok, name, response} <- credential_response(credentials) do
      {:ok,
       %Init{
         mechanism: Value.symbol(name),
         initial_response: response,
         hostname: hostname && Value.string(hostname)
       }}
    else
      _invalid -> :error
    end
  end

  defp credential_response(:anonymous), do: {:ok, SASL.anonymous(), nil}

  defp credential_response({:anonymous, trace}) do
    case Anonymous.parse(trace) do
      {:ok, nil} -> {:ok, SASL.anonymous(), nil}
      {:ok, trace} -> {:ok, SASL.anonymous(), Value.binary(trace)}
      {:error, _reason} -> :error
    end
  end

  defp credential_response(%Plain{} = plain) do
    case Plain.encode(plain) do
      {:ok, message} -> {:ok, SASL.plain(), Value.binary(message)}
      {:error, _reason} -> :error
    end
  end

  defp credential_response(_credentials), do: :error

  defp init(state, %Init{mechanism: %Value{value: name}} = init) do
    cond do
      name not in state.mechanisms -> outcome_failure(state, :auth)
      name == SASL.anonymous() -> anonymous(state, init)
      true -> plain(state, init)
    end
  end

  defp anonymous(state, init) do
    case Anonymous.parse(binary_value(init.initial_response)) do
      {:ok, trace} when state.authenticate ->
        authenticate(state, :anonymous, %{
          mechanism: :anonymous,
          username: nil,
          password: nil,
          authzid: nil,
          trace: trace,
          hostname: string_value(init.hostname)
        })

      {:ok, _trace} ->
        succeed(%{state | mechanism: :anonymous}, [{:expect_header, [0]}])

      {:error, _reason} ->
        outcome_failure(state, :auth)
    end
  end

  defp plain(state, %Init{initial_response: %Value{type: :binary, value: message}} = init) do
    case Plain.parse(message) do
      {:ok, plain} ->
        authenticate(state, :plain, %{
          mechanism: :plain,
          username: plain.username,
          password: plain.password,
          authzid: plain.authzid,
          trace: nil,
          hostname: string_value(init.hostname)
        })

      {:error, _reason} ->
        outcome_failure(state, :auth)
    end
  end

  defp plain(state, _init), do: outcome_failure(state, :auth)

  defp authenticate(state, mechanism, credentials) do
    ref = {:sasl_auth, state.next_auth}

    {%{
       state
       | phase: :authenticating,
         auth: ref,
         mechanism: mechanism,
         next_auth: state.next_auth + 1
     }, [{:expect_header, [0]}, {:authenticate, ref, credentials}]}
  end

  defp succeed(state, prefix) do
    {:ok, frame} = SASLCodec.encode_frame(%Outcome{code: Value.ubyte(0)})
    {state, _id, sent} = send_bytes(state, frame)
    established = [{:outcome, :ok}, {:established, {:sasl, state.mechanism}}]

    case state.early_header do
      nil ->
        {%{state | phase: :await_amqp_header}, prefix ++ sent ++ established}

      header ->
        {state, cancel} = cancel_deadline(state)

        {%{state | phase: :done, early_header: nil},
         prefix ++ sent ++ established ++ cancel ++ [{:start_endpoint, header}]}
    end
  end

  defp outcome_failure(state, code) do
    {:ok, frame} = SASLCodec.encode_frame(%Outcome{code: Value.ubyte(SASL.code(code))})
    {state, effects} = fail(state, {:sasl, code}, frame)
    {state, [{:outcome, code} | effects]}
  end

  defp fail(state, reason, bytes) do
    {state, cancel} = cancel_auth(state)
    {state, id, sent} = send_bytes(state, bytes)
    {%{state | closing: {id, reason}}, cancel ++ sent}
  end

  defp close(state, reason) do
    {state, cancel_auth} = cancel_auth(state)
    {state, cancel} = cancel_deadline(state)

    {%{state | phase: :closed, closing: nil, credentials: nil, early_header: nil},
     cancel_auth ++ cancel ++ [{:close, reason}]}
  end

  defp mechanism_list(%{mechanisms: [one]}), do: Value.symbol(one)

  defp mechanism_list(%{mechanisms: many}),
    do: Value.array(:symbol, Enum.map(many, &Value.symbol/1))

  defp preferred_header(%{mode: :required}), do: @sasl_header
  defp preferred_header(_state), do: @amqp_header

  defp send_bytes(state, bytes) do
    id = {:sasl, state.next_write}

    {%{state | next_write: state.next_write + 1}, id,
     [{:send_bytes, state.generation, id, bytes}]}
  end

  defp set_deadline(state) do
    token = state.next_token
    at = state.now_ms + state.timeout_ms

    {%{state | next_token: token + 1, deadline: {token, at}},
     [{:wake_at, state.generation, @deadline, token, at}]}
  end

  defp cancel_deadline(%{deadline: nil} = state), do: {state, []}

  defp cancel_deadline(%{deadline: {token, _at}} = state),
    do: {%{state | deadline: nil}, [{:cancel_wake, state.generation, @deadline, token}]}

  defp cancel_auth(%{auth: nil} = state), do: {state, []}

  defp cancel_auth(%{auth: ref} = state),
    do: {%{state | auth: nil}, [{:cancel_authentication, ref}]}

  defp binary_value(nil), do: nil
  defp binary_value(%Value{type: :binary, value: value}), do: value
  defp string_value(nil), do: nil
  defp string_value(%Value{type: :string, value: value}), do: value
end
