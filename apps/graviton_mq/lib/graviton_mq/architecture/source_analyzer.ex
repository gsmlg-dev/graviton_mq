defmodule GravitonMQ.Architecture.SourceAnalyzer do
  @moduledoc """
  Extracts module references from parsed Elixir source syntax.

  This supplements compiler xref manifests for references that can exist only
  in typespec syntax. It parses the AST rather than searching source text.
  """

  alias GravitonMQ.Architecture
  alias GravitonMQ.Architecture.Reference

  @protocol_apps [:graviton_mq_amqp10, :graviton_mq_mqtt]
  @direct_process_primitives [:spawn, :spawn_link, :spawn_monitor, :send, :receive]

  @protocol_process_primitives [
    :spawn_opt,
    :spawn_request,
    :self,
    :exit,
    :monitor,
    :demonitor,
    :link,
    :unlink,
    :register,
    :unregister,
    :whereis,
    :process_flag,
    :process_info,
    :send_after,
    :start_timer,
    :cancel_timer,
    :read_timer
  ]
  @protocol_clock_primitives [
    :monotonic_time,
    :system_time,
    :time_offset,
    :timestamp,
    :now,
    :date,
    :time,
    :localtime,
    :universaltime
  ]

  @spec references(atom(), binary(), binary()) :: [Reference.t()]
  def references(source_app, source_file, source) do
    ast = Code.string_to_quoted!(source, file: source_file, columns: true)

    walk(ast, source_app, source_file, nil, MapSet.new())
    |> Enum.uniq()
    |> Enum.sort_by(fn reference ->
      {reference.source_file, reference.line || 0, inspect(reference.target_module)}
    end)
  end

  defp walk(ast, source_app, source_file, source_module, locals) do
    {_ast, references} =
      Macro.prewalk(ast, [], fn
        {:defmodule, meta, [module_ast, body]}, references ->
          module = module_from_ast(module_ast, source_module)
          nested = walk(body, source_app, source_file, module, local_definitions(body))
          declaration = reference(source_app, module, module, source_file, meta[:line])
          # The recursive walk owns this module's scope; do not visit it again
          # using the enclosing module's local function definitions.
          {nil, [declaration | nested] ++ references}

        {{:., _dot_meta, [:erlang, function]}, call_meta, arguments} = node, references
        when source_app in @protocol_apps and function in @protocol_clock_primitives and
               is_list(arguments) ->
          {node,
           [
             reference(source_app, source_module, System, source_file, call_meta[:line])
             | references
           ]}

        {{:., _dot_meta, [:erlang, function]}, call_meta, arguments} = node, references
        when source_app in @protocol_apps and function in @protocol_process_primitives and
               is_list(arguments) ->
          {node,
           [
             reference(source_app, source_module, Process, source_file, call_meta[:line])
             | references
           ]}

        {function, meta, arguments} = node, references
        when source_app in @protocol_apps and function in @protocol_process_primitives and
               is_list(arguments) ->
          if MapSet.member?(locals, {function, length(arguments)}) do
            {node, references}
          else
            {node,
             [
               reference(source_app, source_module, Process, source_file, meta[:line])
               | references
             ]}
          end

        {:&, _meta, [{:/, _arity_meta, [{function, meta, context}, arity]}]} = node, references
        when is_atom(context) and is_integer(arity) and
               (function in @direct_process_primitives or
                  (source_app in @protocol_apps and function in @protocol_process_primitives)) ->
          if MapSet.member?(locals, {function, arity}) do
            {node, references}
          else
            {node,
             [
               reference(source_app, source_module, Process, source_file, meta[:line])
               | references
             ]}
          end

        {{:., _dot_meta, [{:__aliases__, _alias_meta, [:Kernel]}, function]}, call_meta,
         arguments} = node,
        references
        when is_list(arguments) and
               (function in @direct_process_primitives or
                  (source_app in @protocol_apps and function in @protocol_process_primitives)) ->
          reference =
            reference(source_app, source_module, Process, source_file, call_meta[:line])

          {node, [reference | references]}

        {{:., _dot_meta, [target_module, function]}, call_meta, arguments} = node, references
        when target_module in [Kernel, :erlang] and is_list(arguments) and
               (function in @direct_process_primitives or
                  (source_app in @protocol_apps and function in @protocol_process_primitives)) ->
          reference =
            reference(source_app, source_module, Process, source_file, call_meta[:line])

          {node, [reference | references]}

        {{:., _dot_meta, [target_module, _function]}, call_meta, _arguments} = node, references
        when is_atom(target_module) ->
          reference =
            reference(
              source_app,
              source_module,
              target_module,
              source_file,
              call_meta[:line]
            )

          {node, [reference | references]}

        {:__aliases__, meta, parts} = node, references ->
          target_module =
            case parts do
              [{:__MODULE__, _, _} | rest] -> Module.concat([source_module | rest])
              _ -> Module.concat(parts)
            end

          reference =
            reference(source_app, source_module, target_module, source_file, meta[:line])

          {node, [reference | references]}

        {name, meta, arguments} = node, references
        when name in @direct_process_primitives and is_list(arguments) ->
          if MapSet.member?(locals, {name, length(arguments)}) do
            {node, references}
          else
            reference =
              reference(source_app, source_module, Process, source_file, meta[:line])

            {node, [reference | references]}
          end

        node, references ->
          {node, references}
      end)

    references
  end

  defp local_definitions(body) do
    {_body, definitions} =
      Macro.prewalk(body, MapSet.new(), fn
        {:defmodule, _meta, _arguments}, definitions ->
          {nil, definitions}

        {kind, _meta, [head | _body]}, definitions
        when kind in [:def, :defp, :defmacro, :defmacrop] ->
          {nil, MapSet.union(definitions, function_signatures(head))}

        node, definitions ->
          {node, definitions}
      end)

    definitions
  end

  defp function_signatures({:when, _meta, [head | _guards]}), do: function_signatures(head)

  defp function_signatures({name, _meta, arguments}) when is_atom(name) do
    arguments = arguments || []
    arity = length(arguments)
    defaults = Enum.count(arguments, &match?({:\\, _meta, _arguments}, &1))
    MapSet.new((arity - defaults)..arity, &{name, &1})
  end

  defp module_from_ast({:__aliases__, _meta, [:"Elixir" | parts]}, _parent),
    do: Module.concat(parts)

  defp module_from_ast({:__aliases__, _meta, [{:__MODULE__, _, _} | parts]}, parent),
    do: Module.concat([parent | parts])

  defp module_from_ast({:__aliases__, _meta, parts}, nil), do: Module.concat(parts)

  defp module_from_ast({:__aliases__, _meta, parts}, parent),
    do: Module.concat([parent | parts])

  defp module_from_ast(module, _parent) when is_atom(module), do: module

  defp reference(source_app, source_module, target_module, source_file, line) do
    %Reference{
      source_app: source_app,
      source_module: source_module,
      target_app: Architecture.owner_for_module(target_module),
      target_module: target_module,
      source_file: source_file,
      line: line,
      kind: :syntax
    }
  end
end
