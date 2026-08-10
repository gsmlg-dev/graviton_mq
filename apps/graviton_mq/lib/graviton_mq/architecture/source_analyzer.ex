defmodule GravitonMQ.Architecture.SourceAnalyzer do
  @moduledoc """
  Extracts module references from parsed Elixir source syntax.

  This supplements compiler xref manifests for references that can exist only
  in typespec syntax. It parses the AST rather than searching source text.
  """

  alias GravitonMQ.Architecture
  alias GravitonMQ.Architecture.Reference

  @direct_process_primitives [:spawn, :spawn_link, :spawn_monitor, :send, :receive]

  @spec references(atom(), binary(), binary()) :: [Reference.t()]
  def references(source_app, source_file, source) do
    ast = Code.string_to_quoted!(source, file: source_file, columns: true)
    source_module = first_module(ast)

    {_ast, references} =
      Macro.prewalk(ast, [], fn
        {{:., _dot_meta, [{:__aliases__, _alias_meta, [:Kernel]}, function]}, call_meta,
         arguments} = node,
        references
        when function in @direct_process_primitives and is_list(arguments) ->
          reference =
            reference(source_app, source_module, Process, source_file, call_meta[:line])

          {node, [reference | references]}

        {{:., _dot_meta, [target_module, function]}, call_meta, arguments} = node, references
        when target_module in [Kernel, :erlang] and function in @direct_process_primitives and
               is_list(arguments) ->
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
          target_module = Module.concat(parts)

          reference =
            reference(source_app, source_module, target_module, source_file, meta[:line])

          {node, [reference | references]}

        {name, meta, arguments} = node, references
        when name in @direct_process_primitives and is_list(arguments) ->
          reference =
            reference(source_app, source_module, Process, source_file, meta[:line])

          {node, [reference | references]}

        node, references ->
          {node, references}
      end)

    references
    |> Enum.uniq()
    |> Enum.sort_by(fn reference ->
      {reference.source_file, reference.line || 0, inspect(reference.target_module)}
    end)
  end

  defp first_module(ast) do
    {_ast, module} =
      Macro.prewalk(ast, nil, fn
        {:defmodule, _meta, [module_ast, _body]} = node, nil ->
          {node, module_from_ast(module_ast)}

        node, module ->
          {node, module}
      end)

    module
  end

  defp module_from_ast({:__aliases__, _meta, parts}), do: Module.concat(parts)
  defp module_from_ast(module) when is_atom(module), do: module

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
