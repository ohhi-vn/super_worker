# Used by "mix format"
inputs =
  ["{mix,.formatter,.iex}.exs", "{config,lib,test}/**/*.{ex,exs}"]
  |> Enum.flat_map(&Path.wildcard(&1, match_dot: true))
  |> Enum.reject(&String.contains?(Path.basename(&1), "._"))

[
  inputs: inputs
]
