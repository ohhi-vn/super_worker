defmodule SuperWorker.MixProject do
  use Mix.Project

  def project do
    [
      app: :super_worker,
      version: "0.0.2",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),

      # Docs
      name: "SuperWorker",
      source_url: "https://github.com/ohhi-vn/super_worker",
      home_url: "https://ohhi.vn",
      docs: docs(),
      description: description(),
      package: package()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    required = [:logger]

    dev_app =
      case Mix.env() do
        :dev -> [:observer, :wx]
        _ -> []
      end

    [
      mod: {SuperWorker.Application, []},
      extra_applications: dev_app ++ required,
    ]
  end


  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false},
      {:benchee, "~> 1.3", only: :dev},
    ]
  end

  defp description do
    """
    SuperWorker is a powerful Elixir library for working with supervisor & background jobs.
    Much more simply than traditional supervisor (suit for telecom application).
    This library is in development, so it's not recommended for production use.
    """
  end

  defp package do
    [
      licenses: ["MPL 2.0"],
      maintainers: ["Manh Van Vu"],
      links: %{
        "GitHub" => "https://github.com/ohhi-vn/super_worker",
        "About us" => "https://ohhi.vn"
      }
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: extras()
    ]
  end

  defp extras do
    list =
      "guides/**/*.md"
      |> Path.wildcard()

    list = list ++ ["README.md"]

    list
    |> Enum.map(fn path ->
      title =
        path
        |> Path.basename(".md")
        |> String.split(~r|[-_]|)
        |> Enum.map_join(" ", &String.capitalize/1)
        |> case do
          "F A Q" ->"FAQ"
          no_change -> no_change
        end

      {String.to_atom(path),
        [
          title: title,
          default: title == "Guide"
        ]
      }
    end)

  end
end
