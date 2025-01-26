defmodule SuperWorker.MixProject do
  use Mix.Project

  def project do
    [
      app: :super_worker,
      version: "0.0.1",
      elixir: "~> 1.14",
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
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
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
      licenses: ["MIT"],
      maintainers: ["ohhi-vn"],
      links: %{
        "GitHub" => "https://github.com/ohhi-vn/super_worker",
        "About us" => "https://ohhi.vn"
      }
    ]
  end

  defp docs do
    [
      main: "README.md"
    ]
  end
end
