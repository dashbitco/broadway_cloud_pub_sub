defmodule BroadwayCloudPubSub.MixProject do
  use Mix.Project

  @version "0.1.3"
  @description "A Google Cloud Pub/Sub connector for Broadway"
  @repo_url "https://github.com/mcrumm/broadway_cloud_pub_sub"

  def project do
    [
      app: :broadway_cloud_pub_sub,
      version: @version,
      elixir: "~> 1.5",
      elixirc_paths: elixirc_paths(Mix.env()),
      name: "BroadwayCloudPubSub",
      description: @description,
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: docs(),
      package: package()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:broadway, "~> 0.3.0"},
      {:google_api_pub_sub, "~> 0.4"},
      {:goth, "~> 0.6", optional: true},
      {:uuid, "~> 1.0", only: :test},
      {:junit_formatter, "~> 3.0", only: [:test]},
      {:ex_doc, ">= 0.19.0", only: :docs}
    ]
  end

  defp docs do
    [
      main: "BroadwayCloudPubSub.Producer",
      source_ref: "v#{@version}",
      source_url: @repo_url,
      extras: [
        "README.md",
        "CHANGELOG.md"
      ]
    ]
  end

  defp package do
    %{
      licenses: ["Apache 2.0"],
      links: %{"GitHub" => @repo_url}
    }
  end
end
