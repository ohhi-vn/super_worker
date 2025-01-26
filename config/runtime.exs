import Config

if config_env() == :prod do
  config :super_worker,
    test_runtime: []
end
