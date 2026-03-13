import Config

# Configures Elixir's Logger
config :logger, :default_formatter,
  format: "[$level] $message - $metadata\n",
  metadata: [:error_code, :mfa, :file, :line, :request_id]

config :logger, level: :debug

config :super_worker, debug_log: false
