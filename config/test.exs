import Config

# Print only warnings and errors during test
config :logger, level: :warning

# Debug log statements are compiled out so coverage reflects real logic.
# To verify them instead, flip to true, rebuild cleanly, and run the suite
# (this has caught real bugs inside log closures).
config :super_worker, debug_log: false
