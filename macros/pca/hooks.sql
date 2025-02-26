{# Hooks in dbt cannot normally be loaded lazily,
   unless you are a little creative.

   During compile time, these will just return a 'select 1'.
   During execution time, the hooks are recompiled and return
   the actual SQL.

   This discrepancy exists because the hooks do not have access
   to the compiled SQL when they actually get compiled! And this
   is required to read from the injected configs.
#}
