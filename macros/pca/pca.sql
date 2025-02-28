{% macro pca(table,
             index=none,
             columns=none,
             rows=none,
             values=none,
             ncomp=none,
             normalize=true,
             standardize=true,
             demean=true,
             missing=none,
             weights=none,
             output='loadings',
             output_options=none,
             method='nipals',
             method_options=none,
             materialization_options=none) -%}

  {#############################################################################

    This function does 3 things:

    1. Resolves and casts polymorphic inputs.
    1. Validates inputs.
    2. Dispatches the appropriate call.

    The actual calculations occur elsewhere in the code, depending on the
    implementation chosen.

  #############################################################################}
  {% set long = ((values is not none) | as_bool) %}

  {# Format the variables, and cast strings to lists #}
  {# ----------------------------------------------- #}

  {% if output_options is none %}
    {% set output_options = {} %}
  {% endif %}

  {% if method_options is none %}
    {% set method_options = {} %}
  {% endif %}

  {% if materialization_options is none %}
    {% set materialization_options = {} %}
  {% endif %}

  {% if rows is not none and index is none %}
    {% set index = rows %}
  {% elif rows is not none and index is not none %}
    {{ exceptions.raise_compiler_error(
      "Please specify either `index` (preferred) or `rows`, not both."
      " `index` is just an alias for `rows`."
    ) }}
  {% endif %}

  {% if method is none %}
    {% set method = 'nipals' %}
  {% endif %}

  {% set calculate_in_steps = dbt_pca._get_materialization_option(
    'calculate_in_steps',
    materialization_options,
    none
  ) %}  #
  {% set inject_config = adapter.type() == 'snowflake' or calculate_in_steps %}
  {% if adapter.type() == 'clickhouse' and calculate_in_steps is none %}
    {% if (table.identifier is undefined or table.is_cte) %}
      {% do log(
        "Warning: when table= is not a `ref()` or a `source()`, pca() cannot be calculated in steps,"
        " which may significantly degrade performance in Clickhouse."
        " It is suggested you set table= to a `ref()` or a `source()`."
        " To disable this warning message, explicitly set calculate_in_steps to false: "
        " `materialization_options={'calculate_in_steps': false}`.",
        info=true
      ) %}
      {% set calculate_in_steps = false %}
    {% else %}
      {% set calculate_in_steps = true %}
    {% endif %}
  {% endif %}

  {# Check for user input errors #}
  {# --------------------------- #}

  {% if long and index is none %}
    {{ exceptions.raise_compiler_error(
      "Missing arg 'index' for macro `pca()`."
      " Index must be set when data is long."
    ) }}
  {% elif index is string %}
    {% set index = [index] %}
  {% endif %}

  {% if long and values is none %}
      {{ exceptions.raise_compiler_error(
        "Must specify a `values=...` if the data is long."
      ) }}
  {% endif %}

  {% if values is not string and values is iterable %}
      {{ exceptions.raise_compiler_error(
        "`values=...` must be a string, specifically a column name."
      ) }}
  {% endif %}

  {% if weights %}
    {% if values is not none %}
      {{ exceptions.raise_compiler_error("Weights are only supported for wide-formatted data.") }}
    {% elif weights is string or (weights | length) != (columns | length) %}
      {{ exceptions.raise_compiler_error(
        "Weights must be a list of numbers, and the length of the list must be equal to the number of columns."
      ) }}
    {% endif %}

    {# Normalize weights to unit length #}
    {% set ns = namespace(w_rms=0) %}
    {% for w in weights %}
      {% set ns.w_rms = ns.w_rms + (w * w) %}
    {% endfor %}
    {% set ns.w_rms = (ns.w_rms / weights | length) ** 0.5 %}
    {% set new_weights = [] %}
    {% for w in weights %}
      {% do new_weights.append(w / ns.w_rms) %}
    {% endfor %}
    {% set weights = new_weights %}

  {% endif %}

  {% if calculate_in_steps and model.config.materialized in ['ephemeral', 'view'] %}
    {{ exceptions.raise_compiler_error(
      "A dbt node calling `dbt_pca.pca()` cannot be a view or an ephemeral model"
      " when the materialization option `calculate_in_steps` is enabled."
      " Please materialize `"~model.name~"` as a table or as an incremental model to use `pca()`."
    ) }}
  {% elif adapter.type() == 'snowflake' and model.config.materialized in ['ephemeral', 'view'] %}
    {{ exceptions.raise_compiler_error(
      "A dbt node calling `dbt_pca.pca()` cannot be a view or an ephemeral model"
      " with the Snowflake adapter."
      " Please materialize `"~model.name~"` as a table or as an incremental model to use `pca()`."
    ) }}
  {% endif %}

  {% if columns is none %}
    {{ exceptions.raise_compiler_error("Missing arg 'columns' for macro `pca()`.") }}
  {% elif columns is string %}
    {% set columns = [columns] %}
  {% endif %}

  {% set VALID_OUTPUTS = [
        'loadings',
        'loadings-long',
        'loadings-wide',
        'eigenvectors-wide',
        'eigenvectors-wide-transposed',
        'coefficients-wide',
        'coefficients-wide-transposed',
        'factors',
        'factors-long',
        'factors-wide',
        'projections',
        'projections-long',
        'projections-wide',
        'projections-untransformed-wide'
  ] %}
  {% if output not in VALID_OUTPUTS %}
      {{ exceptions.raise_compiler_error(
        "Format must be one of: " ~ ( VALID_OUTPUTS | join(', ') ) ~ ". Received: " ~ format ~ "."
      ) }}
  {% endif %}

  {% if long and output in ['eigenvectors-wide-transposed', 'coefficients-wide-transposed', 'projections-wide', 'projections-untransformed-wide'] %}
      {{ exceptions.raise_compiler_error(
        "Long formatted inputs cannot be combined with "~output~" outputs"
        " because the number of columns is not known at compile time. Please choose another"
        " output type or input wide-formatted data."
      ) }}
  {% endif %}

  {% if output in ['loadings-wide', 'factors-wide'] and ncomp is none %}
    {{ exceptions.raise_compiler_error(
      "'" ~output~ "' formatted outputs require that you specify the number of components with `ncomp=...`"
      " because the number of components that will be created (and therefore the number of columns that will"
      " be output) is not known at compile time. Please choose another output type or specify the number of"
      " components."
    ) }}
  {% endif %}

  {% if index is none and output in ['factors', 'factors-long'] %}
    {{ exceptions.raise_compiler_error(
      "Cannot output factors in long format without an index."
      " Please specify an index with `index=...` or choose `factors-wide`."
    ) }}
  {% endif %}

  {% if index is none and output in ['projections', 'projections-long'] %}
    {{ exceptions.raise_compiler_error(
      "Cannot output projections in long format without an index."
      " Please specify an index with `index=...` or choose `projections-wide`."
    ) }}
  {% endif %}

  {% if ncomp is none %}
    {% if values is none %}
      {% set ncomp = (columns | length) %}
    {% elif adapter.type() not in ['duckdb', 'snowflake'] %}
      {{ exceptions.raise_compiler_error(
        "This database's PCA implementation requires defining the number of principal components"
        " when data is long formatted. Please set `dbt_pca.pca(ncomp=...)`"
      ) }}
    {% elif calculate_in_steps %}
      {{ exceptions.raise_compiler_error(
        "When 'calculate_in_steps' is enabled, the number of principal components must be"
        " defined. Please set `dbt_pca.pca(ncomp=...)`"
      ) }}
    {% endif %}
  {% endif %}

  {% if output == 'loadings-wide' %}
    {% do output_options.setdefault('display_eigenvalues', dbt_pca._get_output_option("display_eigenvalues", {}, false)) %}
  {% elif output == 'eigenvectors-wide' %}
    {% set output = 'loadings-wide' %}
    {% do output_options.setdefault('display_eigenvalues', dbt_pca._get_output_option("display_eigenvalues", {}, false)) %}
    {% do output_options.setdefault('display_coefficients', dbt_pca._get_output_option("display_coefficients", {}, false)) %}
  {% elif output == 'coefficients-wide' %}
    {% set output = 'loadings-wide' %}
    {% do output_options.setdefault('display_eigenvalues', dbt_pca._get_output_option("display_eigenvalues", {}, false)) %}
    {% do output_options.setdefault('display_eigenvectors', dbt_pca._get_output_option("display_eigenvectors", {}, false)) %}
  {% endif %}

  {%
    if output in ['eigenvectors-wide', 'coefficients-wide', 'loadings-wide', 'loadings', 'loadings-long']
    and not dbt_pca._get_output_option("display_eigenvectors", output_options, true)
    and not dbt_pca._get_output_option("display_eigenvalues", output_options, true)
    and not dbt_pca._get_output_option("display_coefficients", output_options, true)
  %}
    {{ exceptions.raise_compiler_error(
      "Nothing to display. Please set either display_eigenvectors, display_eigenvalues, or display_coefficients to true."
    ) }}
  {% endif %}

  {% if inject_config and (table.identifier is undefined or table.is_cte) %}
    {% if adapter.type() == 'snowflake' %}
      {% set _error_cond = "When using the Snowflake adapter" %}
    {% elif adapter.type() == 'clickhouse' %}
      {% set _error_cond = "When using the Clickhouse adapter with 'calculate_in_steps' enabled" %}
    {% else %}
      {% set _error_cond = "When 'calculate_in_steps' is turned on" %}
    {% endif %}
    {{ exceptions.raise_compiler_error(
      _error_cond ~ ", the `table=` input must be either a `ref()` or `source()`"
      " to a non-ephemeral node."
    ) }}
  {% endif %}

  {# Dispatch #}
  {# -------- #}

  {% if adapter.type() == 'snowflake' or calculate_in_steps %}
    {% if method == 'nipals' or (adapter.type() == 'snowflake' and method in ['nipals', 'svd', 'eig']) %}
      {{ return(
        adapter.dispatch('_inject_config_into_relation', 'dbt_pca')(
          table=table,
          index=index,
          columns=columns,
          values=values,
          ncomp=ncomp,
          normalize=normalize,
          standardize=standardize,
          demean=demean,
          missing=missing,
          weights=weights,
          output=output,
          output_options=output_options,
          method_options=method_options,
          materialization_options=materialization_options
        )
      ) }}
    {% else %}
      {{ exceptions.raise_compiler_error(
        "Invalid method specified. Please read the README for more information."
      ) }}
    {% endif %}
  {% elif method == 'nipals' %}
    {{ return(
      dbt_pca._pca_nipals(
        table=table,
        index=index,
        columns=columns,
        values=values,
        ncomp=ncomp,
        normalize=normalize,
        standardize=standardize,
        demean=demean,
        missing=missing,
        weights=weights,
        output=output,
        output_options=output_options,
        method_options=method_options,
        materialization_options=materialization_options
      )
    ) }}
  {% else %}
    {{ exceptions.raise_compiler_error(
      "Invalid method specified. The only valid method for non-pca materializations is 'nipals'."
      " Please read the README for more information."
    ) }}
  {% endif %}

{% endmacro %}
