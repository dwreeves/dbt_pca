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
             _materialized=none) -%}

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

  {% if _materialized is none %}
    {% set _materialized = model.config.materialized %}
  {% endif %}

  {# Check for user input errors #}
  {# --------------------------- #}

  {% if _materialized == 'pca' and (table.identifier is undefined or table.is_cte) %}
    {{ exceptions.raise_compiler_error(
      "When using the 'pca' materialization, the `table=` input must be either a `ref()` or `source()`"
      " to a non-ephemeral node."
    ) }}
  {% endif %}

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
      {% elif adapter.type() != 'duckdb' %}
        {{ exceptions.raise_compiler_error(
          "This database's PCA implementation requires defining the number of principal components"
          " when data is long formatted. Please set `dbt_pca.pca(ncomp=...)`"
        ) }}
      {% elif _materialized == 'pca' %}
        {{ exceptions.raise_compiler_error(
          "The 'pca' materialization requires defining the number of principal components for long-formatted data."
          " Please set `ncomp=...`."
        ) }}
      {% endif %}
  {% endif %}

  {% if output == 'eigenvectors-wide' %}
    {% set output = 'loadings-wide' %}
    {% do output_options.setdefault('display_eigenvalues', false) %}
    {% do output_options.setdefault('display_coefficients', false) %}
  {% elif output == 'coefficients-wide' %}
    {% set output = 'loadings-wide' %}
    {% do output_options.setdefault('display_eigenvalues', false) %}
    {% do output_options.setdefault('display_eigenvectors', false) %}
  {% endif %}

  {# Dispatch #}
  {# -------- #}

  {% if _materialized == 'pca' and method == 'nipals' %}
    {{ return(
      dbt_pca._inject_config_into_materialization(
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
        method_options=method_options
      )
    ) }}
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
        method_options=method_options
      )
    ) }}
  {% else %}
    {{ exceptions.raise_compiler_error(
      "Invalid method specified. The only valid method is 'nipals'"
    ) }}
  {% endif %}

{% endmacro %}
