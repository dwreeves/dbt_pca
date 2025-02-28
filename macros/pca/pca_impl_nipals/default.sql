{###############################################################################
## Compile step
###############################################################################}

{% macro default___inject_config_into_relation(table,
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
  {% set __load = load_result('__dbt_pca__pca_num') %}
  {% if __load is none %}
    {% set pca_num = 0 %}
  {% else %}
    {% set pca_num = __load.response + 1 %}
  {% endif %}
  {% do store_result('__dbt_pca__pca_num', pca_num) %}
  {% set injected_pre_hooks = [] %}
  {% for comp_num in range(ncomp) %}
    {% do injected_pre_hooks.append({
      "sql": "{{ dbt_pca.calculate_comp(pca_num="~pca_num~", comp_num="~comp_num~") }}",
      "transaction": true
    }) %}
  {% endfor %}
  {% do injected_pre_hooks.append({
    "sql": "{{ dbt_pca.calculate_final(pca_num="~pca_num~") }}",
    "transaction": true
  }) %}
  {{ config(pre_hook=injected_pre_hooks) }}
  {% set dbt_pca_config = {
    "table": {
      "database": table.database,
      "schema": table.schema,
      "identifier": table.identifier
    },
    "index": index,
    "columns": columns,
    "rows": rows,
    "values": values,
    "weights": weights,
    "ncomp": ncomp,
    "normalize": normalize,
    "standardize": standardize,
    "demean": demean,
    "missing": missing,
    "output": output,
    "output_options": output_options,
    "method": method,
    "method_options": method_options,
    "materialization_options": materialization_options
  } %}
  {% set final_query -%}
(
  -- !DBT_PCA_CONFIG:{{ (__count | string).zfill(3) }}:{{ tojson(dbt_pca_config) }}
  {#- todo: replace select star with select columns. not for perf reasons, it just reads better. #}
  select *
  from {{ model.name~'__dbt_pca_'~(count | string).zfill(3)~'_final' }}
)
  {%- endset %}
  {{ return(final_query) }}
{% endmacro %}

{% macro retrieve_injected_config(pca_num) %}
  {% for row in model['compiled_code'].split("\n") %}
    {% if row.lstrip().startswith("-- !DBT_PCA_CONFIG") %}
      {% set comp_relations = [] %}
      {% set parsed_row = row.strip().replace("-- !DBT_PCA_CONFIG:", "", 1) %}
      {% set __pca_num = (parsed_row[:3] | int) %}
      {% if __pca_num == pca_num %}
        {{ return(fromjson(parsed_row[4:])) }}
      {% endif %}
    {% endif %}
  {% endfor %}
{% endmacro %}

{% macro calculate_comp(comp_num, pca_num) %}
  {% if 'compiled_code' not in model %}
    {{ return('select 1 /* dbt_pca.calculate_comp(comp_num='~comp_num~', pca_num='~pca_num~') */;') }}
  {% endif %}
  {% set pca_config = dbt_pca.retrieve_injected_config(pca_num) %}
  {% set input_relation = adapter.get_relation(
    database=pca_config["table"]["database"],
    schema=pca_config["table"]["schema"],
    identifier=pca_config["table"]["identifier"]
  ) %}
  {% set comp_relation = make_intermediate_relation(this, suffix=dbt_pca._temp_table_suffix(pca_count, comp_num)) %}
  {% do log('Creating temp relation '~comp_relation) %}
  {% set _sql = dbt_pca._pca_tmp_table(
    table=input_relation,
    index=pca_config.get("index"),
    columns=pca_config.get("columns"),
    values=pca_config.get("values"),
    ncomp=pca_config.get("ncomp"),
    normalize=pca_config.get("normalize"),
    standardize=pca_config.get("standardize"),
    demean=pca_config.get("demean"),
    missing=pca_config.get("missing"),
    weights=pca_config.get("weights"),
    output=pca_config.get("output"),
    output_options=pca_config.get("output_options"),
    method_options=pca_config.get("method_options"),
    comp_num=comp_num,
    pca_num=pca_num
  ) %}
  {{ return(dbt_pca._get_create_table_as_sql(True, comp_relation, _sql)) }}
{% endmacro %}

{% macro calculate_final(pca_num) %}
  {% if 'compiled_code' not in model %}
    {{ return('select 1 /* dbt_pca.calculate_final(pca_num='~pca_num~') */;') }}
  {% endif %}
  {% set pca_config = dbt_pca.retrieve_injected_config(pca_num) %}
  {% set final_suffix = '__dbt_pca_'~(pca_num | string).zfill(3)~'_final' %}
  {% set input_relation = adapter.get_relation(
    database=pca_config["table"]["database"],
    schema=pca_config["table"]["schema"],
    identifier=pca_config["table"]["identifier"]
  ) %}
  {% set combined_comp_relation = make_intermediate_relation(this, suffix=final_suffix) %}
  {% set _sql = dbt_pca._pca_tmp_table_final(
    table=input_relation,
    index=pca_config.get("index"),
    columns=pca_config.get("columns"),
    values=pca_config.get("values"),
    ncomp=pca_config.get("ncomp"),
    normalize=pca_config.get("normalize"),
    standardize=pca_config.get("standardize"),
    demean=pca_config.get("demean"),
    missing=pca_config.get("missing"),
    weights=pca_config.get("weights"),
    output=pca_config.get("output"),
    output_options=pca_config.get("output_options"),
    method_options=pca_config.get("method_options"),
    materialization_options=pca_config.get("materialization_options"),
    pca_num=pca_num
  ) %}
  {{ return(dbt_pca._get_create_table_as_sql(True, combined_comp_relation, _sql)) }}
{% endmacro %}

{###############################################################################
## Execution step
###############################################################################}

{% macro _pca_tmp_table(table,
                        index,
                        columns,
                        values,
                        ncomp,
                        normalize,
                        standardize,
                        demean,
                        missing,
                        weights,
                        output,
                        output_options,
                        method_options,
                        comp_num,
                        pca_num) %}
{% set long = ((values is not none) | as_bool) %}
{%- set cols = dbt_pca._alias_columns_to_list(columns) if long else ['col'] %}
{%- set idx = dbt_pca._alias_index_to_list(index) if index else ['idx'] %}
{% set check_tol = dbt_pca._get_method_option("nipals", "check_tol", method_options, false) %}
{% set tol = dbt_pca._get_method_option("nipals", "tol", method_options, 5e-7) %}
{% if not (1 >= tol >= 0) %}
    {{ exceptions.raise_compiler_error(
      "method_options['tol'] must be between 0 and 1. "
    ) }}
{% endif %}
{% set deterministic_column_seeding = dbt_pca._get_method_option("nipals", "deterministic_column_seeding", method_options, false) %}
{% set max_iter = dbt_pca._get_method_option("nipals", "max_iter", method_options, 500 if adapter.type() == 'duckdb' else 150) %}
{% if max_iter < 0 %}
    {{ exceptions.raise_compiler_error(
      "method_options['max_iter'] must be an integer ≥ 0. "
    ) }}
{% endif %}
{%- if ncomp is none %}
  {% set ncomp = (columns | length) %}
{%- endif %}
{%- if comp_num == 0 %}
  {%- set previous = 'dbt_pca_preproc_step2' %}
{%- else %}
  {% set previous = dbt_pca._single_comp_temp_table_quoted_name(pca_num, comp_num - 1) %}
{%- endif %}
with recursive

{{ dbt_pca.preproc_step_1_cte(
  table=table,
  columns=columns,
  index=index,
  values=values,
  weights=weights,
  long=long,
  output_options=output_options
) }},

{{ dbt_pca.preproc_step_2_cte(
  cols=cols,
  idx=idx,
  standardize=standardize,
  demean=demean,
  weights=weights,
  include_iter=true
) }},

{{ dbt_pca._pca_nipals_single_iteration(
  previous=previous,
  idx=idx,
  cols=cols,
  comp_num=comp_num,
  check_tol=check_tol,
  tol=tol,
  max_iter=max_iter,
  deterministic_column_seeding=deterministic_column_seeding,
) }}

select * from dbt_pca_comp_{{ comp_num }}
{% endmacro %}

{% macro _pca_tmp_table_final(table,
                              index,
                              columns,
                              values,
                              ncomp,
                              normalize,
                              standardize,
                              demean,
                              missing,
                              weights,
                              output,
                              output_options,
                              method_options,
                              materialization_options,
                              pca_num) %}
{% set long = ((values is not none) | as_bool) %}
{%- set cols = dbt_pca._alias_columns_to_list(columns) if long else ['col'] %}
{%- set idx = dbt_pca._alias_index_to_list(index) if index else ['idx'] %}
with
{%- if output not in ['loadings', 'loadings-long', 'loadings-wide', 'eigenvectors-wide-transposed', 'coefficients-wide-transposed']  %}

{{ dbt_pca.preproc_step_1_cte(
  table=table,
  columns=columns,
  index=index,
  values=values,
  weights=weights,
  long=long,
  output_options=output_options
) }},

{{ dbt_pca.preproc_step_2_cte(
  cols=cols,
  idx=idx,
  standardize=standardize,
  demean=demean,
  weights=weights
) }},

{%- endif %}
dbt_pca_comps_combined as (

  {%- for comp_num in range(ncomp) %}
  select {{ comp_num }} as comp, * from {{ dbt_pca._single_comp_temp_table_quoted_name(pca_num, comp_num) }}
  {% if not loop.last %}union all{% endif %}
  {%- endfor %}

),

{{ dbt_pca.final_output(
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
) }}
{% endmacro %}

{###############################################################################
## Utils
###############################################################################}

{% macro _single_comp_temp_table_quoted_name(pca_num, comp) %}
    {{ return(adapter.quote_as_configured(model.name~dbt_pca._temp_table_suffix(pca_num, comp), 'identifier')) }}
{% endmacro %}

{% macro _temp_table_suffix(pca_num, comp) %}
    {{ return('__dbt_pca_'~(pca_num | string).zfill(3)~'_ncomp'~(comp | string)) }}
{% endmacro %}
