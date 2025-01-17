{###############################################################################
## Materialization
###############################################################################}

{% materialization pca, default %}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set backup_relation = none -%}
  {%- set preexisting_backup_relation = none -%}
  {%- set preexisting_intermediate_relation = none -%}

  {% for _count_check in range(1000) %}
    {% set possible_tmp_relation = make_intermediate_relation(target_relation, suffix=dbt_pca._temp_table_suffix(_count_check, 0)) %}
    {% set possible_tmp_relation = adapter.get_relation(
      database=possible_tmp_relation.database,
      schema=possible_tmp_relation.schema,
      identifier=possible_tmp_relation.identifier
    ) %}
      {% if possible_tmp_relation is none or possible_tmp_relation is undefined %}
        {% break %}
      {% endif %}
    {{ drop_relation_if_exists(possible_tmp_relation) }}
    {% for _compnum_check in range(1, 100000) %}
      {% set possible_tmp_relation = make_intermediate_relation(target_relation, suffix=dbt_pca._temp_table_suffix(_count_check, 0)) %}
      {% set possible_tmp_relation = adapter.get_relation(
        database=possible_tmp_relation.database,
        schema=possible_tmp_relation.schema,
        identifier=possible_tmp_relation.identifier
      ) %}
      {% if possible_relation is none or possible_relation is undefined %}
        {% break %}
      {% endif %}
      {{ drop_relation_if_exists(possible_tmp_relation) }}
    {% endfor %}
  {% endfor %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- drop the temp relations if they exist already in the database
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {% set final_relations = [] %}
  {% for row in model.compiled_code.split("\n") %}
    {% if row.lstrip().startswith("-- !DBT_PCA_CONFIG") %}
      {% set comp_relations = [] %}
      {% set parsed_row = row.strip().replace("-- !DBT_PCA_CONFIG:", "", 1) %}
      {% set count = (parsed_row[:3] | int) %}
      {% set pca_config = fromjson(parsed_row[4:]) %}
      {% set input_relation = adapter.get_relation(
        database=pca_config["table"]["database"],
        schema=pca_config["table"]["schema"],
        identifier=pca_config["table"]["identifier"]
      ) %}
      {% for compnum in range(pca_config["ncomp"]) %}
        {% set comp_relation = make_intermediate_relation(target_relation, suffix=dbt_pca._temp_table_suffix(count, compnum)) %}
        {% do comp_relations.append(comp_relation) %}
        {{ drop_relation_if_exists(comp_relation) }}
        {% do log('Creating temp relation '~comp_relation) %}
        {% set _sql = render(dbt_pca._pca_tmp_table(
          table=input_relation,
          index=pca_config.get("index"),
          columns=pca_config.get("columns"),
          values=pca_config.get("values"),
          ncomp=pca_config.get("ncomp"),
          normalize=pca_config.get("normalize"),
          standardize=pca_config.get("standardize"),
          demean=pca_config.get("demean"),
          missing=pca_config.get("missing"),
          output=pca_config.get("output"),
          output_options=pca_config.get("output_options"),
          method_options=pca_config.get("method_options"),
          compnum=compnum,
          count=count)) %}
        {{ run_hooks(pre_hooks, inside_transaction=True) }}
        {% call statement(dbt_pca._temp_table_suffix(count, compnum)) -%}
          {{ get_create_table_as_sql(False, comp_relation, _sql) }}
        {%- endcall %}
         {{ run_hooks(post_hooks, inside_transaction=True) }}
      {% endfor %}
    {% set final_suffix = '__dbt_pca_'~(count | string).zfill(3)~'_final' %}
    {% set combined_comp_relation = make_intermediate_relation(target_relation, suffix=final_suffix) %}
    {{ drop_relation_if_exists(combined_comp_relation) }}
    {% do final_relations.append(combined_comp_relation) %}
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
      output=pca_config.get("output"),
      output_options=pca_config.get("output_options"),
      method_options=pca_config.get("method_options"),
      count=count) %}
    {{ run_hooks(pre_hooks, inside_transaction=True) }}
    {% call statement(final_suffix) -%}
      {{ get_create_table_as_sql(False, combined_comp_relation, _sql) }}
    {%- endcall %}
    {{ run_hooks(post_hooks, inside_transaction=True) }}
    {% for rel in (comp_relations | reverse) %}
      {{ drop_relation_if_exists(rel) }}
    {% endfor %}
    {% endif %}
  {% endfor %}

  {% if existing_relation is not none %}
    {%- set backup_relation_type = existing_relation.type -%}
    {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
    {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
    {% if not existing_relation.can_exchange %}
      {%- set intermediate_relation =  make_intermediate_relation(target_relation) -%}
      {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
    {% endif %}
  {% endif %}

  {% set grant_config = config.get('grants') %}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if backup_relation is none %}
    {{ log('Creating new relation ' + target_relation.name )}}
    -- There is not existing relation, so we can just create
    {% call statement('main') -%}
      {{ get_create_table_as_sql(False, target_relation, sql) }}
    {%- endcall %}
  {% elif existing_relation.can_exchange %}
    -- We can do an atomic exchange, so no need for an intermediate
    {% call statement('main') -%}
      {{ get_create_table_as_sql(False, backup_relation, sql) }}
    {%- endcall %}
    {% do exchange_tables_atomic(backup_relation, existing_relation) %}
  {% else %}
    -- We have to use an intermediate and rename accordingly
    {% call statement('main') -%}
      {{ get_create_table_as_sql(False, intermediate_relation, sql) }}
    {%- endcall %}
    {{ adapter.rename_relation(existing_relation, backup_relation) }}
    {{ adapter.rename_relation(intermediate_relation, target_relation) }}
  {% endif %}

  -- cleanup
  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {{ drop_relation_if_exists(backup_relation) }}

  {% for rel in (final_relations | reverse ) %}
    {{ drop_relation_if_exists(rel) }}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

{###############################################################################
## Compile step
###############################################################################}

{% macro _inject_config_into_materialization(table,
                                             index=none,
                                             columns=none,
                                             rows=none,
                                             values=none,
                                             ncomp=none,
                                             normalize=true,
                                             standardize=true,
                                             demean=true,
                                             missing=none,
                                             weights=weights,
                                             output='loadings',
                                             output_options=none,
                                             method='nipals',
                                             method_options=none) -%}
{% set __dbt_pca_count = load_result('__dbt_pca_count') %}
{% if __dbt_pca_count is none %}
  {% set __count = 0 %}
{% else %}
  {% set __count = __dbt_pca_count.response + 1 %}
{% endif %}
{% do store_result('__dbt_pca_count', __count) %}
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
    "ncomp": ncomp,
    "normalize": normalize,
    "standardize": standardize,
    "demean": demean,
    "missing": missing,
    "output": output,
    "output_options": output_options,
    "method": method,
    "method_options": method_options
  } %}
{% set final_query %}(
  -- !DBT_PCA_CONFIG:{{ (__count | string).zfill(3) }}:{{ tojson(dbt_pca_config) }}
  {#- todo: replace select star with select columns. not for perf reasons, it just reads better. #}
  select *
  from {{ model.name~'__dbt_pca_'~(count | string).zfill(3)~'_final' }}
)
{% endset %}
{{ return(final_query) }}
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
                        compnum,
                        count) %}
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
{%- if compnum == 0 %}
  {%- set previous = 'dbt_pca_preproc_step2' %}
{%- else %}
  {% set previous = dbt_pca._single_comp_temp_table_quoted_name(count, compnum - 1) %}
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
  compnum=compnum,
  check_tol=check_tol,
  tol=tol,
  max_iter=max_iter,
  deterministic_column_seeding=deterministic_column_seeding,
) }}

select * from dbt_pca_comp_{{ compnum }}
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
                              count) %}
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

  {%- for compnum in range(ncomp) %}
  select {{ compnum }} as comp, * from {{ dbt_pca._single_comp_temp_table_quoted_name(count, compnum) }}
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

{% macro _single_comp_temp_table_quoted_name(count, comp) %}
    {{ return(adapter.quote_as_configured(model.name~dbt_pca._temp_table_suffix(count, comp), 'identifier')) }}
{% endmacro %}

{% macro _temp_table_suffix(count, comp) %}
    {{ return('__dbt_pca_'~(count | string).zfill(3)~'_ncomp'~(comp | string)) }}
{% endmacro %}
