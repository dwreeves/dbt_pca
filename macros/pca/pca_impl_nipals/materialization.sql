{###############################################################################
## Materialization
###############################################################################}

{% macro _find_and_drop_tmp_tables() %}
  {# The difference between _drop_tmp_tables() and _find_and_drop_tmp_tables()
     is that this one makes zero assumptions about which tables exist (hence it "finds" tables).
     whereas _drop_tmp_tables() assumes the only tables that were created are the ones specified in the pca config. #}
  {% set all_statements = [] %}
  {% set target_relation = this.incorporate(type='table') %}
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
      {% do all_statements.append(dbt_pca._drop_table(possible_tmp_relation)) %}
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
      {% do all_statements.append(dbt_pca.drop_table(possible_tmp_relation)) %}
    {% endfor %}
  {% endfor %}
  {{ return(all_statements | reverse) }}
{% endmacro %}

{% macro _run_calculation_steps() %}
  {% set all_statements = [] %}
  {% set target_relation = this.incorporate(type='table') %}
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
        {#{% do all_statements.append(dbt_pca._drop_table(comp_relation)) %}#}
        {% do log('Creating temp relation query '~comp_relation) %}
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
          compnum=compnum,
          count=count) %}
        {% do all_statements.append(dbt_pca._get_create_table_as_sql(True, comp_relation, _sql)) %}
      {% endfor %}
      {% set final_suffix = '__dbt_pca_'~(count | string).zfill(3)~'_final' %}
      {% set combined_comp_relation = make_intermediate_relation(target_relation, suffix=final_suffix) %}
      {#{% do all_statements.append(dbt_pca._drop_table(combined_comp_relation)) %}#}
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
        count=count) %}
      {% do all_statements.append(dbt_pca._get_create_table_as_sql(True, combined_comp_relation, _sql)) %}
      {#{% for rel in (comp_relations | reverse) %}
        {% do all_statements.append(dbt_pca._drop_table(rel)) %}
      {% endfor %}#}
    {% endif %}
  {% endfor %}
  {{ return(all_statements) }}
{% endmacro %}

{% macro _get_create_table_as_sql(temporary, relation, sql) -%}
  {# dbt-clickhouse does not implement create_table_as correctly. #}
  {% if adapter.type() == 'clickhouse' %}
    {% set create_query %}
      create {% if temporary: -%}temporary{%- endif %} table
        {{ relation.include(database=false, schema=(not temporary)) }}
      {% set contract_config = config.get('contract') %}
      {% if contract_config.enforced and (not temporary) %}
        {{ get_assert_columns_equivalent(sql) }}
        {{ get_table_columns_and_constraints() }}
        {%- set sql = get_select_subquery(sql) %}
      {% endif %}
      Engine=Log
      as (
        {{ sql }}
      )
    {% endset %}
    {{ return(create_query) }}
  {% else %}
    {{ return(create_table_as(temporary, relation, sql)) }}
  {% endif %}
{% endmacro %}

{% macro _drop_table(relation) -%}
  {% if adapter.type() == 'clickhouse' %}
    {{ return('drop table if exists ' ~ relation.render()) }}
  {% else %}
    {{ return(drop_table(relation)) }}
  {% endif %}
{% endmacro -%}

{###############################################################################
## Compile step
###############################################################################}

{% macro snowflake___inject_config_into_relation(table,
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
                                                 materialization_options=none)
%}
  {% set __load = load_result('__dbt_pca__pca_num') %}
  {% if __load is none %}
    {% set pca_num = 0 %}
  {% else %}
    {% set pca_num = __load.response + 1 %}
  {% endif %}
  {% do store_result('__dbt_pca__pca_num', pca_num) %}
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
  {% set sql = dbt_pca.create_pca_udtf(
      table_json=table,
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
      method=method,
      method_options=method_options,
      materialization_options=materialization_options,
      pca_num=pca_num
  ) %}
  {% do config(
    pre_hook=[{"sql": sql, "transaction": true}]
  ) %}
  {% if dbt_pca._get_materialization_option('drop_udf', materialization_options, true) %}
    {% set arg_data = dbt_pca._get_udtf_function_signature_data(
      table,
      index,
      columns,
      values,
      materialization_options
    ) %}
    {% set types_list = [] %}
    {% for o in arg_data %}
      {% do types_list.append(o['type']) %}
    {% endfor %}
    {% set drop_statement -%}
      drop function if exists {{ dbt_pca._get_udtf_name(table, materialization_options, pca_num) }}({{ ', '.join(types_list) }});
    {%- endset %}
    {% do config(
      post_hook=[{"sql": drop_statement, "transaction": true}]
    ) %}
  {% endif %}
  {% if dbt_pca._get_materialization_option('cast_types_to_udtf', materialization_options, false) %}
    {% set arg_data = dbt_pca._get_udtf_function_signature_data(table, index, columns, values, materialization_options) %}
    {% set final_query %}(
  -- !DBT_PCA_CONFIG:{{ (__count | string).zfill(3) }}:{{ tojson(dbt_pca_config) }}
  select p.*
  from {{ table }} as t,
  table(
    {{ dbt_pca._get_udtf_name(table, materialization_options, __count) }}({{ dbt_pca._get_udtf_function_args(index, columns, values, cast_types=true, arg_data=arg_data) }})
    over (partition by 1)
  ) as p
){% endset %}
  {% else %}
  {% set final_query %}(
  -- !DBT_PCA_CONFIG:{{ (__count | string).zfill(3) }}:{{ tojson(dbt_pca_config) }}
  select p.*
  from {{ table }} as t,
  table(
    {{ dbt_pca._get_udtf_name(table, materialization_options, __count) }}({{ dbt_pca._get_udtf_function_args(index, columns, values) }})
    over (partition by 1)
  ) as p
){% endset %}
{% endif %}
{{ return(final_query) }}
{% endmacro %}

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
  {% for compnum in range(ncomp) %}
    {% set comp_relation = make_intermediate_relation(table, suffix=dbt_pca._temp_table_suffix(pca_count, compnum)
    ) %}
    {% do log('Creating temp relation query '~comp_relation) %}
    {% set _sql = dbt_pca._pca_tmp_table(
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
      compnum=compnum,
      pca_num=pca_num) %}
    {% do injected_pre_hooks.append({"sql": dbt_pca._get_create_table_as_sql(True, comp_relation, _sql), "transaction": true}) %}
  {% endfor %}
  {% set final_suffix = '__dbt_pca_'~(pca_num | string).zfill(3)~'_final' %}
  {% set combined_comp_relation = make_intermediate_relation(table, suffix=final_suffix) %}
  {% set _sql = dbt_pca._pca_tmp_table_final(
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
    pca_num=pca_num
  ) %}
  {% do injected_pre_hooks.append(dbt_pca._get_create_table_as_sql(True, combined_comp_relation, _sql)) %}
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

{% macro _get_udtf_function_args(index, columns, values, cast_types=false, arg_data=none) %}
  {% if values is not none %}
    {% set values = [values] %}
  {% endif %}
  {% if cast_types %}
    {% set _type_maps = {} %}
    {% for o in arg_data %}
      {% do _type_maps.update({o['col']: o['type']}) %}
    {% endfor %}
    {% set _final = [] %}
    {% for i in ((index or []) + columns + (values or [])) %}
      {% do _final.append(i ~ '::' ~ _type_maps[i]) %}
    {% endfor %}
    {{ return(dbt_pca._list_with_alias(_final, 't') ) }}
  {% endif %}
  {{ return(dbt_pca._list_with_alias((index or []) + columns + (values or []), 't')) }}
{% endmacro %}

{% macro _get_udtf_function_signature(table, index, columns, values, materialization_options) %}
  {% set arg_data = dbt_pca._get_udtf_function_signature_data(table, index, columns, values, materialization_options) %}
  {% set li = [] %}
  {% for o in arg_data %}
    {% do li.append(adapter.quote(o['col']) ~ ' ' ~ o['type']) %}
  {% endfor %}
  {{ return(', '.join(li)) }}
{% endmacro %}

{% macro _get_udtf_function_signature_data(table, index, columns, values, materialization_options) %}
  {# Snowflake does not allow generic typing for UDFs except via function overloading.
     We can cast types to varchar and that works OK for loadings outputs.
     However, it causes issues for other types of outputs because the type.
     In these cases, we could do type overloading, or infer types from the ref. #}
  {% set index_types = materialization_options.get('index_types') %}
  {% set column_types = materialization_options.get('column_types') %}
  {% set values_type = materialization_options.get('values_type') %}
  {% set rel_columns_mapping = {} %}
  {% if
      table is not none
      and table is not undefined
      and
      (
        (index or [] | length) != (index_types or [] | length)
        or (columns | length) != (column_types or [] | length)
        or (values is not none) != (values_type is not none)
      )
  %}
    {# In this case, not all types are explicitly defined, so we want to attempt
       querying the database for the types. #}
    {% set rel_columns = adapter.get_columns_in_relation(table) %}
    {% for c in rel_columns %}
      {% do rel_columns_mapping.update({c.column.lower(): c.dtype}) %}
      {% do rel_columns_mapping.update({c.column: c.dtype}) %}
    {% endfor %}
  {% endif %}
  {% set li = [] %}
  {% for i in (index or []) %}
    {% if index_types %}
      {% set typ = index_types[loop.index-1] %}
    {% elif i in rel_columns_mapping %}
      {% set typ = rel_columns_mapping.get(i) %}
    {% elif i.lower() in rel_columns_mapping %}
      {% set typ = rel_columns_mapping.get(i.lower()) %}
    {% else %}
      {% set typ = 'varchar' %}
    {% endif %}
    {% do li.append({'col': i, 'type': typ}) %}
  {% endfor %}
  {% for i in columns %}
    {% if column_types %}
      {% set typ = column_types[loop.index-1] %}
    {% elif i in rel_columns_mapping %}
      {% set typ = rel_columns_mapping.get(i) %}
    {% elif i.lower() in rel_columns_mapping %}
      {% set typ = rel_columns_mapping.get(i.lower()) %}
    {% elif values is none %}
      {% set typ = 'float' %}
    {% else %}
      {% set typ = 'varchar' %}
    {% endif %}
    {% do li.append({'col': i, 'type': typ}) %}
  {% endfor %}
  {% if values is not none %}
    {% if values_type %}
      {% set typ = values_type %}
    {% elif values in rel_columns_mapping %}
      {% set typ = rel_columns_mapping.get(values) %}
    {% elif values.lower() in rel_columns_mapping %}
      {% set typ = rel_columns_mapping.get(values.lower()) %}
    {% else %}
      {% set typ = 'float' %}
    {% endif %}
    {% do li.append({'col': values, 'type': typ}) %}
  {% endif %}
  {{ return(li) }}
{% endmacro %}

{% macro _get_udtf_name(table, materialization_options, pca_num) %}
  {% set udf_database = dbt_pca._get_materialization_option('udf_database', materialization_options, table.database) %}
  {% set udf_schema = dbt_pca._get_materialization_option('udf_schema', materialization_options, table.schema) %}
  {% if udtf_database and udtf_schema %}
    {{ return(
      adapter.quote_as_configured(udf_database, 'identifier')
      ~ '.' ~
      adapter.quote_as_configured(udf_schema, 'identifier')
      ~ '.' ~
      adapter.quote_as_configured(model.name~'__dbt_pca_udtf_'~(pca_num | string).zfill(3), 'identifier')
    ) }}
  {% elif udtf_schema %}
    {{ return(
      adapter.quote_as_configured(udf_schema, 'identifier')
      ~ '.' ~
      adapter.quote_as_configured(model.name~'__dbt_pca_udtf_'~(pca_num | string).zfill(3), 'identifier')
    ) }}
  {% else %}
    {{ return(
      adapter.quote_as_configured(model.name~'__dbt_pca_udtf_'~(pca_num | string).zfill(3), 'identifier')
    ) }}
  {% endif %}
{% endmacro %}

{% macro _get_udtf_return_signature(table, index, columns, values, output, materialization_options, output_options) %}
  {{ return('comp integer, col varchar, eigenvector float, eigenvalue float, coefficient float') }}
  {# Snowflake does not allow generic typing for UDFs except via function overloading.
     We can cast types to varchar and that works OK for loadings outputs.
     However, it causes issues for other types of outputs because the type.
     In these cases, we could do type overloading, or infer types from the ref. #}
  {% set index_types = materialization_options.get('index_types') %}
  {% set column_types = materialization_options.get('column_types') %}
  {% set values_type = materialization_options.get('values_type') %}
  {% set rel_columns_mapping = {} %}
  {% set requires_columns_as_pk = output in ['loadings', 'loadings-long', 'loadings-wide', 'eigenvectors-wide', 'coefficients-wide', 'projections', 'projections-long'] %}
  {% set requires_index = output in ['factors', 'factors-long', 'factors-wide', 'projections', 'projections-long', 'projections-wide', 'projections-untransformed-wide'] %}
  {% if
      true
  %}
    {# In this case, not all types are explicitly defined, so we want to attempt
       querying the database for the types. #}
    {% set rel_columns = adapter.get_columns_in_relation(table) %}
    {% for c in rel_columns %}
      {% do rel_columns_mapping.update({c.column.lower(): c.dtype}) %}
      {% do rel_columns_mapping.update({c.column: c.dtype}) %}
    {% endfor %}
  {% endif %}
  {% set li = [] %}
  {% set columns_li = [] %}
  {% set index_li = [] %}
  {% set other_li = [] %}
  {% if output in ['loadings', 'loadings-long', 'eigenvectors-wide-transposed', 'coefficients-wide-transposed', 'factors', 'factors-long'] %}
    {% do li.append(dbt_pca._get_output_option("component_column_name", output_options, "comp") ~ ' integer') %}
  {% endif %}
  {% if requires_columns_as_pk and values is none %}
    {% do columns_li.append(dbt_pca._get_output_option("column_column_name", output_options, "col") ~ ' varchar') %}
  {% elif requires_columns_as_pk and values is not none %}
    {% for i in columns %}
      {% if column_types %}
        {% set typ = column_types[loop.index-1] %}
      {% elif i in rel_columns_mapping %}
        {% set typ = rel_columns_mapping.get(i) %}
      {% elif i.lower() in rel_columns_mapping %}
        {% set typ = rel_columns_mapping.get(i.lower()) %}
      {% else %}
        {% set typ = 'varchar' %}
      {% endif %}
      {% do columns_li.append(i ~ ' ' ~ typ) %}
    {% endfor %}
  {% elif output in ['eigenvectors-wide-transposed', 'coefficients-wide-transposed', 'projections-wide', 'projections-untransformed-wide'] %}
    {% for i in columns %}
      {% do columns_li.append(i ~ ' float') %}
    {% endfor %}
  {% endif %}

  {% if output in ['factors', 'factors-long', 'factors-wide', 'projections', 'projections-long', 'projections-wide', 'projections-untransformed-wide'] %}
    {% for i in (index or []) %}
      {% if index_types %}
        {% set typ = index_types[loop.index-1] %}
      {% elif i in rel_columns_mapping %}
        {% set typ = rel_columns_mapping.get(i) %}
      {% elif i.lower() in rel_columns_mapping %}
        {% set typ = rel_columns_mapping.get(i.lower()) %}
      {% else %}
        {% set typ = 'varchar' %}
      {% endif %}
      {% do li.append(adapter.quote(i) ~ ' ' ~ typ) %}
    {% endfor %}
  {% endif %}
  {% if values is not none %}
    {% if values_type %}
      {% set typ = values_type %}
    {% elif values in rel_columns_mapping %}
      {% set typ = rel_columns_mapping.get(values) %}
    {% elif values.lower() in rel_columns_mapping %}
      {% set typ = rel_columns_mapping.get(values.lower()) %}
    {% else %}
      {% set typ = 'float' %}
    {% endif %}
    {% do li.append(adapter.quote(values) ~ ' ' ~ typ) %}
  {% endif %}
  {{ return(', '.join(li)) }}
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
{%- if compnum == 0 %}
  {%- set previous = 'dbt_pca_preproc_step2' %}
{%- else %}
  {% set previous = dbt_pca._single_comp_temp_table_quoted_name(pca_num, compnum - 1) %}
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

  {%- for compnum in range(ncomp) %}
  select {{ compnum }} as comp, * from {{ dbt_pca._single_comp_temp_table_quoted_name(pca_num, compnum) }}
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
