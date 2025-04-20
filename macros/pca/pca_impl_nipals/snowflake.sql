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
      "cte": (table.identifier is undefined or table.is_cte),
      "database": table.database if table.database is not undefined else none,
      "schema": table.database if table.schema is not undefined else none,
      "identifier": table.identifier if table.identifier is not undefined else table
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
  {% do config(
    pre_hook=[{"sql": '{{ dbt_pca.snowflake__create_pca_udf(pca_num='~pca_num~') }}', "transaction": true}]
  ) %}
  {% if dbt_pca._get_materialization_option('drop_udf', materialization_options, true) %}
    {% do config(
      post_hook=[{"sql": '{{ dbt_pca.snowflake__drop_pca_udf(pca_num='~pca_num~') }}', "transaction": true}]
    ) %}
  {% endif %}
  {% if table.identifier is undefined or dbt_pca._get_materialization_option('cast_types_to_udf', materialization_options, not dbt_pca._get_materialization_option('infer_function_signature_types', materialization_options, true)) %}
    {% set arg_data = dbt_pca._get_udtf_function_signature_data(table, index, columns, values, materialization_options) %}
    {% set final_query %}(
  -- !DBT_PCA_CONFIG:{{ (__count | string).zfill(3) }}:{{ tojson(dbt_pca_config) }}
  select p.*
  from {{ table }} as t,
  table(
    {{ dbt_pca._get_udtf_name(materialization_options, __count) }}({{ dbt_pca._get_udtf_function_args(index, columns, values, cast_types=true, arg_data=arg_data) }})
    over (partition by 1)
  ) as p
){% endset %}
  {% else %}
  {% set final_query %}(
  -- !DBT_PCA_CONFIG:{{ (__count | string).zfill(3) }}:{{ tojson(dbt_pca_config) }}
  select p.*
  from {{ table }} as t,
  table(
    {{ dbt_pca._get_udtf_name(materialization_options, __count) }}({{ dbt_pca._get_udtf_function_args(index, columns, values) }})
    over (partition by 1)
  ) as p
){% endset %}
{% endif %}
{{ return(final_query) }}
{% endmacro %}

{% macro snowflake__create_pca_udf(pca_num) %}
  {% if 'compiled_code' not in model %}
    {{ return('select 1 /* dbt_pca.snowflake__create_pca_udf(pca_num='~pca_num~') */;') }}
  {% endif %}
  {% set pca_config = dbt_pca.retrieve_injected_config(pca_num) %}
  {% set final_suffix = '__dbt_pca_'~(pca_num | string).zfill(3)~'_final' %}
  {% if pca_config["table"]["cte"] %}
    {% set input_relation = pca_config["table"]["identifier"] %}
  {% else %}
    {% set input_relation = adapter.get_relation(
      database=pca_config["table"]["database"],
      schema=pca_config["table"]["schema"],
      identifier=pca_config["table"]["identifier"]
    ) %}
  {% endif %}
  {% set _sql = dbt_pca._create_pca_udtf(
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
    method=pca_config.get("method"),
    method_options=pca_config.get("method_options"),
    materialization_options=pca_config.get("materialization_options"),
    pca_num=pca_num
  ) %}
  {{ return(_sql) }}
{% endmacro %}

{% macro _create_pca_udtf(table,
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
                         method,
                         method_options,
                         materialization_options,
                         pca_num)
  -%}
{%- set _columns = columns -%}
{%- set columns = [] -%}
{%- for c in _columns -%}
  {%- do columns.append(c.split("::")[0]) -%}
{%- endfor -%}
{%- set _index = index -%}
{%- set index = [] -%}
{%- for c in (_index or []) -%}
  {%- do index.append(c.split("::")[0]) -%}
{%- endfor -%}
{%- if values is none -%}
  {% set col = [dbt_pca._get_output_option("columns_column_name", output_options, "col")] -%}
  {% set _values = none %}
{%- else -%}
  {%- set col = columns -%}
  {%- set _values = values -%}
  {%- set values = values.split("::")[0] -%}
{%- endif -%}
{%- set compcol = dbt_pca._get_output_option("component_column_name", output_options, "comp") -%}
{%- set projcol = dbt_pca._get_output_option("projection_column_name", output_options, "projection") -%}
{%- set coefcol = dbt_pca._get_output_option("coefficient_column_name", output_options, "coefficient") -%}
{%- set eveccol = dbt_pca._get_output_option("eigenvector_column_name", output_options, "eigenvector") -%}
{%- set evalcol = dbt_pca._get_output_option("eigenvalue_column_name", output_options, "eigenvalue") -%}
{%- set factcol = dbt_pca._get_output_option("factor_column_name", output_options, "factor") -%}
{%- set display_eigenvectors = dbt_pca._get_output_option("display_eigenvectors", output_options, true) -%}
{%- set display_coefficients = dbt_pca._get_output_option("display_coefficients", output_options, true) -%}
{%- set display_eigenvalues = dbt_pca._get_output_option("display_eigenvalues", output_options, true) -%}
create or replace function {{ dbt_pca._get_udtf_name(materialization_options, pca_num) }}({{ dbt_pca._get_udtf_function_signature(table, _index, _columns, _values, materialization_options) }})
returns table ({{ dbt_pca._get_udtf_return_signature(table, _index, _columns, _values, output, ncomp, materialization_options, output_options) }})
language python
runtime_version = 3.9
packages=('pandas', 'numpy', 'statsmodels')
handler='Pca'
as $$
import re
import pandas as pd
from statsmodels.multivariate.pca import PCA
from _snowflake import vectorized

class Pca:
    @vectorized(input=pd.DataFrame)
    def end_partition(self, df):
        {%- if values is not none %}
        df = df.pivot(columns=['{{ "', '".join(columns)  }}'], index=['{{ "', '".join(index) }}'], values='{{ values }}')
        {%- else %}
        df.columns.name = '{{ col[0] }}'
        {%- if index %}
        df.set_index(['{{ "', '".join(index) }}'], inplace=True)
        {%- endif %}
        {%- endif %}
        {% if missing == 'zero' %}
        df = df.fillna(0)
        {% set missing = none %}
        {% endif %}
        pca = PCA(
          df,
          ncomp={{ (ncomp | string).title() }},
          standardize={{ (standardize | string).title() }},
          demean={{ (demean | string).title() }},
          normalize={{ (normalize | string).title() }},
          gls={{ (dbt_pca._get_materialization_option("gls", materialization_options, false) | string).title() }},
          weights={{ (weights | string) }},
          method='{{ method }}',
          missing={{ (missing or 'None' | string) }},
          tol={{ dbt_pca._get_method_option("nipals", "tol", method_options, 5e-8) }},
          max_iter={{ dbt_pca._get_method_option("nipals", "max_iter", method_options, 1000) }},
          svd_full_matrices={{ (dbt_pca._get_method_option("svd", "full_matrices", method_options, false) | string).title() }},
        )

        {# ########## Prep ########## #}

        {% if display_eigenvectors and output in ['loadings', 'loadings-long', 'loadings-wide', 'eigenvectors-wide', 'eigenvectors-wide-transposed'] %}
        loadings = pca.loadings.copy()
        loadings.columns = pd.Index([int(re.search(r'\d+', i).group()) for i in loadings.columns], name="{{ compcol }}")
        loadings = loadings.stack().reset_index().rename(columns={0: "{{ eveccol }}"})
        {% endif %}
        {% if display_coefficients and output in ['loadings', 'loadings-long', 'loadings-wide', 'coefficients-wide', 'coefficients-wide-transposed'] %}
        coeff = pca.coeff.copy()
        coeff.index = pd.Index([int(re.search(r'\d+', i).group()) for i in coeff.index], name="{{ compcol }}")
        while isinstance(coeff, pd.DataFrame):  # Required for multiindexes
            coeff = coeff.stack()
        coeff = coeff.reset_index().rename(columns={0: "{{ coefcol }}"})
        {% endif %}

        {# ########## Return ########## #}

        {% if output in ['loadings', 'loadings-long'] %}
        {% if display_eigenvalues %}
        eigenvals = pca.eigenvals.rename("{{ evalcol }}")
        {% endif %}
        {% if display_eigenvectors and display_coefficients and display_eigenvalues %}
        res = loadings.merge(right=coeff, left_on=["{{ compcol }}", *['{{ "', '".join(col)  }}']], right_on=["{{ compcol }}", *['{{ "', '".join(col)  }}']])
        res = res.merge(right=eigenvals, left_on="{{ compcol }}", right_index=True)
        res = res.sort_values(["{{ compcol }}", *['{{ "', '".join(col)  }}']])
        res = res[["{{ compcol }}", *['{{ "', '".join(col)  }}'], "{{ eveccol }}", "{{ evalcol }}", "{{ coefcol }}"]]
        res = res.reset_index(drop=True)
        {% elif display_eigenvectors and display_coefficients and not display_eigenvalues %}
        res = loadings.merge(right=coeff, left_on=["{{ compcol }}", *['{{ "', '".join(col)  }}']], right_on=["{{ compcol }}", *['{{ "', '".join(col)  }}']])
        res = res.sort_values(["{{ compcol }}", *['{{ "', '".join(col)  }}']])
        res = res[["{{ compcol }}", *['{{ "', '".join(col)  }}'], "{{ eveccol }}", "{{ coefcol }}"]]
        res = res.reset_index(drop=True)
        {% elif display_eigenvectors and not display_coefficients and display_eigenvalues %}
        res = loadings.merge(right=eigenvals, left_on="{{ compcol }}", right_index=True)
        res = res.sort_values(["{{ compcol }}", *['{{ "', '".join(col)  }}']])
        res = res[["{{ compcol }}", *['{{ "', '".join(col)  }}'], "{{ eveccol }}", "{{ evalcol }}"]]
        res = res.reset_index(drop=True)
        {% elif not display_eigenvectors and display_coefficients and display_eigenvalues %}
        res = coeff.merge(right=eigenvals, left_on="{{ compcol }}", right_index=True)
        res = res.sort_values(["{{ compcol }}", *['{{ "', '".join(col)  }}']])
        res = res[["{{ compcol }}", *['{{ "', '".join(col)  }}'], "{{ evalcol }}", "{{ coefcol }}"]]
        res = res.reset_index(drop=True)
        {% elif display_eigenvectors and not display_coefficients and not display_eigenvalues %}
        res = loadings
        res = res.sort_values(["{{ compcol }}", *['{{ "', '".join(col)  }}']])
        res = res[["{{ compcol }}", *['{{ "', '".join(col)  }}'], "{{ eveccol }}"]]
        res = res.reset_index(drop=True)
        {% elif not display_eigenvectors and display_coefficients and not display_eigenvalues %}
        res = coeff
        res = res[["{{ compcol }}", *['{{ "', '".join(col)  }}'], "{{ coefcol }}"]]
        res = res.reset_index(drop=True)
        {% elif not display_eigenvectors and not display_coefficients and display_eigenvalues %}
        res = loadings.merge(right=eigenvals, left_on="{{ compcol }}", right_index=True)
        res = res.sort_values(["{{ compcol }}", *['{{ "', '".join(col)  }}']])
        res = res[["{{ compcol }}", *['{{ "', '".join(col)  }}'], "{{ eveccol }}"]]
        res = res.reset_index(drop=True)
        {% endif %}
        return res
        {% elif output in ['loadings-wide', 'eigenvectors-wide', 'coefficients-wide'] %}
        {% if display_eigenvectors and display_coefficients %}
        res = loadings.merge(right=coeff, left_on=["{{ compcol }}", *['{{ "', '".join(col)  }}']], right_on=["{{ compcol }}", *['{{ "', '".join(col)  }}']])
        res = res.pivot(index=['{{ "', '".join(col)  }}'], columns=["{{ compcol }}"], values=["eigenvector", "coefficient"])
        res.columns = [f"{c[0]}_{c[1]}" for c in res.columns]
        return res.reset_index()
        {% elif display_eigenvectors %}
        loadings = loadings.pivot(index=['{{ "', '".join(col)  }}'], columns=["{{ compcol }}"], values=["{{ eveccol }}"])
        loadings.columns = [f"{{ eveccol }}_{c}" for c in loadings.columns]
        return loadings.reset_index()
        {% elif display_coefficients %}
        coeff = coeff.pivot(index=['{{ "', '".join(col)  }}'], columns=["{{ compcol }}"], values="{{ coefcol }}")
        coeff.columns = [f"{{ coefcol }}_{c}" for c in coeff.columns]
        return coeff.reset_index()
        {% endif %}
        loadings.columns = [f"{c[0]}_{c[1]}" for c in res.columns]
        {% elif output == 'eigenvectors-wide-transposed' %}
        {# This will be wide format, so it is guaranteed to not be multi-indexed. #}
        loadings = loadings.pivot(columns=['{{ "', '".join(col)  }}'], index=["{{ compcol }}"], values="{{ eveccol }}")
        return loadings.reset_index()[["{{ compcol }}", *['{{ "', '".join(columns)  }}']]]
        {% elif output == 'coefficients-wide-transposed' %}
        {# This will be wide format, so it is guaranteed to not be multi-indexed. #}
        coeff = coeff.pivot(columns=['{{ "', '".join(col)  }}'], index=["{{ compcol }}"], values="{{ coefcol }}")
        return coeff.reset_index()[["{{ compcol }}", *['{{ "', '".join(columns)  }}']]]
        {% elif output in ['factors', 'factors-long'] %}
        factors = pca.factors
        factors.columns = [int(re.search(r'\d+', i).group()) for i in factors.columns]
        factors.columns.name = "{{ compcol }}"
        return factors.stack().reset_index().rename(columns={0: "{{ factcol }}"})[["{{ compcol }}", *['{{ "', '".join(index)  }}'], "{{ factcol }}"]]
        {% elif output in ['factors-wide'] %}
        factors = pca.factors
        factors.columns = [int(re.search(r'\d+', i).group()) for i in factors.columns]
        factors.columns = [f"{{ compcol }}_{i}" for i in factors.columns]
        {% if index %}
        return factors.reset_index()
        {% else %}
        return factors
        {% endif %}
        {% elif output in ['projections', 'projections-long'] %}
        projection = pca.projection
        while isinstance(projection, pd.DataFrame):  # Required for multiindexes
            projection = projection.stack()
        projection = projection.reset_index().rename(columns={0: "{{ projcol }}"})
        return projection[[*['{{ "', '".join(col)  }}'], *['{{ "', '".join(index)  }}'], "{{ projcol }}"]]
        {% elif output in ['projections-wide', 'projections-untransformed-wide'] %}
        {% if output == 'projections-wide' %}
        projection = pca.projection.reset_index()
        {% else %}
        projection = pca.project(transform=False).reset_index()
        {% endif %}
        {% if index %}
        return projection[[*['{{ "', '".join(index)  }}'], *['{{ "', '".join(columns)  }}']]]
        {% else %}
        return projection[['{{ "', '".join(columns)  }}']]
        {% endif %}
        {% endif %}
$$;
{%- endmacro %}


{% macro snowflake__drop_pca_udf(pca_num) %}
  {% if 'compiled_code' not in model %}
    {{ return('select 1 /* dbt_pca.snowflake__drop_pca_udf(pca_num='~pca_num~') */;') }}
  {% endif %}
  {% set pca_config = dbt_pca.retrieve_injected_config(pca_num) %}
  {% set final_suffix = '__dbt_pca_'~(pca_num | string).zfill(3)~'_final' %}
  {% if pca_config["table"]["cte"] %}
    {% set input_relation = pca_config["table"]["identifier"] %}
  {% else %}
    {% set input_relation = adapter.get_relation(
      database=pca_config["table"]["database"],
      schema=pca_config["table"]["schema"],
      identifier=pca_config["table"]["identifier"]
    ) %}
  {% endif %}

  {% set arg_data = dbt_pca._get_udtf_function_signature_data(
    table=input_relation,
    index=pca_config.get("index"),
    columns=pca_config.get("columns"),
    values=pca_config.get("values"),
    materialization_options=pca_config.get("materialization_options")
  ) %}

  {% set types_list = [] %}
  {% for o in arg_data %}
    {% do types_list.append(o['type']) %}
  {% endfor %}

  {% set drop_statement -%}
    drop function if exists {{ dbt_pca._get_udtf_name(
      materialization_options=pca_config.get("materialization_options"),
      pca_num=pca_num) }}({{ ', '.join(types_list) }});
  {%- endset %}

  {{ return(drop_statement) }}
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
      {% if "::" in i %}
        {% do _final.append(i) %}
      {% else %}
        {% do _final.append(i ~ '::' ~ _type_maps[i]) %}
      {% endif %}
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
      and table.identifier is not undefined
      and
      (
        (index or [] | length) != (index_types or [] | length)
        or (columns | length) != (column_types or [] | length)
        or (values is not none) != (values_type is not none)
      )
      and dbt_pca._get_materialization_option('infer_function_signature_types', materialization_options, true)
  %}
    {# In this case, not all types are explicitly defined, so we want to attempt
       querying the database for the types. #}
    {% set rel_columns = adapter.get_columns_in_relation(table) %}
    {% for c in rel_columns %}
      {% if c.numeric_precision and c.numeric_scale %}
        {% set d = c.dtype ~ '(' ~ c.numeric_precision ~ ', ' ~ c.numeric_scale ~ ')' %}
      {% else %}
        {% set d = c.dtype %}
      {% endif %}
      {% do rel_columns_mapping.update({c.column.lower(): d}) %}
      {% do rel_columns_mapping.update({c.column: d}) %}
    {% endfor %}
  {% endif %}
  {% set li = [] %}
  {% for i in (index or []) %}
    {% if "::" in i %}
      {% set _i = i.split("::")[0] %}
      {% set typ = i.split("::")[1] %}
    {% else %}
      {% set _i = i %}
      {% if index_types %}
        {% set typ = index_types[loop.index-1] %}
      {% elif _i in rel_columns_mapping %}
        {% set typ = rel_columns_mapping.get(_i) %}
      {% elif _i.lower() in rel_columns_mapping %}
        {% set typ = rel_columns_mapping.get(_i.lower()) %}
      {% else %}
        {% set typ = 'varchar' %}
      {% endif %}
    {% endif %}
    {% do li.append({'col': _i, 'type': typ}) %}
  {% endfor %}
  {% for i in columns %}
    {% if "::" in i %}
      {% set _i = i.split("::")[0] %}
      {% set typ = i.split("::")[1] %}
    {% else %}
      {% set _i = i %}
      {% if column_types %}
        {% set typ = column_types[loop.index-1] %}
      {% elif _i in rel_columns_mapping %}
        {% set typ = rel_columns_mapping.get(_i) %}
      {% elif _i.lower() in rel_columns_mapping %}
        {% set typ = rel_columns_mapping.get(_i.lower()) %}
      {% elif values is none %}
        {% set typ = 'float' %}
      {% else %}
        {% set typ = 'varchar' %}
      {% endif %}
    {% endif %}
    {% do li.append({'col': _i, 'type': typ}) %}
  {% endfor %}
  {% if values is not none %}
    {% if "::" in values %}
      {% set _values = values.split("::")[0] %}
      {% set typ = values.split("::")[1] %}
    {% else %}
      {% set _values = values %}
      {% if values_type %}
        {% set typ = values_type %}
      {% elif values in rel_columns_mapping %}
        {% set typ = rel_columns_mapping.get(_values) %}
      {% elif values.lower() in rel_columns_mapping %}
        {% set typ = rel_columns_mapping.get(_values.lower()) %}
      {% else %}
        {% set typ = 'float' %}
      {% endif %}
    {% endif %}
    {% do li.append({'col': _values, 'type': typ}) %}
  {% endif %}
  {{ return(li) }}
{% endmacro %}

{% macro _get_udtf_name(materialization_options, pca_num) %}
  {% set udf_database = dbt_pca._get_materialization_option('udf_database', materialization_options, model.database) %}
  {% set udf_schema = dbt_pca._get_materialization_option('udf_schema', materialization_options, model.schema) %}
  {% if udf_database and udf_schema %}
    {{ return(
      adapter.quote_as_configured(udf_database, 'identifier')
      ~ '.' ~
      adapter.quote_as_configured(udf_schema, 'identifier')
      ~ '.' ~
      adapter.quote_as_configured(model.name~'__dbt_pca_udtf_'~(pca_num | string).zfill(3), 'identifier')
    ) }}
  {% elif udf_schema %}
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

{% macro _get_udtf_return_signature(table, index, columns, values, output, ncomp, materialization_options, output_options) %}
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
  {% if table.identifier is not undefined and dbt_pca._get_materialization_option('infer_function_signature_types', materialization_options, true)
  %}
    {# In this case, not all types are explicitly defined, so we want to attempt
       querying the database for the types. #}
    {% set rel_columns = adapter.get_columns_in_relation(table) %}
    {% for c in rel_columns %}
      {% if c.numeric_precision and c.numeric_scale %}
        {% set d = c.dtype ~ '(' ~ c.numeric_precision ~ ', ' ~ c.numeric_scale ~ ')' %}
      {% else %}
        {% set d = c.dtype %}
      {% endif %}
      {% do rel_columns_mapping.update({c.column.lower(): d}) %}
      {% do rel_columns_mapping.update({c.column: d}) %}
    {% endfor %}
  {% endif %}
  {% set comp_li = [] %}
  {% set columns_li = [] %}
  {% set index_li = [] %}
  {% set other_li = [] %}
  {% if output in ['loadings', 'loadings-long', 'eigenvectors-wide-transposed', 'coefficients-wide-transposed', 'factors', 'factors-long'] %}
    {% do comp_li.append(dbt_pca._get_output_option("component_column_name", output_options, "comp") ~ ' integer') %}
  {% endif %}
  {% if requires_columns_as_pk and values is none %}
    {% do columns_li.append(dbt_pca._get_output_option("column_column_name", output_options, "col") ~ ' varchar') %}
  {% elif requires_columns_as_pk and values is not none %}
    {% for i in columns %}
      {% if "::" in i %}
        {% set _i = i.split("::")[0] %}
        {% set typ = i.split("::")[1] %}
      {% else %}
        {% set _i = i %}
        {% if column_types %}
          {% set typ = column_types[loop.index-1] %}
        {% elif _i in rel_columns_mapping %}
          {% set typ = rel_columns_mapping.get(_i) %}
        {% elif _i.lower() in rel_columns_mapping %}
          {% set typ = rel_columns_mapping.get(_i.lower()) %}
        {% elif values is none %}
          {% set typ = 'float' %}
        {% else %}
          {% set typ = 'varchar' %}
        {% endif %}
      {% endif %}
      {% do columns_li.append(_i ~ ' ' ~ typ) %}
    {% endfor %}
  {% elif output in ['eigenvectors-wide-transposed', 'coefficients-wide-transposed', 'projections-wide', 'projections-untransformed-wide'] %}
    {% for i in columns %}
      {% do columns_li.append(i ~ ' float') %}
    {% endfor %}
  {% endif %}

  {% if output in ['factors', 'factors-long', 'factors-wide', 'projections', 'projections-long', 'projections-wide', 'projections-untransformed-wide'] %}
    {% for i in (index or []) %}
      {% if "::" in i %}
        {% set _i = i.split("::")[0] %}
        {% set typ = i.split("::")[1] %}
      {% else %}
        {% set _i = i %}
        {% if index_types %}
          {% set typ = index_types[loop.index-1] %}
        {% elif _i in rel_columns_mapping %}
          {% set typ = rel_columns_mapping.get(_i) %}
        {% elif _i.lower() in rel_columns_mapping %}
          {% set typ = rel_columns_mapping.get(_i.lower()) %}
        {% else %}
          {% set typ = 'varchar' %}
        {% endif %}
      {% endif %}
      {% do index_li.append(_i ~ ' ' ~ typ) %}
    {% endfor %}
  {% endif %}

  {% if output in ['loadings', 'loadings-long'] %}
    {% if dbt_pca._get_output_option("display_eigenvalues", output_options, true) %}
      {% do other_li.append(dbt_pca._get_output_option("eigenvector_column_name", output_options, "eigenvector") ~ ' float') %}
    {% endif %}
    {% if dbt_pca._get_output_option("display_eigenvectors", output_options, true) %}
      {% do other_li.append(dbt_pca._get_output_option("eigenvalue_column_name", output_options, "eigenvalue") ~ ' float') %}
    {% endif %}
    {% if dbt_pca._get_output_option("display_coefficients", output_options, true) %}
      {% do other_li.append(dbt_pca._get_output_option("coefficient_column_name", output_options, "coefficient") ~ ' float') %}
    {% endif %}
  {% elif output in ['loadings-wide', 'eigenvectors-wide', 'coefficients-wide'] %}
    {% if dbt_pca._get_output_option("display_eigenvectors", output_options, true) %}
      {% for i in range(ncomp) %}
        {% do other_li.append(dbt_pca._get_output_option("eigenvector_column_name", output_options, "eigenvector") ~'_'~i ~ ' float') %}
      {% endfor %}
    {% endif %}
    {% if dbt_pca._get_output_option("display_coefficients", output_options, true) %}
      {% for i in range(ncomp) %}
        {% do other_li.append(dbt_pca._get_output_option("coefficient_column_name", output_options, "coefficient") ~'_'~i ~ ' float') %}
      {% endfor %}
    {% endif %}
  {% elif output in ['factors', 'factors-long'] %}
    {% do other_li.append(dbt_pca._get_output_option("factor_column_name", output_options, "factor") ~ ' float') %}
  {% elif output in ['factors-wide'] %}
    {% for i in range(ncomp) %}
      {% do other_li.append(dbt_pca._get_output_option("factor_column_name", output_options, "factor") ~'_'~i ~ ' float') %}
    {% endfor %}
  {% elif output in ['projections', 'projections-long'] %}
    {% do other_li.append(dbt_pca._get_output_option("projection_column_name", output_options, "projection") ~ ' float') %}
  {% endif %}

  {% if output in ['projections-wide', 'projections-untransformed-wide'] %}
    {{ return(', '.join(index_li + columns_li)) }}
  {% else %}
    {{ return(', '.join(comp_li + columns_li + index_li + other_li)) }}
  {% endif %}

{% endmacro %}
