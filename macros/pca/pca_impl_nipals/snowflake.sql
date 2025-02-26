{% macro snowflake__pre_hook(
    udf_database=none,
    udf_schema=none
) %}
  {% set udf_ddls = [] %}
  {% for row in model['compiled_code'].split("\n") %}
    {% if row.lstrip().startswith("-- !DBT_PCA_CONFIG") %}
      {% set comp_relations = [] %}
      {% set parsed_row = row.strip().replace("-- !DBT_PCA_CONFIG:", "", 1) %}
      {% set pca_num = (parsed_row[:3] | int) %}
      {% set pca_config = fromjson(parsed_row[4:]) %}
      {% if udf_database is not none %}
        {% do pca_config.setdefault("materialization_options", {}) %}
        {% do pca_config["materialization_options"].setdefault("udf_database", udf_database) %}
      {% endif %}
      {% if udf_schema is not none %}
        {% do pca_config.setdefault("materialization_options", {}) %}
        {% do pca_config["materialization_options"].setdefault("udf_schema", udf_schema) %}
      {% endif %}
      {% set udf_relation = adapter.get_relation(
        database=pca_config["table"]["database"],
        schema=pca_config["table"]["schema"],
        identifier=pca_config["table"]["identifier"]
      ) %}
      {% do udf_ddls.append(dbt_pca.create_pca_udtf(
          table_json=pca_config["table"],
          index=pca_config.get('index'),
          columns=pca_config.get('columns'),
          values=pca_config.get('values'),
          ncomp=pca_config.get('ncomp'),
          normalize=pca_config.get('normalize'),
          standardize=pca_config.get('standardize'),
          demean=pca_config.get('demean'),
          missing=pca_config.get('missing'),
          weights=pca_config.get('weights'),
          output=pca_config.get('output'),
          output_options=pca_config.get('output_options'),
          method=pca_config.get('method'),
          method_options=pca_config.get('method_options'),
          materialization_options=pca_config.get('materialization_options'),
          pca_num=pca_num,
      )) %}
    {% endif %}
  {% endfor %}
  {{ return(udf_ddls | join ('\n')) }}
{% endmacro %}

{% macro snowflake__post_hook(
    udf_database=none,
    udf_schema=none
) %}
  {% if 'compiled_code' not in model %}
    {{ return('select 1 /* DBT_PCA POST-HOOK ENABLED */;') }}
  {% endif %}
  {% set udf_ddls = [] %}
  {% for row in model['compiled_code'].split("\n") %}
    {% if row.lstrip().startswith("-- !DBT_PCA_CONFIG") %}
      {% set comp_relations = [] %}
      {% set parsed_row = row.strip().replace("-- !DBT_PCA_CONFIG:", "", 1) %}
      {% set pca_num = (parsed_row[:3] | int) %}
      {% set pca_config = fromjson(parsed_row[4:]) %}
      {% if udf_database is not none %}
        {% do pca_config.setdefault("materialization_options", {}) %}
        {% do pca_config["materialization_options"].setdefault("udf_database", udf_database) %}
      {% endif %}
      {% if udf_schema is not none %}
        {% do pca_config.setdefault("materialization_options", {}) %}
        {% do pca_config["materialization_options"].setdefault("udf_schema", udf_schema) %}
      {% endif %}
      {% if dbt_pca._get_materialization_option('drop_udf', pca_config.get('materialization_options'), true) %}
        {% set udf_relation = adapter.get_relation(
          database=pca_config["table"]["database"],
          schema=pca_config["table"]["schema"],
          identifier=pca_config["table"]["identifier"]
        ) %}
        {% set arg_data = dbt_pca._get_udtf_function_signature_data(
          udf_relation,
          pca_config.get('index'),
          pca_config.get('columns'),
          pca_config.get('values'),
          pca_config.get('materialization_options')
        ) %}
        {% set types_list = [] %}
        {% for o in arg_data %}
          {% do types_list.append(o['type']) %}
        {% endfor %}
        {% set drop_statement -%}
          drop function if exists {{ dbt_pca._get_udtf_name(udf_relation, pca_config.get('materialization_options'), pca_num) }}({{ ', '.join(types_list) }});
        {%- endset %}
        {% do udf_ddls.append(drop_statement) %}
      {% endif %}
    {% endif %}
  {% endfor %}
  {{ return(udf_ddls | join ('\n')) }}
{% endmacro %}
{% macro create_pca_udtf(table,
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
{% if values is none %}
  {% set col = ['col'] %}
{% else %}
  {% set col = columns %}
{% endif %}
create or replace function {{ dbt_pca._get_udtf_name(table, materialization_options, pca_num) }}({{ dbt_pca._get_udtf_function_signature(table, index, columns, values, materialization_options) }})
returns table ({{ dbt_pca._get_udtf_return_signature(table, index, columns, values, output, materialization_options, output_options) }})
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
        {%- endif %}
        # if wide:
        #   df = df.pivot(columns=[], index=[], values="")
        # elif index:  # index is list
        #   df = df.set_index(index)
        #
        # if missing == "zero":  # Then set missing = None
        #   df = df.fillna(0)
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

        {% if output in ['loadings', 'loadings-long', 'loadings-wide', 'eigenvectors-wide', 'eigenvectors-wide-transposed'] %}
        loadings = pca.loadings.copy()
        loadings.columns = pd.Index([int(re.search(r'\d+', i).group()) for i in loadings.columns], name="comp")
        loadings = loadings.stack().reset_index().rename(columns={0: "eigenvector"})
        {% endif %}
        {% if output in ['loadings', 'loadings-long', 'loadings-wide', 'coefficients-wide', 'coefficients-wide-transposed'] %}
        coeff = pca.coeff.copy()
        coeff.index = pd.Index([int(re.search(r'\d+', i).group()) for i in coeff.index], name="comp")
        while isinstance(coeff, pd.DataFrame):  # Required for multiindexes
            coeff = coeff.stack()
        coeff = coeff.reset_index().rename(columns={0: "coefficient"})
        {% endif %}

        {# ########## Return ########## #}

        {% if output in ['loadings', 'loadings-long'] %}
        eigenvals = pca.eigenvals.rename("eigenvalue")
        loadings = loadings.merge(right=coeff, left_on=["comp", *['{{ "', '".join(col)  }}']], right_on=["comp", *['{{ "', '".join(col)  }}']])
        res = loadings.merge(right=eigenvals, left_on="comp", right_index=True).sort_values(["comp", *['{{ "', '".join(col)  }}']])[["comp", *['{{ "', '".join(columns)  }}'], "eigenvector", "eigenvalue", "coefficient"]].reset_index(drop=True)
        return res
        {% elif output == 'loadings-wide' %}
        loadings = loadings.merge(right=coeff, left_on=["comp", *['{{ "', '".join(col)  }}']], right_on=["comp", *['{{ "', '".join(col)  }}']])
        loadings = loadings.pivot(index=['{{ "', '".join(col)  }}'], columns=["comp"], values=["eigenvector", "coefficient"])
        loadings.columns = [f"{c[0]}_{c[1]}" for c in loadings.columns]
        return loadings.reset_index()
        {% elif output == 'eigenvectors-wide' %}
        loadings = loadings.pivot(index=['{{ "', '".join(col)  }}'], columns=["comp"], values="eigenvector")
        loadings.columns = [f"eigenvector_{c}" for c in loadings.columns]
        return loadings.reset_index()
        {% elif output == 'eigenvectors-wide-transposed' %}
        {# This will be wide format, so it is guaranteed to not be multi-indexed. #}
        loadings = loadings.pivot(columns=['{{ "', '".join(col)  }}'], index=["comp"], values="eigenvector")
        return loadings.reset_index()
        {% elif output == 'coefficients-wide' %}
        coeff = coeff.pivot(index=['{{ "', '".join(col)  }}'], columns=["comp"], values="coefficient")
        coeff.columns = [f"coefficient_{c}" for c in loadings.columns]
        return coeff.reset_index()
        {% elif output == 'eigenvectors-wide-transposed' %}
        {# This will be wide format, so it is guaranteed to not be multi-indexed. #}
        coeff = coeff.pivot(columns=['{{ "', '".join(col)  }}'], index=["comp"], values="coefficient")
        return loadings.reset_index()
        {% endif %}
$$;
{%- endmacro %}
