{###############################################################################
## Preprocessing
###############################################################################}

{% macro __alias_cols_to_list(cols, identifier) %}
  {% set li = [] %}
  {% for c in cols %}
    {% do li.append(identifier ~ ( loop.index | string )) %}
  {% endfor %}
  {{ return(li) }}
{% endmacro %}

{% macro __alias_cols(cols, identifier) %}
{%- for c in cols %}
{{ c }} as {{ identifier }}{{ loop.index }}
{%- if not loop.last %},{%- endif %}
{%- endfor %}
{%- endmacro %}

{% macro __unalias_cols(cols, identifier, prefix=none) %}
{%- for c in cols %}
{%- if prefix %}
{{ prefix }}.{{ identifier }}{{ loop.index }} as {{ c }}
{%- else %}
{{ identifier }}{{ loop.index }} as {{ c }}
{%- endif %}
{%- if not loop.last %},{%- endif %}
{%- endfor %}
{%- endmacro %}

{% macro _alias_index_to_list(cols) %}
  {{ return(dbt_pca.__alias_cols_to_list(cols, identifier='idx')) }}
{% endmacro %}

{% macro _unalias_index(cols, prefix=none) %}
  {{ return(dbt_pca.__unalias_cols(cols, identifier='idx', prefix=prefix)) }}
{% endmacro %}

{% macro _alias_columns_to_list(cols) %}
  {{ return(dbt_pca.__alias_cols_to_list(cols, identifier='col')) }}
{% endmacro %}

{% macro _unalias_columns(cols, prefix=none) %}
  {{ return(dbt_pca.__unalias_cols(cols, identifier='col', prefix=prefix)) }}
{% endmacro %}

{% macro preproc_step_1_cte(table,
                            columns,
                            index,
                            values,
                            weights,
                            long,
                            output_options

) %}
{% set tbl = table %}
{% if not long and index is none %}
dbt_pca_preproc_add_idx as (
  select *, row_number() over (order by {{ columns | join(', ') }}) as __dbt_pca_rownum
  from {{ table }}
),
{% set tbl = 'dbt_pca_preproc_add_idx' %}
{% endif %}
{% if not long and missing == 'drop-row' %}
dbt_pca_preproc_missing_drop_row as (
  select *
  from {{ tbl }}
  where
  {%- for i in index %}
    {{ i }} is not null
    {% if not loop.last %}and{% endif %}
  {%- endfor %}
),
{% set tbl = 'dbt_pca_preproc_missing_drop_row' %}
{% endif %}
dbt_pca_preproc_step1 as (
  {% if not long %}
  {#- if the data is wide natively, it needs to be coverted into a long format. #}
  {% for c in columns %}
  select
    {% if index is not none %}
    {{ dbt_pca.__alias_cols(index, 'idx') }},
    {% else %}
    __dbt_pca_rownum as idx,
    {% endif %}
    {{ dbt.string_literal(dbt_pca._strip_quotes(c, output_options)) }} as col,
    {% if weights %}
    {{ weights[loop.index-1] }} as w,
    {% endif %}
    {% if missing == 'zero' %}
    coalesce({{ c }}, 0) as x
    {% else %}
    {{ c }} as x
    {% endif %}
  from {{ tbl }}
  {% if missing == 'drop-col' %}
  qualify min({{ c }} is not null) over ()
  {% endif %}
  {% if not loop.last %}
  union all
  {% endif %}
  {% endfor %}
  {% else %}
  select
    {{ dbt_pca.__alias_cols(index, 'idx') }},
    {{ dbt_pca.__alias_cols(columns, 'col') }},
    {{ values }} as x
  from {{ tbl }}
  {%- endif %}
)
{% endmacro %}

{% macro preproc_step_2_cte(cols,
                            idx,
                            standardize,
                            demean,
                            weights,
                            include_iter=false
) %}
{% set _w = ' / sqrt(p.w)' if weights else '' %}
dbt_pca_preproc_step2 as (
  select
    {{ dbt_pca._list_with_alias(idx, 'p') }},
    {{ dbt_pca._list_with_alias(cols, 'p') }},
    {%- if standardize %}
    avg(p.x) over (partition by {{ dbt_pca._list_with_alias(cols, 'p') }}) as mu,
    stddev_pop(p.x) over (partition by {{ dbt_pca._list_with_alias(cols, 'p') }}) as sigma,
    (p.x - avg(p.x) over (partition by {{ dbt_pca._list_with_alias(cols, 'p') }})) / stddev_pop(p.x) over (partition by {{ dbt_pca._list_with_alias(cols, 'p') }}){{ _w }} as x
    {%- elif demean %}
    avg(p.x) over (partition by {{ dbt_pca._list_with_alias(cols, 'p') }}) as mu,
    (x - avg(p.x) over (partition by {{ dbt_pca._list_with_alias(cols, 'p') }})){{ _w }} as x
    {%- else %}
    p.x{{ _w }} as x
    {%- endif %}
    {%- if weights %},
    p.w as w
    {% endif %}
    {%- if include_iter %},
    -1 as _iter
    {%- endif %}
  from dbt_pca_preproc_step1 as p
)
{% endmacro %}

{###############################################################################
## Postprocessing
###############################################################################}

{% macro final_output(index,
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
                      method_options) %}
{% set long = ((values is not none) | as_bool) %}
{%- set cols = dbt_pca._alias_columns_to_list(columns) if long else ['col'] %}
{%- set idx = dbt_pca._alias_index_to_list(index) if index else ['idx'] %}
dbt_pca_eigenvectors as (

  select
      c.comp,
      {{ dbt_pca._list_with_alias(cols, 'c') }},
      any_value(c.eigenvector) as eigenvector,
      sum(c.factor * c.factor) as eigenvalue,
      {%- if normalize %}
      any_value(c.eigenvector) * sqrt(sum(c.factor * c.factor)) as coefficient
      {%- else %}
      any_value(c.eigenvector) as coefficient
      {%- endif %}
  from dbt_pca_comps_combined as c
  where c.comp >= 0
  group by c.comp, {{ dbt_pca._list_with_alias(cols, 'c') }}

){% if output in ['loadings', 'loadings-long'] %}

select
  e.comp as {{ dbt_pca._get_output_option("component_column_name", output_options, "comp") }},
  {{ dbt_pca._unalias_columns(columns, prefix='e')
     if long
     else "e.col as " ~ dbt_pca._get_output_option("columns_column_name", output_options, "col") }},
  e.eigenvector as {{ dbt_pca._get_output_option("eigenvector_column_name", output_options, "eigenvector") }},
  e.eigenvalue as {{ dbt_pca._get_output_option("eigenvalue_column_name", output_options, "eigenvalue") }},
  e.coefficient as {{ dbt_pca._get_output_option("coefficient_column_name", output_options, "coefficient") }}
from dbt_pca_eigenvectors as e
order by e.comp, {{ dbt_pca._list_with_alias(cols, 'e') }}
{% elif output == 'loadings-wide' %}

select
  {{ dbt_pca._unalias_columns(columns, prefix='e')
     if long
     else "e.col as " ~ dbt_pca._get_output_option("columns_column_name", output_options, "col") }},
  {%- if dbt_pca._get_output_option("display_eigenvectors", output_options, true) %}
  {%- for i in range(ncomp) %}
  max(case when e.comp = {{ i }} then e.eigenvector end) as {{ dbt_pca._get_output_option("eigenvector_column_name", output_options, "eigenvector") ~'_'~i }}{% if not loop.last or dbt_pca._get_output_option("display_eigenvalues", output_options, true) or dbt_pca._get_output_option("display_coefficients", output_options, true) %},{% endif %}
  {%- endfor %}
  {%- endif %}
  {%- if dbt_pca._get_output_option("display_coefficients", output_options, true) %}
  {%- for i in range(ncomp) %}
  max(case when e.comp = {{ i }} then e.coefficient end) as {{ dbt_pca._get_output_option("eigenvalue_column_name", output_options, "coefficient") ~'_'~i }}{% if not loop.last %},{% endif %}
  {%- endfor %}
  {%- endif %}
from dbt_pca_eigenvectors as e
group by {{ dbt_pca._list_with_alias(cols, 'e') }}
order by {{ dbt_pca._list_with_alias(cols, 'e') }}
{% elif output in ['eigenvectors-wide-transposed', 'coefficients-wide-transposed'] %}

select
  e.comp as {{ dbt_pca._get_output_option("component_column_name", output_options, "comp") }},
  {%- for c in columns %}
  max(case when e.col = {{ dbt.string_literal(c) }} then {{ 'e.eigenvector' if output == 'eigenvectors-wide-transposed' else 'e.coefficient'}} end) as {{ c }}{% if not loop.last %},{% endif %}
  {%- endfor %}
from dbt_pca_eigenvectors as e
group by e.comp
order by e.comp
{% else %},
{# The remainder of these are factors/projections, so they involve the following CTE #}

dbt_pca_factors as (
  select
    {{ dbt_pca._list_with_alias(idx, 'p') }},
    e.comp,
    {%- if normalize %}
    sum(p.x * e.eigenvector / sqrt(e.eigenvalue)) as factor
    {%- else %}
    sum(p.x * e.coefficient) as factor
    {%- endif %}
  from dbt_pca_preproc_step2 as p
  inner join dbt_pca_eigenvectors as e
  on {{ dbt_pca._join_predicate(cols, 'p', 'e') }}
  group by e.comp, {{ dbt_pca._list_with_alias(idx, 'p') }}

)
{%- if output in ['factors', 'factors-long'] %}

select
  f.comp as {{ dbt_pca._get_output_option("component_column_name", output_options, "comp") }},
  {{ dbt_pca._unalias_index(index, prefix='f') }},
  f.factor as {{ dbt_pca._get_output_option("factor_column_name", output_options, "factor") }}
from dbt_pca_factors as f
order by f.comp, {{ dbt_pca._list_with_alias(idx, 'f') }}
{%- elif output == 'factors-wide' %}

select
  {{ dbt_pca._unalias_index(index, prefix='f') }},
  {%- for i in range(ncomp) %}
  max(case when f.comp = {{ i }} then f.factor end) as {{ dbt_pca._get_output_option("factor_column_name", output_options, "factor") ~'_'~i }}{% if not loop.last %},{% endif %}
  {%- endfor %}
from dbt_pca_factors as f
group by {{ dbt_pca._list_with_alias(idx, 'f') }}
order by {{ dbt_pca._list_with_alias(idx, 'f') }}
{% else %},
{# The remainder of these are projections, so they involve the following CTE #}
{% set _w = ' * sqrt(any_value(p.w))' if weights else '' %}
dbt_pca_projections as (

  select
    {{ dbt_pca._list_with_alias(cols, 'e', add_as=true) }},
    {{ dbt_pca._list_with_alias(idx, 'f', add_as=true) }},
    {%- if output != "projections-untransformed-wide" %}
    {%- if standardize %}
    sum(e.coefficient * f.factor) * any_value(p.sigma){{ _w }} + any_value(p.mu) as projection
    {%- elif demean %}
    sum(e.coefficient * f.factor){{ _w }} + any_value(p.mu) as projection
    {%- else %}
    sum(e.coefficient * f.factor){{ _w }} as projection
    {%- endif %}
    {%- endif %}{%- if dbt_pca._get_output_option("display_untransformed_projection", output_options, false) or output == "projections-untransformed-wide" %}{% if output != "projections-untransformed-wide" %},{% endif %}
    sum(e.coefficient * f.factor) as projection_untransformed
    {%- endif %}
  from dbt_pca_factors as f
  inner join dbt_pca_eigenvectors as e
  on f.comp = e.comp
  {%- if standardize or demean or weights %}
  inner join dbt_pca_preproc_step2 as p
  on {{ dbt_pca._join_predicate(cols, 'e', 'p') }}
    and {{ dbt_pca._join_predicate(idx, 'f', 'p') }}
  {%- endif %}
  group by
    {{ dbt_pca._list_with_alias(cols, 'e') }},
    {{ dbt_pca._list_with_alias(idx, 'f') }}

)
{%- if output in ['projections', 'projections-long'] %}
select
  {{ dbt_pca._unalias_columns(columns, prefix='p')
     if long
     else "p.col as " ~ dbt_pca._get_output_option("columns_column_name", output_options, "col") }},
  {{ dbt_pca._unalias_index(index, prefix='p') }},
  p.projection as {{ dbt_pca._get_output_option("projection_column_name", output_options, "projection") }}{% if dbt_pca._get_output_option("display_untransformed_projection", output_options, false) %},
  p.projection_untransformed as {{ dbt_pca._get_output_option("projection_column_name", output_options, "projection") }}_untransformed
  {%- endif %}
from dbt_pca_projections as p
order by {{ dbt_pca._list_with_alias(cols, 'p') }}, {{ dbt_pca._list_with_alias(idx, 'p') }}
{%- elif output in ['projections-wide', 'projections-untransformed-wide'] %}

select
  {{ dbt_pca._unalias_index(index, prefix='p') }},
  {%- for c in columns %}
  {%- if output == "projections-wide" %}
  max(case when p.col = {{ dbt.string_literal(c) }} then p.projection end) as {{ c }}{% if not loop.last %},{% endif %}
  {%- else %}
  max(case when p.col = {{ dbt.string_literal(c) }} then p.projection_untransformed end) as {{ c }}{% if not loop.last %},{% endif %}
  {%- endif %}
  {%- endfor %}
from dbt_pca_projections as p
group by {{ dbt_pca._list_with_alias(idx, 'p') }}
order by {{ dbt_pca._list_with_alias(idx, 'p') }}
{%- else %}
    {{ exceptions.raise_compiler_error(
      "Invalid output method '"~output~"'."
      " This error should not happen if the user is using the public API because it should be handled up-front by `dbt_pca.pca()`."
      " Let the author of this library know if you see this error and you are using the public API!"
    ) }}
{%- endif %}
{%- endif %}
{%- endif %}
{%- endmacro %}

{###############################################################################
## Adapter specific handlers
###############################################################################}

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
## Misc.
###############################################################################}

{# Users can pass columns such as '"foo"', with the double quotes included.
   In this situation, we want to strip the double quotes when presenting
   outputs in a long format. #}
{% macro _strip_quotes(x, output_options) -%}
  {% if output_options.get('strip_quotes') | default(True) %}
    {% if x[0] == '"' and x[-1] == '"' and (x | length) > 1 %}
    {{ return(x[1:-1]) }}
    {% endif %}
  {% endif %}
  {{ return(x)}}
{%- endmacro %}

{% macro _join_predicate(cols, a, b) %}
  {# creates a join predicate on a list of column names and two tables 'a' and 'b'. #}
  {% set li = [] %}
  {% for col in cols %}
    {% do li.append(a~'.'~col~' = '~b~'.'~col) %}
  {% endfor %}
  {{ return(' and '.join(li)) }}
{% endmacro %}

{% macro _list_with_alias(cols, alias, add_as=false) %}
  {% set li = [] %}
  {% for col in cols %}
    {% if add_as %}
      {% do li.append(alias~'.'~col~' as '~col) %}
    {% else %}
      {% do li.append(alias~'.'~col) %}
    {% endif %}
  {% endfor %}
  {{ return(', '.join(li)) }}
{% endmacro %}

{% macro _get_output_option(field, output_options, default=none) %}
  {{ return(output_options.get(field, var("dbt_pca", {}).get("output_options", {}).get(field, default))) }}
{% endmacro %}

{% macro _get_method_option(method, field, method_options, default=none) %}
  {{ return(method_options.get(field, var("dbt_pca", {}).get("method_options", {}).get(method, {}).get(field, default))) }}
{% endmacro %}

{% macro _get_materialization_option(field, method_options, default=none) %}
  {{ return(method_options.get(field, var("dbt_pca", {}).get("materialization_options", {}).get(field, default))) }}
{% endmacro %}
