{% macro duckdb___pca_nipals(table,
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
                             method_options) %}
{% set long = ((values is not none) | as_bool) %}
{%- set cols = dbt_pca._alias_columns_to_list(columns) if long else ['col'] %}
{%- set idx = dbt_pca._alias_index_to_list(index) if index else ['idx'] %}
{% set check_tol = dbt_pca._get_method_option("nipals", "check_tol", method_options, true) %}
{% set tol = dbt_pca._get_method_option("nipals", "tol", method_options, 5e-8) %}
{% if not (1 >= tol >= 0) %}
    {{ exceptions.raise_compiler_error(
      "method_options['tol'] must be between 0 and 1"
    ) }}
{% endif %}
{% set max_iter = dbt_pca._get_method_option("nipals", "max_iter", method_options, 500) %}
{% if max_iter < 0 %}
    {{ exceptions.raise_compiler_error(
      "method_options['max_iter'] must be an integer ≥ 0."
    ) }}
{% endif %}
{%- if long %}
  {%- set ncomp_limit -%}
    (select count(*) from (select distinct {{ cols | join(', ') }} from dbt_pca_preproc_step2))
  {%- endset %}
{%- else %}
  {%- set ncomp_limit = (columns | length) %}
{%- endif %}
(with recursive

{{ dbt_pca.preproc_step_1_cte(
  table=table,
  columns=columns,
  index=index,
  values=values,
  long=long,
  output_options=output_options
) }},

{{ dbt_pca.preproc_step_2_cte(
  cols=cols,
  idx=idx,
  standardize=standardize,
  demean=demean
) }},

dbt_pca_comps_combined as materialized (

  with recursive

  dbt_pca_calc_comp as (

    with

    dbt_pca_initial_column as (

        {#- Snowflake and Postgres/Redshift handle this correctly.
            DuckDB can be forced to handle this correctly with "materialized" keyword
            Clickhouse does not handle this correctly. #}
        {%-if not dbt_pca._get_method_option("nipals", "deterministic_column_seeding", method_options, adapter.type() == "clickhouse") %}
        select {{ cols | join(', ') }}
        from (
          select
            {{ cols | join(', ') }},
            var_pop(x) as variance
          from dbt_pca_comps_combined
          group by {{ cols | join(', ') }}
        )
        order by variance desc
        limit 1
        {%-else %}
        select {{ cols | join(', ') }}
        from dbt_pca_comps_combined
        order by {{ cols | join(', ') }} desc
        limit 1
        {%- endif %}

    ),

    dbt_pca_factor_t0 as (

      select
        {{ dbt_pca._list_with_alias(idx, 'c') }},
        c.x
      from dbt_pca_comps_combined as c
      inner join dbt_pca_initial_column as a
      on {{ dbt_pca._join_predicate(cols, 'c', 'a') }}

    ),

    dbt_pca_vec_calc as (

      select
        {{ cols | join(', ') }},
        sum(x * factor) / sum(factor * factor) as vec
      from dbt_pca_calc_comp
      where _iter = (select max(_iter) from dbt_pca_calc_comp)
      group by {{ cols | join(', ') }}

    ),

    dbt_pca_vec_normalized_calc as (

      select
        {{ cols | join(', ') }},
        vec / sqrt(sum(vec * vec) over ()) as eigenvector
      from dbt_pca_vec_calc

    ),

    dbt_pca_factor_calc as (

      select
        {{ dbt_pca._list_with_alias(idx, 'd') }},
        {{ dbt_pca._list_with_alias(cols, 'd') }},
        v.eigenvector,
        sum(d.x * v.eigenvector) over (partition by {{ dbt_pca._list_with_alias(idx, 'd') }}) as factor,
        d.factor as factor_last
      from dbt_pca_vec_normalized_calc as v
      inner join dbt_pca_calc_comp as d
      on {{ dbt_pca._join_predicate(cols, 'v', 'd') }}
      where d._iter = (select max(_iter) from dbt_pca_calc_comp)

    ){% if check_tol %},

    tolerance_check as (

      select sqrt(sum(pow(c.factor - c.factor_last, 2))) / sqrt(sum(pow(c.factor, 2))) > {{ tol }} as above_tol
      from dbt_pca_factor_calc as c
      inner join dbt_pca_initial_column as a
      on {{ dbt_pca._join_predicate(cols, 'c', 'a') }}

    ){% endif %}

    select
      {{ dbt_pca._list_with_alias(idx, 'd') }},
      {{ dbt_pca._list_with_alias(cols, 'd') }},
      j.x as factor,
      null::double as eigenvector,

      d.x,
      0 as _iter
    from dbt_pca_comps_combined as d
    inner join dbt_pca_factor_t0 as j
    on {{ dbt_pca._join_predicate(idx, 'd', 'j') }}

    union all

    select
      {{ dbt_pca._list_with_alias(idx, 'cc') }},
      {{ dbt_pca._list_with_alias(cols, 'cc') }},
      f.factor,
      f.eigenvector,

      cc.x,
      cc._iter + 1 as _iter

    from dbt_pca_calc_comp as cc
    inner join dbt_pca_factor_calc as f
    on
      {{ dbt_pca._join_predicate(cols, 'cc', 'f') }}
      and
      {{ dbt_pca._join_predicate(idx, 'cc', 'f') }}
    where cc._iter = (select max(_iter) from dbt_pca_calc_comp)
    and cc._iter < {{ max_iter }}
    {% if check_tol %}
    and (select above_tol from tolerance_check limit 1)
    {% endif %}

  )

  select
    -1 as comp,
    {{ cols | join(', ') }},
    {{ idx | join(', ') }},
    x as factor,
    0::double as eigenvector,
    x,
    0 as _iter
  from dbt_pca_preproc_step2

  union all

  select
    (select max(comp) from dbt_pca_comps_combined) + 1 as comp,
    {{ cols | join(', ') }},
    {{ idx | join(', ') }},
    factor,
    eigenvector,
    x - factor * eigenvector as x,
    _iter
  from dbt_pca_calc_comp
  {%- if ncomp is none %}
  where comp <= 5 - 1
  {%- else %}
  where comp <= least({{ ncomp }}, {{ ncomp_limit }}) - 1
  {%- endif %}
  qualify _iter = max(_iter) over ()

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

)
{% endmacro %}
