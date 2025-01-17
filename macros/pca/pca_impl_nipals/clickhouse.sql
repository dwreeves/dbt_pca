{% macro clickhouse___pca_nipals(table,
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
{% set check_tol = dbt_pca._get_method_option("nipals", "check_tol", method_options, false) %}
{% set tol = dbt_pca._get_method_option("nipals", "tol", method_options, 5e-7) %}
{% if not (1 >= tol >= 0) %}
    {{ exceptions.raise_compiler_error(
      "method_options['tol'] must be between 0 and 1. "
    ) }}
{% endif %}
{% set deterministic_column_seeding = dbt_pca._get_method_option("nipals", "deterministic_column_seeding", method_options, false) %}
{% set max_iter = dbt_pca._get_method_option("nipals", "max_iter", method_options, 100) %}
{% if max_iter < 0 %}
    {{ exceptions.raise_compiler_error(
      "method_options['max_iter'] must be an integer ≥ 0. "
    ) }}
{% endif %}
{%- if ncomp is none %}
    {{ exceptions.raise_compiler_error(
      "Something went wrong and ncomp should not be null here."
    ) }}
{%- endif %}
(with recursive

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

{%- for compnum in range(ncomp) %}
{%- set previous = 'dbt_pca_preproc_step2' if compnum == 0 else 'dbt_pca_comp_'~(compnum-1) %}
{{ dbt_pca._pca_nipals_single_iteration(
  previous=previous,
  idx=idx,
  cols=cols,
  compnum=compnum,
  check_tol=check_tol,
  tol=tol,
  max_iter=max_iter,
  deterministic_column_seeding=deterministic_column_seeding,
  _first=loop.first
) }},
{%- endfor %}

dbt_pca_comps_combined as (

  {%- for compnum in range(ncomp) %}
  select {{ compnum }} as comp, * from dbt_pca_comp_{{ compnum }}
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
)
{% endmacro %}

{% macro clickhouse___pca_nipals_single_iteration(previous,
                                                  idx,
                                                  cols,
                                                  compnum,
                                                  check_tol,
                                                  tol,
                                                  max_iter,
                                                  deterministic_column_seeding,
                                                  _first=true) %}
dbt_pca_initial_column_{{ compnum }} as (

    {#- Snowflake and Postgres/Redshift handle this correctly.
        DuckDB can be forced to handle this correctly with "materialized" keyword
        Clickhouse does not handle this correctly. #}
    {%-if not deterministic_column_seeding %}
    select {{ cols | join(', ') }}
    from (
      select
        {{ cols | join(', ') }},
        var_pop(x) as variance
      from {{ previous }}
      group by {{ cols | join(', ') }}
    ) as v
    order by v.variance desc
    limit 1
    {%-else %}
    select {{ cols | join(', ') }}
    from {{ previous }}
    order by {{ cols | join(', ') }} desc
    limit 1
    {%- endif %}

),

dbt_pca_factor_t0_{{ compnum }} as (

  select
    {{ dbt_pca._list_with_alias(idx, 'c') }},
    {%- if not _first %}
    c.x - c.factor * c.eigenvector as factor,
    {%- endif %}
    c.x
  from {{ previous }} as c
  inner join dbt_pca_initial_column_{{ compnum }} as a
  on {{ dbt_pca._join_predicate(cols, 'c', 'a') }}

),

dbt_pca_comp_{{ compnum }} as (

with recursive _dbt_pca_calc_comp_{{ compnum }} as (

  select
    {{ dbt_pca._list_with_alias(idx, 'd') }},
    {{ dbt_pca._list_with_alias(cols, 'd') }},
    {%- if _first %}
    d.x as factor,
    {%- else %}
    j.factor as factor,
    {%- endif %}
    -- null::double as eigenvector,
    0.0::float as factor_last,
    0.0::float as eigenvector,

    d.x,
    0 as _iter
  from {{ previous }} as d
  inner join dbt_pca_factor_t0_{{ compnum }} as j
  on {{ dbt_pca._join_predicate(idx, 'd', 'j') }}

  union all

  select
    {{ dbt_pca._list_with_alias(idx, 'cc', add_as=true) }},
    {{ dbt_pca._list_with_alias(cols, 'cc', add_as=true) }},
    f.factor as factor,
    f.factor_last as factor_last,
    f.eigenvector as eigenvector,

    cc.x as x,
    cc._iter + 1 as _iter

  from _dbt_pca_calc_comp_{{ compnum }} as cc
  inner join (

    select
      {{ dbt_pca._list_with_alias(idx, 'd') }},
      {{ dbt_pca._list_with_alias(cols, 'd', add_as=true) }},
      v.eigenvector,
      sum(d.x * v.eigenvector) over (partition by {{ dbt_pca._list_with_alias(idx, 'd') }}) as factor,
      d.factor as factor_last
    from (

      select
        {{ cols | join(', ') }},
        vec / sqrt(sum(vec * vec) over ()) as eigenvector
      from (

        select
          {{ dbt_pca._list_with_alias(cols, 'd', add_as=true) }},
          sum(d.x * d.factor) / sum(d.factor * d.factor) as vec
        from (
          select *
          from _dbt_pca_calc_comp_{{ compnum }}
          qualify _iter = max(_iter) over ()
        ) as d
--           from _dbt_pca_calc_comp_{{ compnum }} as d
--           inner join (
--             select max(_iter) as _max_iter
--             from _dbt_pca_calc_comp_{{ compnum }}
--           ) as m
--           on d._iter = m._max_iter
        group by {{ cols | join(', ') }}
        having max(_iter) < {{ max_iter }}
      )
    ) as v
    inner join (
      select
        {{ cols | join(', ') }},
        {{ idx | join(', ') }},
        x, factor
      from _dbt_pca_calc_comp_{{ compnum }}
      qualify _iter = max(_iter) over ()
--         select d.*
--         from _dbt_pca_calc_comp_{{ compnum }} as d
--         inner join (
--           select max(_iter) as _max_iter
--           from _dbt_pca_calc_comp_{{ compnum }}
--         ) as m
--         on d._iter = m._max_iter
    ) as d
    on {{ dbt_pca._join_predicate(cols, 'v', 'd') }}

  ) as f
  on
    {{ dbt_pca._join_predicate(cols, 'cc', 'f') }}
    and
    {{ dbt_pca._join_predicate(idx, 'cc', 'f') }}
  {% if check_tol %}
  inner join (
    select sqrt(sum(pow(c.factor - c.factor_last, 2))) / sqrt(sum(pow(c.factor, 2))) > {{ tol }} as tol_check
    from (
      select {{ cols | join(', ') }}, factor, factor_last, _iter
      from _dbt_pca_calc_comp_{{ compnum }}
      qualify _iter = max(_iter) over ()
    ) as c
    inner join dbt_pca_initial_column_{{ compnum }} as a
    on {{ dbt_pca._join_predicate(cols, 'c', 'a') }}
--       inner join (
--         select max(_iter) as _max_iter
--         from _dbt_pca_calc_comp_{{ compnum }}
--       ) as m
--       on c._iter = m._max_iter
  ) as fl
  on true
  {% endif %}
  where cc._iter < {{ max_iter }}
  {% if check_tol %}
    and coalesce(fl.tol_check, true)
  {% endif %}
)
  select
      {{ dbt_pca._list_with_alias(idx, 'd') }},
      {{ dbt_pca._list_with_alias(cols, 'd') }},
      d.factor,
      d.eigenvector,
      d.x - d.factor * d.eigenvector as x,
      d._iter
  from _dbt_pca_calc_comp_{{ compnum }} as d
  qualify _iter = max(_iter) over ()

)
{% endmacro %}
