with expected as (
  select
    0 as idx,
    0 as comp,
    -0.007531873796311788 as factor
  union all
  select
    0 as idx,
    1 as comp,
    -0.015497183329978934 as factor
  union all
  select
    0 as idx,
    2 as comp,
    -0.0033640181060839588 as factor
  union all
  select
    1 as idx,
    0 as comp,
    -0.006652803625498362 as factor
  union all
  select
    1 as idx,
    1 as comp,
    0.00297332846609244 as factor
  union all
  select
    1 as idx,
    2 as comp,
    0.008421475796314328 as factor
  union all
  select
    2 as idx,
    0 as comp,
    0.011459594172715793 as factor
  union all
  select
    2 as idx,
    1 as comp,
    0.0053156660446589 as factor
  union all
  select
    2 as idx,
    2 as comp,
    0.014124672166084809 as factor
  union all
  select
    3 as idx,
    0 as comp,
    0.0077023423457717195 as factor
  union all
  select
    3 as idx,
    1 as comp,
    -0.0056956867103752805 as factor
  union all
  select
    3 as idx,
    2 as comp,
    -0.009690301032865098 as factor
  union all
  select
    4 as idx,
    0 as comp,
    0.00023533504953835392 as factor
  union all
  select
    4 as idx,
    1 as comp,
    -0.008643281012909005 as factor
  union all
  select
    4 as idx,
    2 as comp,
    -0.006573376366082388 as factor
)

select
  coalesce(a.comp, e.comp) as comp,
  coalesce(a.idx, e.idx) as idx,
  e.factor as expected_factor,
  a.factor as actual_factor
from expected as e
full outer join (
  /* Only test first 5 rows of each factor. */
  select *
  from {{ ref("collinear_matrix_wide_input_ncomp_3_factors_output") }}
  where idx <= 4
) as a
on a.comp = e.comp
and a.idx = e.idx
where
  least(
    abs(expected_factor - actual_factor),
    abs(expected_factor + actual_factor)
  ) > {{ var('test_precision') }}
  or a.comp is null
  or e.comp is null
  or a.idx is null
  or e.idx is null
