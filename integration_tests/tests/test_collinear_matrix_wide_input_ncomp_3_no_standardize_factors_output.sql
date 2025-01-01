with expected as (
  select
    0 as idx,
    0 as comp,
    0.0062978886701229985 as factor
  union all
  select
    0 as idx,
    1 as comp,
    -0.005196433370652831 as factor
  union all
  select
    0 as idx,
    2 as comp,
    -0.01580631689471048 as factor
  union all
  select
    1 as idx,
    0 as comp,
    0.008553516767201945 as factor
  union all
  select
    1 as idx,
    1 as comp,
    0.007217435235178837 as factor
  union all
  select
    1 as idx,
    2 as comp,
    0.0037117903746461452 as factor
  union all
  select
    2 as idx,
    0 as comp,
    -0.010588064192087361 as factor
  union all
  select
    2 as idx,
    1 as comp,
    -0.010478579391848817 as factor
  union all
  select
    2 as idx,
    2 as comp,
    0.005732907480966119 as factor
  union all
  select
    3 as idx,
    0 as comp,
    -0.008094038943685902 as factor
  union all
  select
    3 as idx,
    1 as comp,
    0.006796579542596352 as factor
  union all
  select
    3 as idx,
    2 as comp,
    -0.006058616882433404 as factor
  union all
  select
    4 as idx,
    0 as comp,
    -0.0006884599955045926 as factor
  union all
  select
    4 as idx,
    1 as comp,
    0.0005434691460537505 as factor
  union all
  select
    4 as idx,
    2 as comp,
    -0.008993939395910299 as factor
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
  from {{ ref("collinear_matrix_wide_input_ncomp_3_no_standardize_factors_output") }}
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
