with expected as (
  select
    0 as idx,
    'x1' as col,
    0.9525945622115313 as projection
  union all
  select
    0 as idx,
    'x2' as col,
    1.6764823604386097 as projection
  union all
  select
    0 as idx,
    'x3' as col,
    5.342069034989084 as projection
  union all
  select
    0 as idx,
    'x4' as col,
    0.31621067273772235 as projection
  union all
  select
    0 as idx,
    'x5' as col,
    2.605856188372686 as projection
  union all
  select
    1 as idx,
    'x1' as col,
    2.2150879349763346 as projection
  union all
  select
    1 as idx,
    'x2' as col,
    0.8557555110697068 as projection
  union all
  select
    1 as idx,
    'x3' as col,
    5.046736452432577 as projection
  union all
  select
    1 as idx,
    'x4' as col,
    4.083456073993432 as projection
  union all
  select
    1 as idx,
    'x5' as col,
    4.563071578220776 as projection
  union all
  select
    2 as idx,
    'x1' as col,
    2.640798937563051 as projection
  union all
  select
    2 as idx,
    'x2' as col,
    -3.685721903516807 as projection
  union all
  select
    2 as idx,
    'x3' as col,
    -3.727955220469059 as projection
  union all
  select
    2 as idx,
    'x4' as col,
    -12.272951556073394 as projection
  union all
  select
    2 as idx,
    'x5' as col,
    5.16440452001238 as projection
  union all
  select
    3 as idx,
    'x1' as col,
    1.9437792852626041 as projection
  union all
  select
    3 as idx,
    'x2' as col,
    -3.7758582197329567 as projection
  union all
  select
    3 as idx,
    'x3' as col,
    -5.356600907734263 as projection
  union all
  select
    3 as idx,
    'x4' as col,
    -7.380584931462119 as projection
  union all
  select
    3 as idx,
    'x5' as col,
    3.234378884389776 as projection
  union all
  select
    4 as idx,
    'x1' as col,
    1.581244482454955 as projection
  union all
  select
    4 as idx,
    'x2' as col,
    -1.037459973991235 as projection
  union all
  select
    4 as idx,
    'x3' as col,
    0.21597621372890577 as projection
  union all
  select
    4 as idx,
    'x4' as col,
    -3.4458844752347635 as projection
  union all
  select
    4 as idx,
    'x5' as col,
    3.1934087644263065 as projection
)

select
  coalesce(a.col, e.col) as col,
  coalesce(a.idx, e.idx) as idx,
  e.projection as expected_projection,
  a.projection as actual_projection
from expected as e
full outer join (
  /* Only test first 5 rows of each factor. */
  select *
  from {{ ref("collinear_matrix_wide_input_ncomp_3_no_demean_projections_output") }}
  where idx <= 4
) as a
on a.col = e.col
and a.idx = e.idx
where
  least(
    abs(expected_projection - actual_projection),
    abs(expected_projection + actual_projection)
  ) > {{ var('test_precision') }}
  or a.col is null
  or e.col is null
  or a.idx is null
  or e.idx is null
