with expected as (
  select
    0 as idx,
    'x1' as col,
    1.5196289696664378 as projection
  union all
  select
    0 as idx,
    'x2' as col,
    1.7776241397640717 as projection
  union all
  select
    0 as idx,
    'x3' as col,
    5.344825751623116 as projection
  union all
  select
    0 as idx,
    'x4' as col,
    0.2912977596049946 as projection
  union all
  select
    0 as idx,
    'x5' as col,
    2.330873575435126 as projection
  union all
  select
    1 as idx,
    'x1' as col,
    2.4524505137246586 as projection
  union all
  select
    1 as idx,
    'x2' as col,
    0.7056070231752565 as projection
  union all
  select
    1 as idx,
    'x3' as col,
    5.141447447741078 as projection
  union all
  select
    1 as idx,
    'x4' as col,
    4.073780684183442 as projection
  union all
  select
    1 as idx,
    'x5' as col,
    4.37991403241838 as projection
  union all
  select
    2 as idx,
    'x1' as col,
    3.7856447736364034 as projection
  union all
  select
    2 as idx,
    'x2' as col,
    -3.332524364683374 as projection
  union all
  select
    2 as idx,
    'x3' as col,
    -3.794805888899784 as projection
  union all
  select
    2 as idx,
    'x4' as col,
    -12.323833912325854 as projection
  union all
  select
    2 as idx,
    'x5' as col,
    4.661885256835711 as projection
  union all
  select
    3 as idx,
    'x1' as col,
    1.4385159561858445 as projection
  union all
  select
    3 as idx,
    'x2' as col,
    -4.2696910635279135 as projection
  union all
  select
    3 as idx,
    'x3' as col,
    -5.162836954911533 as projection
  union all
  select
    3 as idx,
    'x4' as col,
    -7.356806157358939 as projection
  union all
  select
    3 as idx,
    'x5' as col,
    3.3366844674656937 as projection
  union all
  select
    4 as idx,
    'x1' as col,
    1.4635423006698223 as projection
  union all
  select
    4 as idx,
    'x2' as col,
    -1.6499799661939711 as projection
  union all
  select
    4 as idx,
    'x3' as col,
    0.5029113246322254 as projection
  union all
  select
    4 as idx,
    'x4' as col,
    -3.438398409497844 as projection
  union all
  select
    4 as idx,
    'x5' as col,
    3.0413693211194985 as projection
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
  from {{ ref("collinear_matrix_wide_input_projections_output") }}
  where idx <= 4
) as a
on a.col = e.col
and a.idx = e.idx
where
  abs(expected_projection - actual_projection) > {{ var('test_precision') }} * greatest(abs(expected_projection), 1)
  or a.col is null
  or e.col is null
  or a.idx is null
  or e.idx is null
