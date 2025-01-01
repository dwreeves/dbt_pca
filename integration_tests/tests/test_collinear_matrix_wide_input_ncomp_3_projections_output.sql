with expected as (
  select
    0 as idx,
    'x1' as col,
    1.4963383965789152 as projection
  union all
  select
    0 as idx,
    'x2' as col,
    1.1711349275797134 as projection
  union all
  select
    0 as idx,
    'x3' as col,
    5.41227613740505 as projection
  union all
  select
    0 as idx,
    'x4' as col,
    1.5921895853543906 as projection
  union all
  select
    0 as idx,
    'x5' as col,
    2.333346002808246 as projection
  union all
  select
    1 as idx,
    'x1' as col,
    2.481462125185497 as projection
  union all
  select
    1 as idx,
    'x2' as col,
    1.2270716362627736 as projection
  union all
  select
    1 as idx,
    'x3' as col,
    5.5468687194576445 as projection
  union all
  select
    1 as idx,
    'x4' as col,
    2.433033263517012 as projection
  union all
  select
    1 as idx,
    'x5' as col,
    4.376855822014036 as projection
  union all
  select
    2 as idx,
    'x1' as col,
    3.7201556890843346 as projection
  union all
  select
    2 as idx,
    'x2' as col,
    -4.173191131771575 as projection
  union all
  select
    2 as idx,
    'x3' as col,
    -5.4137167007565425 as projection
  union all
  select
    2 as idx,
    'x4' as col,
    -8.590909022226088 as projection
  union all
  select
    2 as idx,
    'x5' as col,
    4.668757710898932 as projection
  union all
  select
    3 as idx,
    'x1' as col,
    1.4748983671226572 as projection
  union all
  select
    3 as idx,
    'x2' as col,
    -3.691834720490476 as projection
  union all
  select
    3 as idx,
    'x3' as col,
    -4.495254508202602 as projection
  union all
  select
    3 as idx,
    'x4' as col,
    -9.421011830150396 as projection
  union all
  select
    3 as idx,
    'x5' as col,
    3.3328562803692865 as projection
  union all
  select
    4 as idx,
    'x1' as col,
    1.4745833984074825 as projection
  union all
  select
    4 as idx,
    'x2' as col,
    -1.308782686835862 as projection
  union all
  select
    4 as idx,
    'x3' as col,
    0.35864586737054394 as projection
  union all
  select
    4 as idx,
    'x4' as col,
    -4.050438318408194 as projection
  union all
  select
    4 as idx,
    'x5' as col,
    3.0401923041889196 as projection
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
  from {{ ref("collinear_matrix_wide_input_ncomp_3_projections_output") }}
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
