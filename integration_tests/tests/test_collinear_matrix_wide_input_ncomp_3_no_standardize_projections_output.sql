with expected as (
  select
    0 as idx,
    'x1' as col,
    1.728156240189143 as projection
  union all
  select
    0 as idx,
    'x2' as col,
    1.2711594842537006 as projection
  union all
  select
    0 as idx,
    'x3' as col,
    5.617259117648726 as projection
  union all
  select
    0 as idx,
    'x4' as col,
    0.2769547849621685 as projection
  union all
  select
    0 as idx,
    'x5' as col,
    2.3335364113014774 as projection
  union all
  select
    1 as idx,
    'x1' as col,
    1.9487601038122075 as projection
  union all
  select
    1 as idx,
    'x2' as col,
    0.9241695628820894 as projection
  union all
  select
    1 as idx,
    'x3' as col,
    4.9846161389938155 as projection
  union all
  select
    1 as idx,
    'x4' as col,
    4.0992779351344115 as projection
  union all
  select
    1 as idx,
    'x5' as col,
    4.375152161865144 as projection
  union all
  select
    2 as idx,
    'x1' as col,
    2.030625422902731 as projection
  union all
  select
    2 as idx,
    'x2' as col,
    -3.3778870702075587 as projection
  union all
  select
    2 as idx,
    'x3' as col,
    -3.938744221793905 as projection
  union all
  select
    2 as idx,
    'x4' as col,
    -12.242339467878361 as projection
  union all
  select
    2 as idx,
    'x5' as col,
    4.64663457638999 as projection
  union all
  select
    3 as idx,
    'x1' as col,
    2.386283576017011 as projection
  union all
  select
    3 as idx,
    'x2' as col,
    -4.04564568378753 as projection
  union all
  select
    3 as idx,
    'x3' as col,
    -5.184647109360288 as projection
  union all
  select
    3 as idx,
    'x4' as col,
    -7.398999098369696 as projection
  union all
  select
    3 as idx,
    'x5' as col,
    3.3445886502521116 as projection
  union all
  select
    4 as idx,
    'x1' as col,
    2.0439911317148303 as projection
  union all
  select
    4 as idx,
    'x2' as col,
    -1.3035522085610285 as projection
  union all
  select
    4 as idx,
    'x3' as col,
    0.38519076184392764 as projection
  union all
  select
    4 as idx,
    'x4' as col,
    -3.4623342537953463 as projection
  union all
  select
    4 as idx,
    'x5' as col,
    3.0458623947556056 as projection
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
  from {{ ref("collinear_matrix_wide_input_ncomp_3_no_standardize_projections_output") }}
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
