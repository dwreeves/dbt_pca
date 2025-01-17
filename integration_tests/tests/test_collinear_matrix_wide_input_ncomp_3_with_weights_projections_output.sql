with expected as (
  select
    0 as idx,
    'x1' as col,
    1.509286998872265 as projection
  union all
  select
    0 as idx,
    'x2' as col,
    1.0480915470334327 as projection
  union all
  select
    0 as idx,
    'x3' as col,
    5.169030344652458 as projection
  union all
  select
    0 as idx,
    'x4' as col,
    1.4952477194423204 as projection
  union all
  select
    0 as idx,
    'x5' as col,
    2.3362198731536 as projection
  union all
  select
    1 as idx,
    'x1' as col,
    2.4638078018982204 as projection
  union all
  select
    1 as idx,
    'x2' as col,
    1.3162162083803173 as projection
  union all
  select
    1 as idx,
    'x3' as col,
    5.732706854575864 as projection
  union all
  select
    1 as idx,
    'x4' as col,
    2.740800119592682 as projection
  union all
  select
    1 as idx,
    'x5' as col,
    4.374069039660291 as projection
  union all
  select
    2 as idx,
    'x1' as col,
    3.7598946766216805 as projection
  union all
  select
    2 as idx,
    'x2' as col,
    -4.3787045198228105 as projection
  union all
  select
    2 as idx,
    'x3' as col,
    -5.8422135627212075 as projection
  union all
  select
    2 as idx,
    'x4' as col,
    -9.282369586843034 as projection
  union all
  select
    2 as idx,
    'x5' as col,
    4.675091000220832 as projection
  union all
  select
    3 as idx,
    'x1' as col,
    1.4540705718084155 as projection
  union all
  select
    3 as idx,
    'x2' as col,
    -3.525193249300339 as projection
  union all
  select
    3 as idx,
    'x3' as col,
    -4.161271428129353 as projection
  union all
  select
    3 as idx,
    'x4' as col,
    -9.187635932663666 as projection
  union all
  select
    3 as idx,
    'x5' as col,
    3.328691940274597 as projection
  union all
  select
    4 as idx,
    'x1' as col,
    1.4679790818171612 as projection
  union all
  select
    4 as idx,
    'x2' as col,
    -1.2700079130742954 as projection
  union all
  select
    4 as idx,
    'x3' as col,
    0.4383163707097802 as projection
  union all
  select
    4 as idx,
    'x4' as col,
    -3.9510926100466763 as projection
  union all
  select
    4 as idx,
    'x5' as col,
    3.0390665169752893 as projection
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
  from {{ ref("collinear_matrix_wide_input_ncomp_3_with_weights_projections_output") }}
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
