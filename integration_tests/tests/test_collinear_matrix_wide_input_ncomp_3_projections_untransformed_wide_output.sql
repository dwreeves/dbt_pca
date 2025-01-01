with expected as (
  select
    0 as idx,
    -0.5097369046714606 as x1,
    0.7079178257816718 as x2,
    0.7104644163049084 as x3,
    0.6588780207698892 as x4,
    -1.5697548711072453 as x5
  union all
  select
    1 as idx,
    0.4764095691716541 as x1,
    0.7257146415534536 as x2,
    0.7316066152073619 as x3,
    0.7774177212378808 as x4,
    0.3506727981942011 as x5
  union all
  select
    2 as idx,
    1.716389131879792 as x1,
    -0.9924322402459105 as x2,
    -0.9901144002268192 as x3,
    -0.776705655635126 as x4,
    0.6249932144052557 as x5
  union all
  select
    3 as idx,
    -0.5311991929439068 as x1,
    -0.8392839486530943 as x2,
    -0.8458396559339205 as x3,
    -0.8937311394016286 as x4,
    -0.6304458573999372 as x5
  union all
  select
    4 as idx,
    -0.5315144886563343 as x1,
    -0.0810924315614829 as x2,
    -0.08337469963372691 as x3,
    -0.1366032784722577 as x4,
    -0.9054824598093117 as x5
  union all
  select
    5 as idx,
    0.6238367980233702 as x1,
    0.42950757517226423 as x2,
    0.4344737350835343 as x3,
    0.5011367932601557 as x4,
    0.3928397413700163 as x5
  union all
  select
    6 as idx,
    -0.3997972261144227 as x1,
    -0.5667016720333423 as x2,
    -0.5721495077235446 as x3,
    -0.6229198641554805 as x4,
    0.701891897827482 as x5
  union all
  select
    7 as idx,
    -2.1514181143143936 as x1,
    -0.02951321423291583 as x2,
    -0.040205584674398255 as x3,
    -0.30864312382535936 as x4,
    0.5057959685233036 as x5
  union all
  select
    8 as idx,
    1.1437275831892897 as x1,
    -1.711225582998196 as x2,
    -1.7146825917156083 as x3,
    -1.5502494953047454 as x4,
    -0.4545687448088823 as x5
  union all
  select
    9 as idx,
    0.15683571197666116 as x1,
    -0.5059577473026348 as x2,
    -0.5083362248338547 as x3,
    -0.48996858696892864 as x4,
    0.5469935318211454 as x5
)

select
  coalesce(a.idx, e.idx) as idx,
  e.x1 as expected_x1,
  a.x1 as actual_x1,
  e.x2 as expected_x2,
  a.x2 as actual_x2,
  e.x3 as expected_x3,
  a.x3 as actual_x3,
  e.x4 as expected_x4,
  a.x4 as actual_x4,
  e.x5 as expected_x5,
  a.x5 as actual_x5
from expected as e
full outer join (
  /* Only test first 10 rows. */
  select *
  from {{ ref("collinear_matrix_wide_input_ncomp_3_projections_untransformed_wide_output") }}
  where idx <= 9
) as a
on a.idx = e.idx
where
  least(
    abs(expected_x1 - actual_x1),
    abs(expected_x1 + actual_x1)
  ) > {{ var('test_precision') }}
  or
  least(
    abs(expected_x2 - actual_x2),
    abs(expected_x2 + actual_x2)
  ) > {{ var('test_precision') }}
  or
  least(
    abs(expected_x3 - actual_x3),
    abs(expected_x3 + actual_x3)
  ) > {{ var('test_precision') }}
  or
  least(
    abs(expected_x4 - actual_x4),
    abs(expected_x4 + actual_x4)
  ) > {{ var('test_precision') }}
  or
  least(
    abs(expected_x5 - actual_x5),
    abs(expected_x5 + actual_x5)
  ) > {{ var('test_precision') }}
  or a.idx is null
  or e.idx is null
