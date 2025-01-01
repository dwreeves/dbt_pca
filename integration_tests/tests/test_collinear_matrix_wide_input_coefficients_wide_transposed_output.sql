with expected as (
  select
    0 as comp,
    40.65483565495506 as x1,
    -97.69117254819653 as x2,
    -98.02408068335475 as x3,
    -91.95678455390825 as x4,
    0.17838562052752704 as x5
  union all
  select
    1 as comp,
    -6.626101262204105 as x1,
    -0.1675027150433251 as x2,
    -0.27289116799266117 as x3,
    -2.2670382795372994 as x4,
    99.78419023802815 as x5
  union all
  select
    2 as comp,
    91.02677502779223 as x1,
    9.059278573216492 as x2,
    9.533133250120997 as x3,
    20.470039359868917 as x4,
    6.55091705192758 as x5
  union all
  select
    3 as comp,
    -4.171976497557569 as x1,
    -17.785254770364126 as x2,
    -15.394599764949897 as x3,
    33.461002729949406 as x4,
    0.41122051954175615 as x5
  union all
  select
    4 as comp,
    0.011368861521255267 as x1,
    -7.617143879027633 as x2,
    7.958062553822355 as x3,
    -0.3859604047557586 as x4,
    0.0009634484194132912 as x5
)

select
  coalesce(a.comp, e.comp) as comp,
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
full outer join {{ ref("collinear_matrix_wide_input_coefficients_wide_transposed_output") }} as a
on a.comp = e.comp
where
  least(
    abs(expected_x1 - actual_x1),
    abs(expected_x1 + actual_x1)
  ) > {{ var('test_precision') }} * greatest(abs(expected_x1), 1) * 10
  or
  least(
    abs(expected_x2 - actual_x2),
    abs(expected_x2 + actual_x2)
  ) > {{ var('test_precision') }} * greatest(abs(expected_x2), 1) * 10
  or
  least(
    abs(expected_x3 - actual_x3),
    abs(expected_x3 + actual_x3)
  ) > {{ var('test_precision') }} * greatest(abs(expected_x3), 1) * 10
  or
  least(
    abs(expected_x4 - actual_x4),
    abs(expected_x4 + actual_x4)
  ) > {{ var('test_precision') }} * greatest(abs(expected_x4), 1) * 10
  or
  least(
    abs(expected_x5 - actual_x5),
    abs(expected_x5 + actual_x5)
  ) > {{ var('test_precision') }} * greatest(abs(expected_x5), 1) * 10
  or a.comp is null
  or e.comp is null
