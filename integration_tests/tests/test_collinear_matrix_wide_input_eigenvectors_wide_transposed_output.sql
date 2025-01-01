with expected as (
  select
    0 as comp,
    0.23766556798297242 as x1,
    -0.571096344052245 as x2,
    -0.5730425037096113 as x3,
    -0.5375734787462785 as x4,
    0.001042830923792024 as x5
  union all
  select
    1 as comp,
    -0.06624103790399177 as x1,
    -0.001674522205010131 as x2,
    -0.0027280890356712506 as x3,
    -0.022663548693591457 as x4,
    0.9975410978819939 as x5
  union all
  select
    2 as comp,
    0.9637584106354821 as x1,
    0.09591634897052516 as x2,
    0.10093335006878709 as x3,
    0.2167293369790332 as x4,
    0.06935872883822335 as x5
  union all
  select
    3 as comp,
    -0.10146854110613553 as x1,
    -0.43256328404685196 as x2,
    -0.37441907450265255 as x3,
    0.8138202918794208 as x4,
    0.010001481603560571 as x5
  union all
  select
    4 as comp,
    0.0010314009380258666 as x1,
    -0.6910392326636107 as x2,
    0.7219679092348236 as x3,
    -0.03501493291669362 as x4,
    8.740555082534684e-05 as x5
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
full outer join {{ ref("collinear_matrix_wide_input_eigenvectors_wide_transposed_output") }} as a
on a.comp = e.comp
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
  or a.comp is null
  or e.comp is null
