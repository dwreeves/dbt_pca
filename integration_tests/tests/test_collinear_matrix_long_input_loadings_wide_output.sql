with expected as (
  select
    'x1' as my_columns_column,
    0.23766556798297242 as eigenvector_0,
    -0.06624103790399177 as eigenvector_1,
    0.9637584106354821 as eigenvector_2,
    40.65483565495506 as coefficient_0,
    -6.626101262204105 as coefficient_1,
    91.02677502779223 as coefficient_2
  union all
  select
    'x2' as my_columns_column,
    -0.571096344052245 as eigenvector_0,
    -0.001674522205010131 as eigenvector_1,
    0.09591634897052516 as eigenvector_2,
    -97.69117254819653 as coefficient_0,
    -0.1675027150433251 as coefficient_1,
    9.059278573216492 as coefficient_2
  union all
  select
    'x3' as my_columns_column,
    -0.5730425037096113 as eigenvector_0,
    -0.0027280890356712506 as eigenvector_1,
    0.10093335006878709 as eigenvector_2,
    -98.02408068335475 as coefficient_0,
    -0.27289116799266117 as coefficient_1,
    9.533133250120997 as coefficient_2
  union all
  select
    'x4' as my_columns_column,
    -0.5375734787462785 as eigenvector_0,
    -0.022663548693591457 as eigenvector_1,
    0.2167293369790332 as eigenvector_2,
    -91.95678455390825 as coefficient_0,
    -2.2670382795372994 as coefficient_1,
    20.470039359868917 as coefficient_2
  union all
  select
    'x5' as my_columns_column,
    0.001042830923792024 as eigenvector_0,
    0.9975410978819939 as eigenvector_1,
    0.06935872883822335 as eigenvector_2,
    0.17838562052752704 as coefficient_0,
    99.78419023802815 as coefficient_1,
    6.55091705192758 as coefficient_2
)

select
  coalesce(a.my_columns_column, e.my_columns_column) as my_columns_column,
  e.eigenvector_0 as expected_eigenvector_0,
  a.eigenvector_0 as actual_eigenvector_0,
  e.eigenvector_1 as expected_eigenvector_1,
  a.eigenvector_1 as actual_eigenvector_1,
  e.eigenvector_2 as expected_eigenvector_2,
  a.eigenvector_2 as actual_eigenvector_2,
  e.coefficient_0 as expected_coefficient_0,
  a.coefficient_0 as actual_coefficient_0,
  e.coefficient_1 as expected_coefficient_1,
  a.coefficient_1 as actual_coefficient_1,
  e.coefficient_2 as expected_coefficient_2,
  a.coefficient_2 as actual_coefficient_2
from expected as e
full outer join {{ ref("collinear_matrix_long_input_loadings_wide_output") }} as a
on a.my_columns_column = e.my_columns_column
where
  least(
    abs(expected_eigenvector_0 - actual_eigenvector_0),
    abs(expected_eigenvector_0 + actual_eigenvector_0)
  ) > {{ var('test_precision') }}
  or
  least(
    abs(expected_eigenvector_1 - actual_eigenvector_1),
    abs(expected_eigenvector_1 + actual_eigenvector_1)
  ) > {{ var('test_precision') }}
  or
  least(
    abs(expected_eigenvector_2 - actual_eigenvector_2),
    abs(expected_eigenvector_2 + actual_eigenvector_2)
  ) > {{ var('test_precision') }}
  or
  least(
    abs(expected_coefficient_0 - actual_coefficient_0),
    abs(expected_coefficient_0 + actual_coefficient_0)
  ) > {{ var('test_precision') }} * abs(expected_coefficient_0) * 100
  or
  least(
    abs(expected_coefficient_1 - actual_coefficient_1),
    abs(expected_coefficient_1 + actual_coefficient_1)
  ) > {{ var('test_precision') }} * abs(expected_coefficient_1) * 100
  or
  least(
    abs(expected_coefficient_2 - actual_coefficient_2),
    abs(expected_coefficient_2 + actual_coefficient_2)
  ) > {{ var('test_precision') }} * abs(expected_coefficient_2) * 100
  or a.my_columns_column is null
  or e.my_columns_column is null
