with expected as (
  select
    0 as comp,
    'x1' as my_columns_column,
    0.23766556798297242 as eigenvector,
    29261.18329671326 as eigenvalue
  union all
  select
    0 as comp,
    'x2' as my_columns_column,
    -0.571096344052245 as eigenvector,
    29261.18329671326 as eigenvalue
  union all
  select
    0 as comp,
    'x3' as my_columns_column,
    -0.5730425037096113 as eigenvector,
    29261.18329671326 as eigenvalue
  union all
  select
    0 as comp,
    'x4' as my_columns_column,
    -0.5375734787462785 as eigenvector,
    29261.18329671326 as eigenvalue
  union all
  select
    0 as comp,
    'x5' as my_columns_column,
    0.001042830923792024 as eigenvector,
    29261.18329671326 as eigenvalue
  union all
  select
    1 as comp,
    'x1' as my_columns_column,
    -0.06624103790399177 as eigenvector,
    10006.031828705976 as eigenvalue
  union all
  select
    1 as comp,
    'x2' as my_columns_column,
    -0.001674522205010131 as eigenvector,
    10006.031828705976 as eigenvalue
  union all
  select
    1 as comp,
    'x3' as my_columns_column,
    -0.0027280890356712506 as eigenvector,
    10006.031828705976 as eigenvalue
  union all
  select
    1 as comp,
    'x4' as my_columns_column,
    -0.022663548693591457 as eigenvector,
    10006.031828705976 as eigenvalue
  union all
  select
    1 as comp,
    'x5' as my_columns_column,
    0.9975410978819939 as eigenvector,
    10006.031828705976 as eigenvalue
  union all
  select
    2 as comp,
    'x1' as my_columns_column,
    0.9637584106354821 as eigenvector,
    8920.76195540782 as eigenvalue
  union all
  select
    2 as comp,
    'x2' as my_columns_column,
    0.09591634897052516 as eigenvector,
    8920.76195540782 as eigenvalue
  union all
  select
    2 as comp,
    'x3' as my_columns_column,
    0.10093335006878709 as eigenvector,
    8920.76195540782 as eigenvalue
  union all
  select
    2 as comp,
    'x4' as my_columns_column,
    0.2167293369790332 as eigenvector,
    8920.76195540782 as eigenvalue
  union all
  select
    2 as comp,
    'x5' as my_columns_column,
    0.06935872883822335 as eigenvector,
    8920.76195540782 as eigenvalue
  union all
  select
    3 as comp,
    'x1' as my_columns_column,
    -0.10146854110613553 as eigenvector,
    1690.522183075302 as eigenvalue
  union all
  select
    3 as comp,
    'x2' as my_columns_column,
    -0.43256328404685196 as eigenvector,
    1690.522183075302 as eigenvalue
  union all
  select
    3 as comp,
    'x3' as my_columns_column,
    -0.37441907450265255 as eigenvector,
    1690.522183075302 as eigenvalue
  union all
  select
    3 as comp,
    'x4' as my_columns_column,
    0.8138202918794208 as eigenvector,
    1690.522183075302 as eigenvalue
  union all
  select
    3 as comp,
    'x5' as my_columns_column,
    0.010001481603560571 as eigenvector,
    1690.522183075302 as eigenvalue
  union all
  select
    4 as comp,
    'x1' as my_columns_column,
    0.0010314009380258666 as eigenvector,
    121.50073609764209 as eigenvalue
  union all
  select
    4 as comp,
    'x2' as my_columns_column,
    -0.6910392326636107 as eigenvector,
    121.50073609764209 as eigenvalue
  union all
  select
    4 as comp,
    'x3' as my_columns_column,
    0.7219679092348236 as eigenvector,
    121.50073609764209 as eigenvalue
  union all
  select
    4 as comp,
    'x4' as my_columns_column,
    -0.03501493291669362 as eigenvector,
    121.50073609764209 as eigenvalue
  union all
  select
    4 as comp,
    'x5' as my_columns_column,
    8.740555082534684e-05 as eigenvector,
    121.50073609764209 as eigenvalue
)

select
  coalesce(a.comp, e.comp) as comp,
  coalesce(a.my_columns_column, e.my_columns_column) as my_columns_column,
  e.eigenvector as expected_eigenvector,
  a.eigenvector as actual_eigenvector,
  e.eigenvalue as expected_eigenvalue,
  a.eigenvalue as actual_eigenvalue
from expected as e
full outer join {{ ref("collinear_matrix_long_input") }} as a
on a.comp = e.comp
and a.my_columns_column = e.my_columns_column
where
  least(
    abs(expected_eigenvector - actual_eigenvector),
    abs(expected_eigenvector + actual_eigenvector)
  ) > {{ var('test_precision') }}
  or abs(expected_eigenvalue - actual_eigenvalue) > expected_eigenvalue * {{ var('test_precision') }}
  or a.comp is null
  or e.comp is null
  or a.my_columns_column is null
  or e.my_columns_column is null
