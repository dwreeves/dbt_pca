with expected as (
  select
    0 as comp,
    'x1' as col,
    0.23766556798297242 as eigenvector,
    29261.18329671326 as eigenvalue
  union all
  select
    0 as comp,
    'x2' as col,
    -0.571096344052245 as eigenvector,
    29261.18329671326 as eigenvalue
  union all
  select
    0 as comp,
    'x3' as col,
    -0.5730425037096113 as eigenvector,
    29261.18329671326 as eigenvalue
  union all
  select
    0 as comp,
    'x4' as col,
    -0.5375734787462785 as eigenvector,
    29261.18329671326 as eigenvalue
  union all
  select
    0 as comp,
    'x5' as col,
    0.001042830923792024 as eigenvector,
    29261.18329671326 as eigenvalue
  union all
  select
    1 as comp,
    'x1' as col,
    -0.06624103790399177 as eigenvector,
    10006.031828705976 as eigenvalue
  union all
  select
    1 as comp,
    'x2' as col,
    -0.001674522205010131 as eigenvector,
    10006.031828705976 as eigenvalue
  union all
  select
    1 as comp,
    'x3' as col,
    -0.0027280890356712506 as eigenvector,
    10006.031828705976 as eigenvalue
  union all
  select
    1 as comp,
    'x4' as col,
    -0.022663548693591457 as eigenvector,
    10006.031828705976 as eigenvalue
  union all
  select
    1 as comp,
    'x5' as col,
    0.9975410978819939 as eigenvector,
    10006.031828705976 as eigenvalue
  union all
  select
    2 as comp,
    'x1' as col,
    0.9637584106354821 as eigenvector,
    8920.76195540782 as eigenvalue
  union all
  select
    2 as comp,
    'x2' as col,
    0.09591634897052516 as eigenvector,
    8920.76195540782 as eigenvalue
  union all
  select
    2 as comp,
    'x3' as col,
    0.10093335006878709 as eigenvector,
    8920.76195540782 as eigenvalue
  union all
  select
    2 as comp,
    'x4' as col,
    0.2167293369790332 as eigenvector,
    8920.76195540782 as eigenvalue
  union all
  select
    2 as comp,
    'x5' as col,
    0.06935872883822335 as eigenvector,
    8920.76195540782 as eigenvalue
)

select
  coalesce(a.comp, e.comp) as comp,
  coalesce(a.col, e.col) as col,
  e.eigenvector as expected_eigenvector,
  a.eigenvector as actual_eigenvector,
  e.eigenvalue as expected_eigenvalue,
  a.eigenvalue as actual_eigenvalue
from expected as e
full outer join {{ ref("collinear_matrix_wide_input_ncomp_3") }} as a
on a.comp = e.comp
and a.col = e.col
where
  least(
    abs(expected_eigenvector - actual_eigenvector),
    abs(expected_eigenvector + actual_eigenvector)
  ) > {{ var('test_precision') }}
  or abs(expected_eigenvalue - actual_eigenvalue) > {{ var('test_precision') }} * greatest(abs(expected_eigenvalue), 1)
  or expected_eigenvector is null
  or actual_eigenvector is null
  or expected_eigenvalue is null
  or actual_eigenvalue is null
