/* This is testing whether two calls to pca() is properly supported. */
with expected as (
  select
    1 as i,
    0 as comp,
    'x1' as my_columns_column,
    0.23766556798297242 as eigenvector,
    29261.18329671326 as eigenvalue
  union all
  select
    1 as i,
    0 as comp,
    'x2' as my_columns_column,
    -0.571096344052245 as eigenvector,
    29261.18329671326 as eigenvalue
  union all
  select
    1 as i,
    0 as comp,
    'x3' as my_columns_column,
    -0.5730425037096113 as eigenvector,
    29261.18329671326 as eigenvalue
  union all
  select
    1 as i,
    0 as comp,
    'x4' as my_columns_column,
    -0.5375734787462785 as eigenvector,
    29261.18329671326 as eigenvalue
  union all
  select
    1 as i,
    0 as comp,
    'x5' as my_columns_column,
    0.001042830923792024 as eigenvector,
    29261.18329671326 as eigenvalue
  union all
  select
    1 as i,
    1 as comp,
    'x1' as my_columns_column,
    -0.06624103790399177 as eigenvector,
    10006.031828705976 as eigenvalue
  union all
  select
    1 as i,
    1 as comp,
    'x2' as my_columns_column,
    -0.001674522205010131 as eigenvector,
    10006.031828705976 as eigenvalue
  union all
  select
    1 as i,
    1 as comp,
    'x3' as my_columns_column,
    -0.0027280890356712506 as eigenvector,
    10006.031828705976 as eigenvalue
  union all
  select
    1 as i,
    1 as comp,
    'x4' as my_columns_column,
    -0.022663548693591457 as eigenvector,
    10006.031828705976 as eigenvalue
  union all
  select
    1 as i,
    1 as comp,
    'x5' as my_columns_column,
    0.9975410978819939 as eigenvector,
    10006.031828705976 as eigenvalue
  union all
  select
    2 as i,
    0 as comp,
    'x1' as my_columns_column,
    0.23766556798297242 as eigenvector,
    29261.18329671326 as eigenvalue
  union all
  select
    2 as i,
    0 as comp,
    'x2' as my_columns_column,
    -0.571096344052245 as eigenvector,
    29261.18329671326 as eigenvalue
  union all
  select
    2 as i,
    0 as comp,
    'x3' as my_columns_column,
    -0.5730425037096113 as eigenvector,
    29261.18329671326 as eigenvalue
  union all
  select
    2 as i,
    0 as comp,
    'x4' as my_columns_column,
    -0.5375734787462785 as eigenvector,
    29261.18329671326 as eigenvalue
  union all
  select
    2 as i,
    0 as comp,
    'x5' as my_columns_column,
    0.001042830923792024 as eigenvector,
    29261.18329671326 as eigenvalue
)

select
  coalesce(a.i, e.i) as i,
  coalesce(a.comp, e.comp) as comp,
  coalesce(a.my_columns_column, e.my_columns_column) as my_columns_column,
  e.eigenvector as expected_eigenvector,
  a.eigenvector as actual_eigenvector,
  e.eigenvalue as expected_eigenvalue,
  a.eigenvalue as actual_eigenvalue
from expected as e
full outer join {{ ref("collinear_matrix_two_pca_calls") }} as a
on a.comp = e.comp
and a.my_columns_column = e.my_columns_column
and a.i = e.i
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
