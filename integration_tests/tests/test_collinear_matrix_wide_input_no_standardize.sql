with expected as (
  select
    0 as comp,
    'x1' as col,
    -0.027238965384545108 as eigenvector,
    939899.3104917525 as eigenvalue
  union all
  select
    0 as comp,
    'x2' as col,
    0.3102131572138164 as eigenvector,
    939899.3104917525 as eigenvalue
  union all
  select
    0 as comp,
    'x3' as col,
    0.6347831266313418 as eigenvector,
    939899.3104917525 as eigenvalue
  union all
  select
    0 as comp,
    'x4' as col,
    0.7071605171901422 as eigenvector,
    939899.3104917525 as eigenvalue
  union all
  select
    0 as comp,
    'x5' as col,
    -0.00047002949276106146 as eigenvector,
    939899.3104917525 as eigenvalue
  union all
  select
    1 as comp,
    'x1' as col,
    0.09244184579971015 as eigenvector,
    66541.70940098274 as eigenvalue
  union all
  select
    1 as comp,
    'x2' as col,
    -0.31878263306678006 as eigenvector,
    66541.70940098274 as eigenvalue
  union all
  select
    1 as comp,
    'x3' as col,
    -0.6261443030719896 as eigenvector,
    66541.70940098274 as eigenvalue
  union all
  select
    1 as comp,
    'x4' as col,
    0.7054544118420794 as eigenvector,
    66541.70940098274 as eigenvalue
  union all
  select
    1 as comp,
    'x5' as col,
    -0.010465301868162425 as eigenvector,
    66541.70940098274 as eigenvalue
  union all
  select
    2 as comp,
    'x1' as col,
    -0.0076341830596271375 as eigenvector,
    11316.849559331671 as eigenvalue
  union all
  select
    2 as comp,
    'x2' as col,
    -0.0021882498970313574 as eigenvector,
    11316.849559331671 as eigenvalue
  union all
  select
    2 as comp,
    'x3' as col,
    -0.007570197423940187 as eigenvector,
    11316.849559331671 as eigenvalue
  union all
  select
    2 as comp,
    'x4' as col,
    0.008125872123634748 as eigenvector,
    11316.849559331671 as eigenvalue
  union all
  select
    2 as comp,
    'x5' as col,
    0.9999067922184512 as eigenvector,
    11316.849559331671 as eigenvalue
  union all
  select
    3 as comp,
    'x1' as col,
    0.9952399229707823 as eigenvector,
    8795.84901048305 as eigenvalue
  union all
  select
    3 as comp,
    'x2' as col,
    0.027002362766230577 as eigenvector,
    8795.84901048305 as eigenvalue
  union all
  select
    3 as comp,
    'x3' as col,
    0.08098733583157446 as eigenvector,
    8795.84901048305 as eigenvalue
  union all
  select
    3 as comp,
    'x4' as col,
    -0.04620239939722191 as eigenvector,
    8795.84901048305 as eigenvalue
  union all
  select
    3 as comp,
    'x5' as col,
    0.00864626248708697 as eigenvector,
    8795.84901048305 as eigenvalue
  union all
  select
    4 as comp,
    'x1' as col,
    0.012319045581765754 as eigenvector,
    1961.4949162854164 as eigenvalue
  union all
  select
    4 as comp,
    'x2' as col,
    0.8952159035192995 as eigenvector,
    1961.4949162854164 as eigenvalue
  union all
  select
    4 as comp,
    'x3' as col,
    -0.44539582106312653 as eigenvector,
    1961.4949162854164 as eigenvalue
  union all
  select
    4 as comp,
    'x4' as col,
    0.0075752380877704825 as eigenvector,
    1961.4949162854164 as eigenvalue
  union all
  select
    4 as comp,
    'x5' as col,
    -0.0013804164210173024 as eigenvector,
    1961.4949162854164 as eigenvalue
)

select
  coalesce(a.comp, e.comp) as comp,
  coalesce(a.col, e.col) as col,
  e.eigenvector as expected_eigenvector,
  a.eigenvector as actual_eigenvector,
  e.eigenvalue as expected_eigenvalue,
  a.eigenvalue as actual_eigenvalue
from expected as e
full outer join {{ ref("collinear_matrix_wide_input_no_standardize") }} as a
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
