with expected as (
  select
    0 as comp,
    'x1' as col,
    0.6526987970651394 as eigenvector,
    37130.63566972391 as eigenvalue
  union all
  select
    0 as comp,
    'x2' as col,
    -0.4202505171282636 as eigenvector,
    37130.63566972391 as eigenvalue
  union all
  select
    0 as comp,
    'x3' as col,
    -0.4213217879426871 as eigenvector,
    37130.63566972391 as eigenvalue
  union all
  select
    0 as comp,
    'x4' as col,
    -0.4688936849731618 as eigenvector,
    37130.63566972391 as eigenvalue
  union all
  select
    0 as comp,
    'x5' as col,
    -0.0006681019486932526 as eigenvector,
    37130.63566972391 as eigenvalue
  union all
  select
    1 as comp,
    'x1' as col,
    0.7549096908937165 as eigenvector,
    21235.210813810972 as eigenvalue
  union all
  select
    1 as comp,
    'x2' as col,
    0.3223714699689113 as eigenvector,
    21235.210813810972 as eigenvalue
  union all
  select
    1 as comp,
    'x3' as col,
    0.3264521834745814 as eigenvector,
    21235.210813810972 as eigenvalue
  union all
  select
    1 as comp,
    'x4' as col,
    0.46858187276500235 as eigenvector,
    21235.210813810972 as eigenvalue
  union all
  select
    1 as comp,
    'x5' as col,
    -0.006927796587535453 as eigenvector,
    21235.210813810972 as eigenvalue
  union all
  select
    2 as comp,
    'x1' as col,
    0.00664241488952428 as eigenvector,
    6982.585654800197 as eigenvalue
  union all
  select
    2 as comp,
    'x2' as col,
    0.009557577056777133 as eigenvector,
    6982.585654800197 as eigenvalue
  union all
  select
    2 as comp,
    'x3' as col,
    0.00866738570852569 as eigenvector,
    6982.585654800197 as eigenvalue
  union all
  select
    2 as comp,
    'x4' as col,
    -0.008532523122917092 as eigenvector,
    6982.585654800197 as eigenvalue
  union all
  select
    2 as comp,
    'x5' as col,
    0.9998582917190696 as eigenvector,
    6982.585654800197 as eigenvalue
  union all
  select
    3 as comp,
    'x1' as col,
    -0.0636490335088572 as eigenvector,
    2032.2381983418795 as eigenvalue
  union all
  select
    3 as comp,
    'x2' as col,
    -0.4911615990981664 as eigenvector,
    2032.2381983418795 as eigenvalue
  union all
  select
    3 as comp,
    'x3' as col,
    -0.4413236222730471 as eigenvector,
    2032.2381983418795 as eigenvalue
  union all
  select
    3 as comp,
    'x4' as col,
    0.748136084279949 as eigenvector,
    2032.2381983418795 as eigenvalue
  union all
  select
    3 as comp,
    'x5' as col,
    0.01532788070094962 as eigenvector,
    2032.2381983418795 as eigenvalue
  union all
  select
    4 as comp,
    'x1' as col,
    0.0005637040865160475 as eigenvector,
    113.15654153959025 as eigenvalue
  union all
  select
    4 as comp,
    'x2' as col,
    -0.6914731191417685 as eigenvector,
    113.15654153959025 as eigenvalue
  union all
  select
    4 as comp,
    'x3' as col,
    0.7218554285715612 as eigenvector,
    113.15654153959025 as eigenvalue
  union all
  select
    4 as comp,
    'x4' as col,
    -0.028095126904052276 as eigenvector,
    113.15654153959025 as eigenvalue
  union all
  select
    4 as comp,
    'x5' as col,
    0.00010875692905442444 as eigenvector,
    113.15654153959025 as eigenvalue
)

select
  coalesce(a.comp, e.comp) as comp,
  coalesce(a.col, e.col) as col,
  e.eigenvector as expected_eigenvector,
  a.eigenvector as actual_eigenvector,
  e.eigenvalue as expected_eigenvalue,
  a.eigenvalue as actual_eigenvalue
from expected as e
full outer join {{ ref("collinear_matrix_wide_input_with_weights") }} as a
on a.comp = e.comp
and a.col = e.col
where
  least(
    abs(expected_eigenvector - actual_eigenvector),
    abs(expected_eigenvector + actual_eigenvector)
  ) > {{ var('test_precision') }}
  or abs(expected_eigenvalue - actual_eigenvalue) > expected_eigenvalue * {{ var('test_precision') }}
  or a.comp is null
  or e.comp is null
  or a.col is null
  or e.col is null
