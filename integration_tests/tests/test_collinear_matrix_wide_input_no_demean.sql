with expected as (
  select
    0 as comp,
    'x1' as col,
    -0.08098112064531761 as eigenvector,
    995476.3459739941 as eigenvalue
  union all
  select
    0 as comp,
    'x2' as col,
    0.3174284643059494 as eigenvector,
    995476.3459739941 as eigenvalue
  union all
  select
    0 as comp,
    'x3' as col,
    0.5653084779250076 as eigenvector,
    995476.3459739941 as eigenvalue
  union all
  select
    0 as comp,
    'x4' as col,
    0.7486488583547374 as eigenvector,
    995476.3459739941 as eigenvalue
  union all
  select
    0 as comp,
    'x5' as col,
    -0.11239412714930776 as eigenvector,
    995476.3459739941 as eigenvalue
  union all
  select
    1 as comp,
    'x1' as col,
    0.33401427664974975 as eigenvector,
    281474.59415884415 as eigenvalue
  union all
  select
    1 as comp,
    'x2' as col,
    0.007974685570940745 as eigenvector,
    281474.59415884415 as eigenvalue
  union all
  select
    1 as comp,
    'x3' as col,
    0.5433850424877764 as eigenvector,
    281474.59415884415 as eigenvalue
  union all
  select
    1 as comp,
    'x4' as col,
    -0.269240210690627 as eigenvector,
    281474.59415884415 as eigenvalue
  union all
  select
    1 as comp,
    'x5' as col,
    0.7215353573678932 as eigenvector,
    281474.59415884415 as eigenvalue
  union all
  select
    2 as comp,
    'x1' as col,
    0.30628427288594223 as eigenvector,
    54043.19521823815 as eigenvalue
  union all
  select
    2 as comp,
    'x2' as col,
    -0.37475833644002143 as eigenvector,
    54043.19521823815 as eigenvalue
  union all
  select
    2 as comp,
    'x3' as col,
    -0.4599757970693137 as eigenvector,
    54043.19521823815 as eigenvalue
  union all
  select
    2 as comp,
    'x4' as col,
    0.6045684953840069 as eigenvector,
    54043.19521823815 as eigenvalue
  union all
  select
    2 as comp,
    'x5' as col,
    0.4343562293222062 as eigenvector,
    54043.19521823815 as eigenvalue
  union all
  select
    3 as comp,
    'x1' as col,
    0.8703344931429858 as eigenvector,
    9400.401323697864 as eigenvalue
  union all
  select
    3 as comp,
    'x2' as col,
    -0.016637800271994102 as eigenvector,
    9400.401323697864 as eigenvalue
  union all
  select
    3 as comp,
    'x3' as col,
    0.08777205316385067 as eigenvector,
    9400.401323697864 as eigenvalue
  union all
  select
    3 as comp,
    'x4' as col,
    -0.03756593933088638 as eigenvector,
    9400.401323697864 as eigenvalue
  union all
  select
    3 as comp,
    'x5' as col,
    -0.48283115116284864 as eigenvector,
    9400.401323697864 as eigenvalue
  union all
  select
    4 as comp,
    'x1' as col,
    0.17488377661019514 as eigenvector,
    2611.495790053735 as eigenvalue
  union all
  select
    4 as comp,
    'x2' as col,
    0.8708931893804361 as eigenvector,
    2611.495790053735 as eigenvalue
  union all
  select
    4 as comp,
    'x3' as col,
    -0.407280435548432 as eigenvector,
    2611.495790053735 as eigenvalue
  union all
  select
    4 as comp,
    'x4' as col,
    -0.010969522447283338 as eigenvector,
    2611.495790053735 as eigenvalue
  union all
  select
    4 as comp,
    'x5' as col,
    0.21204488620592898 as eigenvector,
    2611.495790053735 as eigenvalue
)

select
  coalesce(a.comp, e.comp) as comp,
  coalesce(a.col, e.col) as col,
  e.eigenvector as expected_eigenvector,
  a.eigenvector as actual_eigenvector,
  e.eigenvalue as expected_eigenvalue,
  a.eigenvalue as actual_eigenvalue
from expected as e
full outer join {{ ref("collinear_matrix_wide_input_no_demean") }} as a
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
