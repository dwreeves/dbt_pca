with expected as (
  select
    0 as comp,
    'c0' as col,
    0.4090260128919895 as eigenvector,
    44941.19903187152 as eigenvalue
  union all
  select
    0 as comp,
    'c1' as col,
    0.40944265472705543 as eigenvector,
    44941.19903187152 as eigenvalue
  union all
  select
    0 as comp,
    'c3' as col,
    0.4084758835434476 as eigenvector,
    44941.19903187152 as eigenvalue
  union all
  select
    0 as comp,
    'c5' as col,
    0.4078633204663924 as eigenvector,
    44941.19903187152 as eigenvalue
  union all
  select
    0 as comp,
    'c6' as col,
    0.4073182835966303 as eigenvector,
    44941.19903187152 as eigenvalue
  union all
  select
    0 as comp,
    'c7' as col,
    0.4073588264628216 as eigenvector,
    44941.19903187152 as eigenvalue
  union all
  select
    1 as comp,
    'c0' as col,
    -0.1367078340079672 as eigenvector,
    3085.932106297134 as eigenvalue
  union all
  select
    1 as comp,
    'c1' as col,
    -0.47419797005703934 as eigenvector,
    3085.932106297134 as eigenvalue
  union all
  select
    1 as comp,
    'c3' as col,
    -0.28774001774128616 as eigenvector,
    3085.932106297134 as eigenvalue
  union all
  select
    1 as comp,
    'c5' as col,
    -0.19442662657204052 as eigenvector,
    3085.932106297134 as eigenvalue
  union all
  select
    1 as comp,
    'c6' as col,
    0.6789166778349378 as eigenvector,
    3085.932106297134 as eigenvalue
  union all
  select
    1 as comp,
    'c7' as col,
    0.4182384092144879 as eigenvector,
    3085.932106297134 as eigenvalue
  union all
  select
    2 as comp,
    'c0' as col,
    0.04973697573737186 as eigenvector,
    3056.0941019766933 as eigenvalue
  union all
  select
    2 as comp,
    'c1' as col,
    -0.01704959102428779 as eigenvector,
    3056.0941019766933 as eigenvalue
  union all
  select
    2 as comp,
    'c3' as col,
    -0.4628175173413314 as eigenvector,
    3056.0941019766933 as eigenvalue
  union all
  select
    2 as comp,
    'c5' as col,
    0.644491535883528 as eigenvector,
    3056.0941019766933 as eigenvalue
  union all
  select
    2 as comp,
    'c6' as col,
    0.3082066390523553 as eigenvector,
    3056.0941019766933 as eigenvalue
  union all
  select
    2 as comp,
    'c7' as col,
    -0.5221827440187521 as eigenvector,
    3056.0941019766933 as eigenvalue
  union all
  select
    3 as comp,
    'c0' as col,
    0.4548681871834347 as eigenvector,
    3036.6747720808985 as eigenvalue
  union all
  select
    3 as comp,
    'c1' as col,
    -0.08560582241212028 as eigenvector,
    3036.6747720808985 as eigenvalue
  union all
  select
    3 as comp,
    'c3' as col,
    0.2981067842092897 as eigenvector,
    3036.6747720808985 as eigenvalue
  union all
  select
    3 as comp,
    'c5' as col,
    -0.47101027614303115 as eigenvector,
    3036.6747720808985 as eigenvalue
  union all
  select
    3 as comp,
    'c6' as col,
    0.37821606235300703 as eigenvector,
    3036.6747720808985 as eigenvalue
  union all
  select
    3 as comp,
    'c7' as col,
    -0.5761951497642516 as eigenvector,
    3036.6747720808985 as eigenvalue
)

select
  coalesce(a.comp, e.comp) as comp,
  coalesce(a.col, e.col) as col,
  e.eigenvector as expected_eigenvector,
  a.eigenvector as actual_eigenvector,
  e.eigenvalue as expected_eigenvalue,
  a.eigenvalue as actual_eigenvalue
from expected as e
full outer join {{ ref("missing_data_matrix_ncomp_4_drop_col") }} as a
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
