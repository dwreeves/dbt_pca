with expected as (
  select
    0 as comp,
    'c0' as col,
    0.3541543546347137 as eigenvector,
    58868.865222311506 as eigenvalue
  union all
  select
    0 as comp,
    'c1' as col,
    0.35437903225269335 as eigenvector,
    58868.865222311506 as eigenvalue
  union all
  select
    0 as comp,
    'c2' as col,
    0.3539362990425001 as eigenvector,
    58868.865222311506 as eigenvalue
  union all
  select
    0 as comp,
    'c3' as col,
    0.35343984715028265 as eigenvector,
    58868.865222311506 as eigenvalue
  union all
  select
    0 as comp,
    'c4' as col,
    0.3530997058643093 as eigenvector,
    58868.865222311506 as eigenvalue
  union all
  select
    0 as comp,
    'c5' as col,
    0.3533888963412878 as eigenvector,
    58868.865222311506 as eigenvalue
  union all
  select
    0 as comp,
    'c6' as col,
    0.35316424905467436 as eigenvector,
    58868.865222311506 as eigenvalue
  union all
  select
    0 as comp,
    'c7' as col,
    0.35286182013682 as eigenvector,
    58868.865222311506 as eigenvalue
  union all
  select
    1 as comp,
    'c0' as col,
    -0.2907957214777183 as eigenvector,
    3123.4493825619693 as eigenvalue
  union all
  select
    1 as comp,
    'c1' as col,
    -0.427725799820192 as eigenvector,
    3123.4493825619693 as eigenvalue
  union all
  select
    1 as comp,
    'c2' as col,
    0.4547773384050576 as eigenvector,
    3123.4493825619693 as eigenvalue
  union all
  select
    1 as comp,
    'c3' as col,
    -0.22976329412008215 as eigenvector,
    3123.4493825619693 as eigenvalue
  union all
  select
    1 as comp,
    'c4' as col,
    -0.21343911276552757 as eigenvector,
    3123.4493825619693 as eigenvalue
  union all
  select
    1 as comp,
    'c5' as col,
    -0.17352906747252503 as eigenvector,
    3123.4493825619693 as eigenvalue
  union all
  select
    1 as comp,
    'c6' as col,
    0.5041877855065907 as eigenvector,
    3123.4493825619693 as eigenvalue
  union all
  select
    1 as comp,
    'c7' as col,
    0.3781546965418664 as eigenvector,
    3123.4493825619693 as eigenvalue
  union all
  select
    2 as comp,
    'c0' as col,
    0.039219227065679485 as eigenvector,
    3079.668268737849 as eigenvalue
  union all
  select
    2 as comp,
    'c1' as col,
    -0.18806159147420007 as eigenvector,
    3079.668268737849 as eigenvalue
  union all
  select
    2 as comp,
    'c2' as col,
    -0.00764262649771376 as eigenvector,
    3079.668268737849 as eigenvalue
  union all
  select
    2 as comp,
    'c3' as col,
    -0.583928128997888 as eigenvector,
    3079.668268737849 as eigenvalue
  union all
  select
    2 as comp,
    'c4' as col,
    0.5260246763029777 as eigenvector,
    3079.668268737849 as eigenvalue
  union all
  select
    2 as comp,
    'c5' as col,
    0.36192914190695363 as eigenvector,
    3079.668268737849 as eigenvalue
  union all
  select
    2 as comp,
    'c6' as col,
    0.24553274650199017 as eigenvector,
    3079.668268737849 as eigenvalue
  union all
  select
    2 as comp,
    'c7' as col,
    -0.39253436412604364 as eigenvector,
    3079.668268737849 as eigenvalue
  union all
  select
    3 as comp,
    'c0' as col,
    0.46604560525220196 as eigenvector,
    3040.3103218290853 as eigenvalue
  union all
  select
    3 as comp,
    'c1' as col,
    -0.19913600313029015 as eigenvector,
    3040.3103218290853 as eigenvalue
  union all
  select
    3 as comp,
    'c2' as col,
    -0.13748786373143865 as eigenvector,
    3040.3103218290853 as eigenvalue
  union all
  select
    3 as comp,
    'c3' as col,
    0.23457897664015426 as eigenvector,
    3040.3103218290853 as eigenvalue
  union all
  select
    3 as comp,
    'c4' as col,
    0.1457409989385779 as eigenvector,
    3040.3103218290853 as eigenvalue
  union all
  select
    3 as comp,
    'c5' as col,
    -0.6131330086923586 as eigenvector,
    3040.3103218290853 as eigenvalue
  union all
  select
    3 as comp,
    'c6' as col,
    0.4167130987809413 as eigenvector,
    3040.3103218290853 as eigenvalue
  union all
  select
    3 as comp,
    'c7' as col,
    -0.3136778686998052 as eigenvector,
    3040.3103218290853 as eigenvalue
)

select
  coalesce(a.comp, e.comp) as comp,
  coalesce(a.col, e.col) as col,
  e.eigenvector as expected_eigenvector,
  a.eigenvector as actual_eigenvector,
  e.eigenvalue as expected_eigenvalue,
  a.eigenvalue as actual_eigenvalue
from expected as e
full outer join {{ ref("missing_data_matrix_ncomp_4_drop_row") }} as a
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
