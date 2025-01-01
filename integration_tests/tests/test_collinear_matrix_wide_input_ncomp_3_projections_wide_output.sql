with expected as (
  select
    0 as idx,
    1.4963383965789152 as x1,
    1.1711349275797134 as x2,
    5.41227613740505 as x3,
    1.5921895853543906 as x4,
    2.333346002808246 as x5
  union all
  select
    1 as idx,
    2.481462125185497 as x1,
    1.2270716362627736 as x2,
    5.5468687194576445 as x3,
    2.433033263517012 as x4,
    4.376855822014036 as x5
  union all
  select
    2 as idx,
    3.7201556890843346 as x1,
    -4.173191131771575 as x2,
    -5.4137167007565425 as x3,
    -8.590909022226088 as x4,
    4.668757710898932 as x5
  union all
  select
    3 as idx,
    1.4748983671226572 as x1,
    -3.691834720490476 as x2,
    -4.495254508202602 as x3,
    -9.421011830150396 as x4,
    3.3328562803692865 as x5
  union all
  select
    4 as idx,
    1.4745833984074825 as x1,
    -1.308782686835862 as x2,
    0.35864586737054394 as x3,
    -4.050438318408194 as x4,
    3.0401923041889196 as x5
  union all
  select
    5 as idx,
    2.6287364553552806 as x1,
    0.29607091149830733 as x2,
    3.6553019182414834 as x3,
    0.4732757243143424 as x4,
    4.421725287187392 as x5
  union all
  select
    6 as idx,
    1.6061640552762948 as x1,
    -2.835088457846104 as x2,
    -2.7529256284134327 as x3,
    -7.500052456481587 as x4,
    4.750584908647274 as x5
  union all
  select
    7 as idx,
    -0.1436402042917888 as x1,
    -1.1466653906404956 as x2,
    0.6334631971151352 as x3,
    -5.270777291647727 as x4,
    4.541920993080416 as x5
  union all
  select
    8 as idx,
    3.1480880550775066 as x1,
    -6.432411788570695 as x2,
    -10.026363862747685 as x3,
    -14.077926829761976 as x4,
    3.520005534828902 as x5
  union all
  select
    9 as idx,
    2.162219702162644 as x1,
    -2.6441658082272417 as x2,
    -2.346686213440411 as x3,
    -6.556982409192706 as x4,
    4.585758949839868 as x5
)

select
  coalesce(a.idx, e.idx) as idx,
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
full outer join (
  /* Only test first 10 rows. */
  select *
  from {{ ref("collinear_matrix_wide_input_ncomp_3_projections_wide_output") }}
  where idx <= 9
) as a
on a.idx = e.idx
where
  least(
    abs(expected_x1 - actual_x1),
    abs(expected_x1 + actual_x1)
  ) > {{ var('test_precision') }} * greatest(abs(expected_x1), 1)
  or
  least(
    abs(expected_x2 - actual_x2),
    abs(expected_x2 + actual_x2)
  ) > {{ var('test_precision') }} * greatest(abs(expected_x2), 1)
  or
  least(
    abs(expected_x3 - actual_x3),
    abs(expected_x3 + actual_x3)
  ) > {{ var('test_precision') }} * greatest(abs(expected_x3), 1)
  or
  least(
    abs(expected_x4 - actual_x4),
    abs(expected_x4 + actual_x4)
  ) > {{ var('test_precision') }} * greatest(abs(expected_x4), 1)
  or
  least(
    abs(expected_x5 - actual_x5),
    abs(expected_x5 + actual_x5)
  ) > {{ var('test_precision') }} * greatest(abs(expected_x5), 1)
  or a.idx is null
  or e.idx is null
