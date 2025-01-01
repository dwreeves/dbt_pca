with expected as (
  select
    0 as idx,
    0 as comp,
    0.0034265448248486257 as factor
  union all
  select
    0 as idx,
    1 as comp,
    0.009479801265163753 as factor
  union all
  select
    0 as idx,
    2 as comp,
    -0.0063263234933535395 as factor
  union all
  select
    1 as idx,
    0 as comp,
    0.005501894476530804 as factor
  union all
  select
    1 as idx,
    1 as comp,
    0.010709812987072475 as factor
  union all
  select
    1 as idx,
    2 as comp,
    0.010698478704316361 as factor
  union all
  select
    2 as idx,
    0 as comp,
    -0.01328992608752498 as factor
  union all
  select
    2 as idx,
    1 as comp,
    0.011040844601365356 as factor
  union all
  select
    2 as idx,
    2 as comp,
    -0.005470721786298696 as factor
  union all
  select
    3 as idx,
    0 as comp,
    -0.010296412983722483 as factor
  union all
  select
    3 as idx,
    1 as comp,
    0.003824978410454261 as factor
  union all
  select
    3 as idx,
    2 as comp,
    0.006095762491241554 as factor
  union all
  select
    4 as idx,
    0 as comp,
    -0.0032813852994960707 as factor
  union all
  select
    4 as idx,
    1 as comp,
    0.00729286760434824 as factor
  union all
  select
    4 as idx,
    2 as comp,
    0.0003336620416481003 as factor
)

select
  coalesce(a.comp, e.comp) as comp,
  coalesce(a.idx, e.idx) as idx,
  e.factor as expected_factor,
  a.factor as actual_factor
from expected as e
full outer join (
  /* Only test first 5 rows of each factor. */
  select *
  from {{ ref("collinear_matrix_wide_input_ncomp_3_no_demean_factors_output") }}
  where idx <= 4
) as a
on a.comp = e.comp
and a.idx = e.idx
where
  least(
    abs(expected_factor - actual_factor),
    abs(expected_factor + actual_factor)
  ) > {{ var('test_precision') }}
  or a.comp is null
  or e.comp is null
  or a.idx is null
  or e.idx is null
