with expected as (
  select
    0 as idx,
    -0.007531873796311788 as factor_0,
    -0.015497183329978934 as factor_1,
    -0.0033640181060839588 as factor_2
  union all
  select
    1 as idx,
    -0.006652803625498362 as factor_0,
    0.00297332846609244 as factor_1,
    0.008421475796314328 as factor_2
  union all
  select
    2 as idx,
    0.011459594172715793 as factor_0,
    0.0053156660446589 as factor_1,
    0.014124672166084809 as factor_2
  union all
  select
    3 as idx,
    0.0077023423457717195 as factor_0,
    -0.0056956867103752805 as factor_1,
    -0.009690301032865098 as factor_2
  union all
  select
    4 as idx,
    0.00023533504953835392 as factor_0,
    -0.008643281012909005 as factor_1,
    -0.006573376366082388 as factor_2
  union all
  select
    5 as idx,
    -0.003595167519456717 as factor_0,
    0.0033718647157139096 as factor_1,
    0.008704472497084207 as factor_2
  union all
  select
    6 as idx,
    0.005215079767207483 as factor_0,
    0.007430523371299804 as factor_1,
    -0.006180379745591899 as factor_2
  union all
  select
    7 as idx,
    -0.0017828806642594442 as factor_0,
    0.00654021270537083 as factor_1,
    -0.022362643097406012 as factor_2
  union all
  select
    8 as idx,
    0.017915363857436705 as factor_0,
    -0.004863885914909225 as factor_1,
    0.004209231944661316 as factor_2
  union all
  select
    9 as idx,
    0.0051531216919065365 as factor_0,
    0.00548432641165697 as factor_1,
    -0.00017933076673516633 as factor_2
)

select
  coalesce(a.idx, e.idx) as idx,
  e.factor_0 as expected_factor_0,
  a.factor_0 as actual_factor_0,
  e.factor_1 as expected_factor_1,
  a.factor_1 as actual_factor_1,
  e.factor_2 as expected_factor_2,
  a.factor_2 as actual_factor_2
from expected as e
full outer join (
  /* Only test first 10 rows. */
  select *
  from {{ ref("collinear_matrix_wide_input_ncomp_3_factors_wide_output") }}
  where idx <= 9
) as a
on a.idx = e.idx
where
  least(
    abs(expected_factor_0 - actual_factor_0),
    abs(expected_factor_0 + actual_factor_0)
  ) > {{ var('test_precision') }}
  or
  least(
    abs(expected_factor_1 - actual_factor_1),
    abs(expected_factor_1 + actual_factor_1)
  ) > {{ var('test_precision') }}
  or
  least(
    abs(expected_factor_2 - actual_factor_2),
    abs(expected_factor_2 + actual_factor_2)
  ) > {{ var('test_precision') }}
  or a.idx is null
  or e.idx is null
