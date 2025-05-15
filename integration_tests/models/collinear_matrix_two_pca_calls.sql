select 1 as i, * from {{
  dbt_pca.pca(
    table=ref('_collinear_matrix_long'),
    index='my_index_column',
    columns='my_columns_column',
    values='my_values_column',
    ncomp=2
  )
}}

union all

select 2 as i, * from {{
  dbt_pca.pca(
    table=ref('_collinear_matrix_long'),
    index='my_index_column',
    columns='my_columns_column',
    values='my_values_column',
    ncomp=1
  )
}}
