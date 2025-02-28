select * from {{
  dbt_pca.pca(
    table=ref('_collinear_matrix_long'),
    index='my_index_column',
    columns='my_columns_column',
    values='my_values_column',
    output='loadings-wide',
    ncomp=3
  )
}}
