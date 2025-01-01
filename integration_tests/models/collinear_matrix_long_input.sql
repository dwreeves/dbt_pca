select * from {{
  dbt_pca.pca(
    table=ref('_collinear_matrix_long'),
    index='my_index_column',
    columns='my_columns_column',
    values='my_values_column',
    ncomp=5 if adapter.type() == 'clickhouse' or model.config.materialized == 'pca' else none
  )
}}
