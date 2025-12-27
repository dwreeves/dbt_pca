select * from {{
  dbt_pca.pca(
    table=ref('missing_data_matrix'),
    columns=['c0', 'c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7'],
    missing='drop-col',
    ncomp=4
  )
}}
