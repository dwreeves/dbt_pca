select * from {{
  dbt_pca.pca(
    table=ref('collinear_matrix'),
    columns=['x1', 'x2', 'x3', 'x4', 'x5'],
    index='idx',
    output='projections-untransformed-wide',
    ncomp=3
  )
}}
