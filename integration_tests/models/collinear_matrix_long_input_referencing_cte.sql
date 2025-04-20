{{ config(materialized='table') }}
with data as (
  select idx as my_index_column, 'x1' as my_columns_column, x1 as my_values_column
  from {{ ref('collinear_matrix') }}
  union all
  select idx as my_index_column, 'x2' as my_columns_column, x2 as my_values_column
  from {{ ref('collinear_matrix') }}
  union all
  select idx as my_index_column, 'x3' as my_columns_column, x3 as my_values_column
  from {{ ref('collinear_matrix') }}
  union all
  select idx as my_index_column, 'x4' as my_columns_column, x4 as my_values_column
  from {{ ref('collinear_matrix') }}
  union all
  select idx as my_index_column, 'x5' as my_columns_column, x5 as my_values_column
  from {{ ref('collinear_matrix') }}
)

select * from {{
  dbt_pca.pca(
    table='data',
    index='my_index_column',
    columns='my_columns_column',
    values='my_values_column',
    ncomp=2
  )
}}
