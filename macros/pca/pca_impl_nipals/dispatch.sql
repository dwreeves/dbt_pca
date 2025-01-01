{% macro _pca_nipals(table,
                     index,
                     columns,
                     values,
                     ncomp,
                     normalize,
                     standardize,
                     demean,
                     missing,
                     weights,
                     output,
                     output_options,
                     method_options) %}
  {{ return(
    adapter.dispatch('_pca_nipals', 'dbt_pca')(
      table=table,
      index=index,
      columns=columns,
      values=values,
      ncomp=ncomp,
      normalize=normalize,
      standardize=standardize,
      demean=demean,
      missing=missing,
      weights=weights,
      output=output,
      output_options=output_options,
      method_options=method_options
    )
  ) }}
{% endmacro %}

{% macro default___pca_nipals(table,
                              index,
                              columns,
                              values,
                              ncomp,
                              normalize,
                              standardize,
                              demean,
                              missing,
                              weights,
                              output,
                              output_options,
                              method_options) %}
  {{ exceptions.raise_not_implemented(
    "This database is not currently supported."
  ) }}
{% endmacro %}

{% macro _pca_nipals_single_iteration(previous,
                                      idx,
                                      cols,
                                      compnum,
                                      check_tol,
                                      tol,
                                      max_iter,
                                      deterministic_column_seeding,
                                      _first=true) %}
  {{ return(
    adapter.dispatch('_pca_nipals_single_iteration', 'dbt_pca')(
      previous=previous,
      idx=idx,
      cols=cols,
      compnum=compnum,
      check_tol=check_tol,
      tol=tol,
      max_iter=max_iter,
      deterministic_column_seeding=deterministic_column_seeding,
      _first=_first
    )
  ) }}
{% endmacro %}

{% macro default___pca_nipals_single_iteration(previous,
                                               idx,
                                               cols,
                                               compnum,
                                               check_tol,
                                               tol,
                                               max_iter,
                                               deterministic_column_seeding,
                                               _first) %}
  {# For now, DuckDB just uses the Clickhouse impl.
     It should be nearly equivalent to what a dedicated DuckDB impl would look like. #}
  {{ return(dbt_pca.clickhouse___pca_nipals_single_iteration(
      previous=previous,
      idx=idx,
      cols=cols,
      compnum=compnum,
      check_tol=check_tol,
      tol=tol,
      max_iter=max_iter,
      deterministic_column_seeding=deterministic_column_seeding,
      _first=_first
  )) }}
{% endmacro %}
