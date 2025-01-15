> [!WARNING]
> This project is in **beta** and is not fully publicly released.
> The API is subject to breakage and bugs may occur.
> Please wait until **0.1.0** for full release.
>
> The following features are currently missing and are being prioritized for a **0.1.0** release:
> - Missing value support (but probably not the EM algorithm algorithm for the time being).
> - Support for weights
> - Snowflake support
> - User facing conversion functions for eigenvectors --> factors + projections.
> - (Maybe) Fuller, paginated documentation on Github Pages.

<p align="center">
    <picture>
        <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/dwreeves/dbt_pca/main/docs/src/img/dbt-pca-banner-dark.png#readme-logo">
        <img src="https://raw.githubusercontent.com/dwreeves/dbt_pca/main/docs/src/img/dbt-pca-banner-light.png#readme-logo" alt="dbt_pca logo">
    </picture>
</p>
<p align="center">
    <em>PCA in SQL, powered by dbt.</em>
</p>
<p align="center">
    <img src="https://github.com/dwreeves/dbt_pca/workflows/tests/badge.svg" alt="Tests badge">
    <img src="https://github.com/dwreeves/dbt_pca/workflows/docs/badge.svg" alt="Docs badge">
</p>

# Overview

**dbt_pca** is an easy way to perform principal component analysis (PCA) in SQL using dbt.

Reasons to use **dbt_pca**:

- 📈 **PCA in pure SQL:** With the power of recursive CTEs and math, it is possible to implement PCA in pure SQL. Most SQL engines (even OLAP engines) do not have an implementation of PCA, so this fills a valuable niche. **`dbt_pca` implements a true implementation of PCA via the NIPALS algorithm.**
- 📱 **Simple interface:** Just define a `table=` (which works with `ref()`, `source()`, and CTEs), your column(s) with `columns=`, an index with `index=`, and you're all set! Both "wide" and "long" data formats are supported.
- 🤸‍ **Flexibility:** Tons of output options available to return things the way you want: choose from eigenvectors, factors, and projections in both wide and long formats.
- 🤗 **User friendly:** The API provides comprehensive feedback on input errors.
- 💪 **Durable and tested:** Everything in this code base is tested against equivalent PCAz performed in Statsmodels with high precision assertions (between 10e-6 to 10e-7, depending on the database engine).

**Currently only DuckDB and Clickhouse are supported.**

_Note: If you enjoy this project, you may also enjoy my other dbt machine learning project, [**dbt_linreg**](https://github.com/dwreeves/dbt_linreg)._ 😊

# Installation

dbt-core `>=1.4.0` is required to install `dbt_pca`.

Add this the `packages:` list your dbt project's `packages.yml`:

```yaml
  - package: "dwreeves/dbt_linreg"
    version: "0.0.1"
```

The full file will look something like this:

```yaml
packages:
  # ...
  # Other packages here
  # ...
  - package: "dwreeves/dbt_linreg"
    version: "0.0.1"
```

# Examples

### Simple example (wide input format)

The following example runs PCA on 5 columns `x1, x2, x3, x4, x5`, using data in the dbt model named `collinear_matrix`.

```sql
{{
  config(
    materialized="table"
  )
}}
select * from {{
  dbt_pca.pca(
    table=ref('collinear_matrix'),
    index='idx',
    columns=['x1', 'x2', 'x3', 'x4', 'x5'],
    ncomp=3
  )
}} as pca
```

| comp | col     | eigenvector            | eigenvalue          | coefficient          |
|------|---------|------------------------|---------------------|----------------------|
| 0    | x1      | 0.23766561884860857    | 29261.18329671314   | 40.6548443559801     |
| 0    | x2      | -0.5710963390821377    | 29261.183296713134  | -97.69117169801471   |
| 0    | x3      | -0.5730424984614262    | 29261.18329671314   | -98.02407978560524   |
| 0    | x4      | -0.5375734671620216    | 29261.18329671314   | -91.9567825723166    |
| 0    | x5      | 0.0010428157925962138  | 29261.183296713138  | 0.17838303220022225  |
| 1    | x1      | -0.0662409860256577    | 10006.031828705965  | -6.626096072806324   |
| 1    | x2      | -0.001674528192609409  | 10006.031828705967  | -0.16750331398380647 |
| 1    | x3      | -0.0027280948128929504 | 10006.031828705962  | -0.27289174588904075 |
| 1    | x4      | -0.022663548107992145  | 10006.031828705964  | -2.2670382209597086  |
| 1    | x5      | 0.9975411013143917     | 10006.031828705964  | 99.78419058137138    |
| 2    | x1      | 0.9637584043866467     | 8920.76195540799    | 91.02677443759194    |
| 2    | x2      | 0.09591639009268058    | 8920.761955407994   | 9.059282457195334    |
| 2    | x3      | 0.10093338977915689    | 8920.761955407994   | 9.533137000756994    |
| 2    | x4      | 0.2167293438854099     | 8920.761955407994   | 20.470040012174913   |
| 2    | x5      | 0.06935867943078204    | 8920.761955407997   | 6.550912385405418    |

The default output for **dbt_pca** is eigenvectors + coefficients + eigenvalues, although you can configure to output principal components / factors and projections as well with the `output='...'` argument.

Also, `collinear_matrix` is one of the test cases, so you can try this yourself!

### Simple example (long input format)

The below example is equivalent to the previous example.

By setting an argument for `values=...`, **dbt_pca** will infer that your data is long-formatted.

```sql
{{
  config(
    materialized="table"
  )
}}

with preprocessed_data as (
  select idx, 'x1' as c, x1 as x
  union all
  select idx, 'x2' as c, x2 as x
  union all
  select idx, 'x3' as c, x3 as x
  union all
  select idx, 'x4' as c, x4 as x
  union all
  select idx, 'x5' as c, x5 as x
)
select * from {{
  dbt_pca.pca(
    table='preprocessed_data',
    index='idx',
    columns='c',
    values='x'
  )
}} as pca
```

Data that we want to run PCA on very often comes long-formatted by default.
For example, imagine a database containing movies, and movies can be tagged as being in multiple genres as an array.
We can imagine flattening the array column into a column of strings, doing some additional preprocessing and then running PCA where `rows='movie_id'`, `columns='genre'`, and `values='tagged_as_genre'`;
this would be a more natural way to define data for this particular PCA than widening the data.
See [Karl Rohe's `longpca`](https://github.com/karlrohe/longpca) for a longer (pun not intentional) discussion about this.

### Complex example

The below example uses the [**dbt_linreg**](https://github.com/dwreeves/dbt_linreg) alongside **dbt_pca**.

We have a table called `movies`, which contains ratings and tags for each movie in the database.

We want to see which movies over or under-perform based on their tags,
e.g. perhaps the tag `foo` usually indicates a poor quality movie, but some movies tagged with `foo` are exceptionally good.

We can do the following:

- Define a matrix with the following structure:
  - Rows are movies
  - Columns are tags
  - Cells are `1` if movie has the tag, otherwise `0`
- Use `output='factors-wide'` to output principal components in a CTE with the following columns: `id`, `comp_0`, `comp_1`, `comp_2`, `comp_3`, `comp_4`.
- Use the principal components as features in a linear regression.
- Predict the movie rating using the regression coefficients.
- Return the predictions sorted by `abs(residual) desc`.

Here is what that would look like:

```sql
{{
  config(
    materialized="table"
  )
}}

with

movie_tags as (

  select
    id,
    unnest(tags) as tag,
    1 as has_tag
  from {{ ref("movies") }}

),

pca as (
  select * from {{
    dbt_pca.pca(
      table='movie_tags',
      index='id',
      columns='tag',
      values='has_tag',
      missing='zero',
      output='factors-wide',
      ncomp=5
    )
  }}

),

movie_rating_with_principal_components as (

  select
    m.id,
    m.name,
    m.rating,
    c.comp_0,
    c.comp_1,
    c.comp_2,
    c.comp_3,
    c.comp_4
  from {{ ref("movies") }} as m

),

linear_regression_coefficients as (

  select * from {{
    dbt_linreg.ols(
      table='movie_rating_with_principal_components',
      endog='rating',
      exog=['comp_0', 'comp_1', 'comp_2', 'comp_3', 'comp_4']
    )
  }}

),

predictions as (

  select
    m.id,
    m.name,
    m.rating as actual_rating,
    l.const
      + m.comp_0 * l.comp_0
      + m.comp_1 * l.comp_1
      + m.comp_2 * l.comp_2
      + m.comp_3 * l.comp_3
      + m.comp_4 * l.comp_4
    as predicted_rating,
    predicted_rating - actual_rating as residual
  from
    movie_rating_with_principal_components as m,
    linear_regression_coefficients as l

)

select *
from predictions
order by abs(residual) desc
```

Of course, there are many other things you can do with **dbt_pca** than just the above.
Hopefully this example inspires you to explore all the possibilities!

# Supported Databases

**dbt_pca** works with the following databases:

| Database       | Supported | Precision asserted in CI\* | Supported since version |
|----------------|-----------|----------------------------|-------------------------|
| **DuckDB**     | ✅         | 10e-7                      | 0.1.0                   |
| **Clickhouse** | ✅         | 10e-6                      | 0.1.0                   |

Please see the **Performance optimization** section on how to get the best performance out of each database.

**dbt_pca** does not currently work with Snowflake, unfortunately due to limitations to Snowflake that are hard to overcome.
My goal is to have this working in Snowflake as well, but this has proven difficult to do.
Please check back soon.

# API

### `{{ dbt_pca.pca() }}`

This is the core function of the **dbt_pca** package. Using Python typing notation, the full API for `dbt_pca.pca()` looks like this:

```python
def pca(
    table: str,
    index: str | list[str] | None = None,
    columns: str | list[str] | None = None,
    values: str | None = None,
    ncomp: int | None = None,
    normalize: bool = True,
    standardize: bool = True,
    demean: bool = True,
    # missing: Literal[None] = None,
    # weights: Literal[None] = None,
    output: Literal['loadings'] = 'loadings',
    output_options: dict[str, Any] | None = None,
    method: Literal['nipals'] = 'nipals',
    method_options: dict[str, Any] | None = None
):
    ...
```

Where:

- **table**: Name of table or CTE to pull the data from. You can use a `ref()`, `source()`, a CTE name, or subquery.
- **index**: The uniquely identifying index for each row of data. You may define one or more columns as the index. This field is **required** if your data is long or you want to output components. (You can also specify `rows=...` instead of `index=...` if you prefer.)
- **columns**: Either a list of columns (for wide-formatted data), or a uniquely identifying index for each column of data (for long-formatted data). You may specify multiple columns even for long-formatted data; in this case the multiple columns will be treated like a multi-index.
- **values**: Specifying this will make **dbt_pca** treat your data as long-formatted. This field defines the column in your table corresponding with the cell values of the matrix.
- **ncomp**: Number of components to return. If `none`, it is set to the number of columns in data if the data is wide-formatted; if data is long-formatted, this must be set. **It is strongly recommended you set the number of components for large data sets**. Most database implementations (except DuckDB) require `ncomp` to be set for long-formatted data.
- **normalize**: Indicates whether to normalize the factors to have unit inner product. If False, the loadings will have unit inner product.
- **standardize**: Flag indicating to use standardized data with mean 0 and unit variance. standardized being True implies demean. Using standardized data is equivalent to computing principal components from the correlation matrix of data.
- **demean**: Flag indicating whether to demean data before computing principal components. demean is ignored if standardize is True. Demeaning data but not standardizing is equivalent to computing principal components from the covariance matrix of data.
- **missing**: _Does nothing currently._
- **weights**: _Does nothing currently._
- **output**: See **Outputs and output options** section of the README for more. This can be one of the following:
  - `'loadings'`
  - `'loadings-long'`
  - `'loadings-wide'`
  - `'eigenvectors-wide'`
  - `'eigenvectors-wide-transposed'`
  - `'coefficients-wide'`
  - `'coefficients-wide-transposed'`
  - `'factors'`
  - `'factors-long'`
  - `'factors-wide'`
  - `'projections'`
  - `'projections-long'`
  - `'projections-wide'`
  - `'projections-untransformed-wide'`
- **output_options**:  See **Outputs and output options** section of the README for more.
- **method**: The method used to calculate the regression. Currently only `'nipals'` is supported. See **Methods and method options** for more.
- **method_options**: Options specific to the estimation method. See **Methods and method options** for more.

Names for function arguments and concepts vary across PCA implementations in different languages and frameworks.
**In this library, all names and concepts are equivalent to those in Statsmodels.**

### `{{ config(materialized='pca') }}`

**dbt_pca** comes with an optional materialization method called `'pca'`.

The `'pca'` materialization is recommended if:

- Almost always if are using Clickhouse.

The `'pca'` is _**not**_ recommended if:

- You are running DuckDB, and haven't yet run into any issues with dbt's built-in materializations like `'table'`, `'incremental'`, etc.

This materialization method bypasses some runtime performance limitations (the most notable being Clickhouse not materializing CTEs) by generating components as a series of tables written to the database.
So when this materialization runs with `dbt run`, it actually writes a lot of tables to generate the components instead of doing it in a single shot.

For the most part, the `'pca'` materialization can do the same things that materializing as a table can. The most notable exception is the `table=...` passed to `dbt_pca.pca()` (i.e. first arg) _must_ be either a `ref()` or a `source()`, and additionally the `ref()` cannot be to an ephemeral model.

# Outputs and output options

<table>
    <thead>
        <th>Output type</th>
        <th>Description</th>
        <th>Uniquely identifying columns</th>
        <th>Other columns</th>
        <th>Restrictions</th>
        <th>Notes</th>
    </thead>
    <tbody>
        <tr>
            <td><code>'loadings'</code></td>
            <td>Returns the eigenvectors (i.e. loadings), eigenvalues, and coefficients.</td>
            <td><code>comp</code>, <code>[columns]</code></td>
            <td><code>eigenvector</code>, <code>eigenvalue</code>, <code>coefficient</code></td>
            <td>-</td>
            <td>-</td>
        </tr>
        <tr>
            <td><code>'loadings-long'</code></td>
            <td>-</td>
            <td><code>comp</code>, <code>[columns]</code></td>
            <td><code>eigenvector</code>, <code>eigenvalue</code>, <code>coefficient</code></td>
            <td>All</td>
            <td>Alias for <code>'loadings'</code>.</td>
        </tr>
        <tr>
            <td><code>'loadings-wide'</code></td>
            <td>Same as <code>'loadings'</code>, but wide.</td>
            <td><code>[columns]</code></td>
            <td><code>eigenvector_{i}</code>, <code>coefficient_{i}</code></td>
            <td>-</td>
            <td>-</td>
        </tr>
        <tr>
            <td><code>'eigenvectors-wide'</code></td>
            <td>Same as <code>'loadings-wide'</code>, except only display eigenvectors.</td>
            <td><code>[columns]</code></td>
            <td><code>eigenvector_{i}</code></td>
            <td>-</td>
            <td>Alias for <code>'loadings-wide'</code> with <code>output_options['display_coefficients'] = false</code>.</td>
        </tr>
        <tr>
            <td><code>'eigenvectors-wide-transposed'</code></td>
            <td>Same as <code>'eigenvectors-wide'</code>, except transposed.</td>
            <td><code>comp</code></td>
            <td><code>[columns]</code></td>
            <td>Wide input format only</td>
            <td>-</td>
        </tr>
        <tr>
            <td><code>'coefficients-wide'</code></td>
            <td>Same as <code>'loadings-wide'</code>, except only display coefficients.</td>
            <td><code>[columns]</code></td>
            <td><code>coefficient_{i}</code></td>
            <td>-</td>
            <td>Alias for <code>'loadings-wide'</code> with <code>output_options['display_eigenvectors'] = false</code>.</td>
        </tr>
        <tr>
            <td><code>'coefficients-wide-transposed'</code></td>
            <td>Same as <code>'coefficients-wide'</code>, except transposed.</td>
            <td><code>comp</code></td>
            <td><code>[columns]</code></td>
            <td>Wide input format only</td>
            <td>-</td>
        </tr>
        <tr>
            <td><code>'factors'</code></td>
            <td>Returns the principal components (i.e. factors) in a long format.</td>
            <td><code>comp</code>, <code>[index]</code></td>
            <td><code>factor</code></td>
            <td>Requires an <code>index=...</code> to be defined.</td>
            <td>-</td>
        </tr>
        <tr>
            <td><code>'factors-long'</code></td>
            <td>-</td>
            <td><code>comp</code>, <code>[index]</code></td>
            <td><code>factor</code></td>
            <td>Requires an <code>index=...</code> to be defined.</td>
            <td>Alias for <code>'factors'</code>.</td>
        </tr>
        <tr>
            <td><code>'factors-wide'</code></td>
            <td>Returns the principal components (i.e. factors) in a wide format.</td>
            <td><code>[index]</code></td>
            <td><code>factor_{i}</code></td>
            <td>-</td>
            <td>-</td>
        </tr>
        <tr>
            <td><code>'projections'</code></td>
            <td>Returns projections in a long format.</td>
            <td><code>[columns]</code>, <code>[index]</code></td>
            <td><code>projection</code></td>
            <td>Requires an <code>index=...</code> to be defined.</td>
            <td>-</td>
        </tr>
        <tr>
            <td><code>'projections-long'</code></td>
            <td>-</td>
            <td><code>[columns]</code>, <code>[index]</code></td>
            <td><code>projection</code></td>
            <td>Requires an <code>index=...</code> to be defined.</td>
            <td>Alias for <code>'projections'</code>.</td>
        </tr>
        <tr>
            <td><code>'projections-wide'</code></td>
            <td>Returns projections in a wide format (each column is original column of the data.)</td>
            <td><code>[index]</code></td>
            <td><code>[columns]</code></td>
            <td>Wide input format only</td>
            <td>-</td>
        </tr>
        <tr>
            <td><code>'projections-untransformed-wide'</code></td>
            <td>Returns untransformed projections.</td>
            <td><code>[index]</code></td>
            <td><code>[columns]</code></td>
            <td>Wide input format only</td>
            <td>This is niche and you probably don't need this.</td>
        </tr>
    </tbody>
</table>

## Output options

### Column names

- **columns_column_name** (`string`; default: `'col'`): When converting a wide input to a long output, this is the column name used to group all the columns together.
- **eigenvector_column_name** (`string`; default: `'eigenvector'`): Column name for eigenvectors i.e. loadings.
  - In long formatted outputs, the column `{{eigenvector_column_name}}` contains the values of the eigenvectors.
  - In wide formatted outputs, there will be multiple columns `{{eigenvector_column_name}}_{{i}}`, where `i` is an index for the principal component.
- **eigenvalue_column_name** (`string`; default: `'eigenvalue'`): Column name for eigenvalues.
- **coefficient_column_name** (`string`; default: `'coefficient'`): Column names for coefficient i.e. `eigenvector * sqrt(eigenvalue)`.
  - In long formatted outputs, the column `{{coefficient_column_name}}` contains the values of the coefficients.
  - In wide formatted outputs, there will be multiple columns `{{coefficient_column_name}}_{{i}}`, where `i` is an index for the principal component.
- **component_column_name** (`string`; default: `'comp'`): Identifier for principal component; this is an integer typed column that identifies the component (e.g. if there are 3 components, then `{{component_column_name}}` can take on values of `0`, `1`, and `2`).
- **factor_column_name** (`string`; default: `'factor'`): Column name for factors i.e. principal components.
  - In long formatted outputs, the column `{{factor_column_name}}` contains the values of the principal component vectors.
  - In wide formatted outputs, there will be multiple columns `{{factor_column_name}}_{{i}}`, where `i` is an index for the principal component.
- **projection_column_name** (`string`; default: `'projection'`): Column name for projections of data onto the principal components.

### Column display

- **display_eigenvalues** (`bool`; default = `True`): If True, display eigenvalues in loadings output.
- **display_coefficients** (`bool`; default = `True`): If True, display coefficients in loadings output.

### Other

- **strip_quotes** (`bool`; default = `True`): If true, strip outer quotes from column names in long outputs; if false, always use string literals.

## Setting output options globally

Output options can be set globally via `vars`, e.g. in your `dbt_project.yml`:

```yaml
# dbt_project.yml
vars:
  dbt_pca:
    output_options:
      eigenvector_column_name: loading
      eigenvalue_column_name: eigenval
      coefficient_column_name: coeff
```

# Methods and method options

There is currently only one method for calculating PCA, `'nipals'`, and I currently do not have plans to implement more as frankly it's taken years off my life to just implement the one. 😆

## `nipals` method

Nonlinear Iterative Partial Least Squares (NIPALS), a method that is optimized for calculating the first few PCs of a matrix but is less performant and less accurate for the last few PCs.
In practical settings, we usually want far fewer components than there are columns in the data, so this ends up being a good trade-off.

Another advantage of NIPALS is it can be modified to handle missing data.
This is a common method in many implementations of PCA, although it is not currently supported. (It's on the roadmap!)

### Options for `method='nipals'`

Specify these in a dict using the `method_options=` kwarg:

- **max_iter** (`int`; default: varies) - Maximum iterations of the NIPALS algorithm.
- **check_tol** (`bool`; default: varies) - If True, then check for convergence using the `tol` value. If False, always iterate `max_iter` times.
- **tol** (`bool`; default: `5e-8`) - Tolerance to use when checking for convergence.
- **deterministic_column_seeding** (`bool`; default: `False`) - If True, seed the initial column in a factor calculation with the alphanumerically first column. This guarantees the sign of the eigenvector even when normalized, but is significantly slower to converge. It is strongly recommended to keep this turned off.

Some default values vary based on the database engine being used, and whether `materialized='pca'` is set.

## Setting method options globally

Method options can be set globally via `vars`, e.g. in your `dbt_project.yml`. Each `method` gets its own config, e.g. the `dbt_pca: method_options: nipals: ...` namespace only applies to the `nipals` method. Here is an example:

```yaml
# dbt_project.yml
vars:
  dbt_pca:
    method_options:
      nipals:
        max_iter: 300
```

# Performance optimization

**Please [open an issue on Github](https://github.com/dwreeves/dbt_pca/issues) if you experience any performance problems.**
I am still ironing things out a bit.

### DuckDB performance optimization

The performance for DuckDB is blazing fast, and users should generally not have any issues with DuckDB.
In testing, values are asserted to equal the Statsmodels implementation of PCA with a very high level of precision,
and the test cases run very quickly.

In DuckDB, it is generally recommended you do **_not_** start with the `'pca'` materialization unless you run into issues.

### Clickhouse performance optimization

The most performant way to run `{{ dbt_pca.pca() }}` inside Clickhouse is to use the `'pca'` materialization:

```sql
{{
  config(
    materialized='pca'
  )
}}
```

**Without the `'pca'` materialization method, `pca()` still works in Clickhouse, but the implementation gets parabolically slower as `ncomp` increases.**

Clickhouse also has some other performance considerations.
The `max_iter` is set to 150 (100 without `materialized='pca'`), which is lower than in DuckDB and achieves an accuracy of `10e-6`, which is also lower than DuckDB.
This `max_iter` was chosen to play nicely with Clickhouse's default `max_recursive_cte_evaluation_depth` of 1000, however you can adjust this through a pre-hook:

```sql
{{
  config(
    materialized='pca',
    pre_hook='set max_recursive_cte_evaluation_depth = 5000;'
  )
}}

select * from {{
  dbt_pca.pca(
    table=ref('my_table'),
    index='a_id',
    columns='b_id',
    values='x',
    ncomp=10,
    method_options={'max_iter': 400}
  )
}}
```

# FAQ

### How does this work?

PCA via the NIPALS method can be implemented with nested recursive CTEs.
The `pca()` macro generates SQL containing a fairly straightforward implementation of the [steps outlined here](https://cran.r-project.org/web/packages/nipals/vignettes/nipals_algorithm.html).
Unlike my other library [**dbt_linreg**](https://github.com/dwreeves/dbt_linreg) no Jinja2 trickery is required for the core implementation.
I am a little surprised myself that I could not find any attempts at implementing this online, even though the does not require

All approaches were validated using Statsmodels `sm.PCA()`.

### Should I pre-process my wide data into long data (or vice-versa)?

**dbt_pca**'s implementation of PCA is optimized for long-formatted data. That said, I generally recommend not bothering to preprocess your data into long format if it's naturally wide, unless you have a good reason. **dbt_pca**'s wide-to-long conversion is perfectly optimal as-is, so just do whatever is easiest.

Definitely **do not** take data that is already long-formatted and convert it into wide-formatted data! Because **dbt_pca** already works best with long-formatted data, this would be terribly inefficient.

# Development

## Running tests

Run tests with `./run test {target}`.

The default target is `duckdb`, which runs locally.

The `clickhouse` target requires Docker.

# Trademark & Copyright

dbt is a trademark of dbt Labs.

This package is **unaffiliated** with dbt Labs.
