> [!WARNING]
> This project is in **beta** and is not fully publicly released.
> The API is subject to breakage and bugs may occur.
> Please wait until **0.1.0** for full release.
>
> The following features are currently missing and are being prioritized for a **0.1.0** release:
> - Missing value support (but probably not the EM algorithm algorithm for the time being).
> - ~~Support for weights~~
> - ~~Snowflake support~~
> - (Maybe) Fuller, paginated documentation on Github Pages.
> - ~~Remove custom materialization~~

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
- 💪 **Durable and tested:** Everything in this code base is tested against equivalent PCA's performed in Statsmodels with high precision assertions (between 10e-5 to 10e-7, depending on the database engine).
- 🪄 **Deep under-the-hood magic:** In the pursuit of making this as easy as possible to implement across databases, tons of dbt dark magic wizardry happens under the hood. I spent weeks of my life so you could save seconds of yours!

**Currently only DuckDB, Clickhouse, and Snowflake are supported.**

_Note: If you enjoy this project, you may also enjoy my other dbt machine learning project, [**dbt_linreg**](https://github.com/dwreeves/dbt_linreg)._ 😊

# Supported Databases

**dbt_pca** works with the following databases:

| Database       | Supported | Precision asserted in CI\* | Supported since version |
|----------------|-----------|----------------------------|-------------------------|
| **DuckDB**     | ✅         | 10e-7                      | 0.1.0                   |
| **Clickhouse** | ✅         | 10e-5                      | 0.1.0                   |
| **Snowflake**  | ✅         | 10e-7                      | 0.1.0                   |

Please see the **Performance optimization** section on how to get the best performance out of each database.

> _\* Precision is comparison to `PCA(method='nipals')` in Statsmodels. In some cases, precision depends on the implementation method; precision is based on suggested implementation._

# Installation

dbt-core `>=1.4.0` is required to install `dbt_pca`.

Add this the `packages:` list your dbt project's `packages.yml`:

```yaml
  - package: "dwreeves/dbt_pca"
    version: "0.0.4"
```

The full file will look something like this:

```yaml
packages:
  # ...
  # Other packages here
  # ...
  - package: "dwreeves/dbt_pca"
    version: "0.0.4"
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

Here is a Snowflake specific example using Cortex vector embeddings to construct principal components over an embedding space.
The resulting table of components (using `output='factors-wide'`) can then be used as features in a machine learning model.
(This is very similar to something I am doing right now at my current company!)

```sql
{{
  config(
    materialized="table"
  )
}}

with

base as (

  select
    id,
    /* Note: it is STRONGLY recommended you precompute embeddings in a separate incremental materialization,
       since the cost of rerunning this function can be quite high. */
    snowflake.cortex.embed_text_768('snowflake-arctic-embed-m', description) as description_embedding
  from {{ ref("entity") }}

),

reshaped_embeddings as (

    select
      b.id,
      v.index as embedding_index,
      v.value::float as cell
    from base as b,
    lateral flatten(input => b.description_embedding::array) as v

)

select *
from {{ dbt_pca.pca(
  table='reshaped_embeddings',
  columns='embedding_index::integer',
  values='cell',
  index='id::integer',
  ncomp=10,
  output='factors-wide'
) }}
order by id
```

Of course, there are many other things you can do with **dbt_pca** than just the above.
Hopefully this example inspires you to explore all the possibilities!

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
    weights: list[float | int] | None = None,
    output: str = 'loadings',
    output_options: dict[str, Any] | None = None,
    method: Literal['nipals'] = 'nipals',
    method_options: dict[str, Any] | None = None,
    materialization_options: dict[str, Any] | None = None
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
- **weights**: Column weights to use after transforming data according to standardize or demean when computing the principal components.
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
- **method**: The method used to calculate the regression. Currently only `'nipals'` is supported in non-Snowflake databases; `'eig'` and `'svd'` are additionally supported in Snowflake. See **Methods and method options** for more.
- **method_options**: Options specific to the estimation method. See **Methods and method options** for more.
- **materialization_options**: Database-specific options relating to how the table is materialized. See **Materialization options** for more.

Names for function arguments and concepts vary across PCA implementations in different languages and frameworks.
**In this library, all names and concepts are equivalent to those in Statsmodels.**

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

There is currently only one method for calculating PCA in pure SQL, `'nipals'`, and I currently do not have plans to implement more as frankly it's taken years off my life to just implement the one. 😆

Because Snowflake is just a pass-through to `sm.PCA()`, the Snowflake implementation supports everything Statsmodels supports: `'nipals'`, `'svd'`, and `'eig'`.

## `nipals` method

Nonlinear Iterative Partial Least Squares (NIPALS), a method that is optimized for calculating the first few PCs of a matrix but is less performant and less accurate for the last few PCs.
In practical settings, we usually want far fewer components than there are columns in the data, so this ends up being a good trade-off.

Another advantage of NIPALS is it can be modified to handle missing data.
This is a common method in many implementations of PCA, although it is not currently supported in the pure SQL implementations. (It's on the roadmap!)

### Options for `method='nipals'`

Specify these in a dict using the `method_options=` kwarg:

- **max_iter** (`int`; default: varies) - Maximum iterations of the NIPALS algorithm.
- **check_tol** (`bool`; default: varies) - If True, then check for convergence using the `tol` value. If False, always iterate `max_iter` times.
- **tol** (`bool`; default: `5e-8`) - Tolerance to use when checking for convergence.
- **deterministic_column_seeding** (`bool`; default: `False`) - If True, seed the initial column in a factor calculation with the alphanumerically first column. This guarantees the sign of the eigenvector even when normalized, but is significantly slower to converge. It is strongly recommended to keep this turned off.

Some default values vary based on the database engine being used, and whether `calculate_in_steps` is enabled.

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

# Materialization options

Materialization options influences the behind-the-scenes stuff relating to how the dbt model runs.

Materialization options can be set globally via `vars`, e.g. in your `dbt_project.yml`:

```yaml
# dbt_project.yml
vars:
  dbt_pca:
    materialization_options:
      udf_database: my_udf_db
      udf_schema: my_udf_schema
      drop_udf: false
```

## Snowflake

A lot of magic happens under the hood to make the Snowflake implementation possible.
The defaults should work fine, but a lot of things are exposed to the user via the `materialization_options` to give the user control over things.

Basically, the Snowflake implementation cheats by wrapping `sm.PCA()`.
This is implemented as a user-defined table function under the hood, which is created as pre-hook which is injected into the model config as a side-effect of calling `dbt_pca.pca()`.
The pre-hook is called lazily by using the JSON config injected into the compiled model.
**dbt_pca** then reads the schema of the referenced node in the `table=` arg to infer the types for the function inputs and outputs.

With all of that out of the way, here are the materialization options available for the Snowflake implementation:

- `udf_database`: (`string`; default: use the model's database) - The database where the UDF is written to.
- `udf_schema`: (`string`; default: use the model's schema) - The schema where the UDF is written to.
- `infer_function_signature_types` (`bool`; default: `True`) - If True, infer types of the UDF function signature based on the types of the upstream node. If False, use `float`
- `cast_types_to_udf` (`bool`; default: `False` if `infer_function_signature_types` is `True`, otherwise `True`) - If True, all columns are cast before being placed into the UDF (e.g. `function(foo::number)` instead of `function(foo)`).
- `drop_udf` (`bool`; default: `True`) - If True, drop the UDF after running the model (as a post-hook). If False, do not drop the UDF.

The below options you probably will not need (it is suggested you instead cast the type in the definition, e.g. `index=['id::integer']`), but are available just in case the above options are insufficient:

- `column_types` (`list[str] | None`; default: `None`) - If set, take input types for the columns from this instead of automatically inferring them.
- `index_types` (`list[str] | None`; default: `None`) - If set, take input types for the index from this instead of automatically inferring them.
- `values_type` (`str | None`; default: `None`) - If set, take input types for the values from this instead of automatically inferring them.

## Clickhouse

- `calculate_in_steps` (`bool`; default: `True`) - If set, calculate the PCA in multiple steps: i.e. create temporary tables for each step involved. By default, this is `True` for Clickhouse for performance reasons; I will set this to `False` by default when Clickhouse releases [materialized CTEs](https://github.com/ClickHouse/ClickHouse/issues/53449) (for such supported versions of Clickhouse).

## Duckdb

- `calculate_in_steps` (`bool`; default: `False`) - If set, calculate the PCA in multiple steps: i.e. create temporary tables for each step involved. By default, this is `False` for Duckdb as it is unnecessary for performance.

# Performance and Misc. Notes

**Please [open an issue on Github](https://github.com/dwreeves/dbt_pca/issues) if you experience any performance problems.**
I am still ironing things out a bit.

### DuckDB

The performance for DuckDB is very fast, and users should generally not have any issues with DuckDB.
In testing, values are asserted to equal the Statsmodels implementation of PCA with a very high level of precision,
and the test cases run very quickly.

### Clickhouse

Clickhouse is made performant by calculating individual steps in temporary tables.
This is because Clickhouse does not support materialized CTEs (although this feature is scheduled to come out in 2025),
which means expensive calculations are calculated redundantly multiple times as the number of principal components increases.

By default, the `calculate_in_steps` materialization option is turned on for Clickhouse.
But `calculate_in_steps` only works when `table=...` is a `ref()` or `source()` to a non-ephemeral node.
For example, the following code works, but will be non-performant because it references a CTE:

```sql
with my_cte as (select * from {{ ref("my_table") }})

select * from {{ dbt_pca.pca(
    table="my_cte",
    index="index",
    columns="col",
    values="val",
    ncomp=10
) }}
```

It would be better to write the code like this, so long as `ref("my_table")` is not ephemeral (views are OK):

```sql
select * from {{ dbt_pca.pca(
    table=ref("my_table"),
    index="index",
    columns="col",
    values="val",
    ncomp=10
) }}
```

Clickhouse also has some other performance considerations.
The `max_iter` is set to 150 (100 without `calculate_in_steps`), which is lower than in DuckDB and achieves an accuracy of `10e-6`, which is also lower than DuckDB.
This `max_iter` was chosen to play nicely with Clickhouse's default `max_recursive_cte_evaluation_depth` of 1000, however you can adjust this through a pre-hook:

```sql
{{
  config(
    materialized='table',
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

### Snowflake

The Snowflake implementation cheats by wrapping `sm.PCA()` (there is no other way to do it, really).
Because it runs as a Python UDF, for heavy workloads, "Snowpark-optimized" warehouses are recommended.
These warehouses allow for additional memory to be allocated for a given warehouse size, relative to the amount of compute power of the warehouse instance.
Read more about Snowpark-optimized warehouses in the [Snowflake documentation](https://docs.snowflake.com/en/user-guide/warehouses-snowpark-optimized).

For the Snowflake implementation, column types can be cast explicitly and this casting will be used for the UDF definition.
This can be useful when referencing a CTE, from which column types cannot be inferred. (Column types are inferred for `ref()`'s and `source()`'s only.)
For example:

```sql
{{
  config(
    materialized='table',
  )
}}

with my_cte as (
  select id, column_a, column_b, column_c, column_d, column_e
  from {{ ref('upstream_table') }}
)

select * from {{
  dbt_pca.pca(
    table='my_cte',
    index='id::integer',
    columns=[
      'column_a::number(38, 8)',
      'column_b::number(38, 8)',
      'column_c::number(38, 8)',
      'column_d::number(38, 8)',
      'column_e::number(38, 8)',
    ],
    ncomp=2
  )
}}
```

If you reference a CTE and do not cast types yourself, then types will be cast to the most permissive types, i.e. `float`s for all columns used in the matrix, and `varchar`s for all other columns.
For example, in the above example, without explicit typecasting, the `index='id'` will be cast to a varchar:

```sql
{{
  config(
    materialized='table',
  )
}}

with my_cte as (
  select id, column_a, column_b, column_c, column_d, column_e
  from {{ ref('upstream_table') }}
)

-- Because table=... is a CTE and there are no explicit typecasts,
-- id will be cast to a varchar, and column_*'s will be cast to floats

select * from {{
  dbt_pca.pca(
    table='my_cte',
    index='id',
    columns=[
      'column_a',
      'column_b',
      'column_c',
      'column_d',
      'column_e',
    ],
    ncomp=2
  )
}}
```

Please note explicit type-casting of `columns` / `index` / `values` only works in Snowflake.

# Notes

## Possible future features

Some things that could happen in the future:

- Better support for weights. Weights are only supported right now with wide formatted inputs.

Note that this wish list is unlikely unless I personally need it or unless someone else contributes these features.
I will continue to maintain this project going forward, but this project was a lot of work to get where it is today, and adding new features is not a high priority in my life.

# FAQ

### How does this work?

PCA via the NIPALS method can be implemented in SQL with nested recursive CTEs. This is how PCA is implemented in DuckDB and Clickhouse.
The `pca()` macro generates SQL containing a fairly straightforward implementation of the [steps outlined here](https://cran.r-project.org/web/packages/nipals/vignettes/nipals_algorithm.html).
Unlike my other library [**dbt_linreg**](https://github.com/dwreeves/dbt_linreg) no Jinja2 trickery (so to speak) is required to build the SQL for the core implementation.

The Snowflake implementation just wraps Statsmodels directly as a UDTF, although it is formatted to have the exact same API as DuckDB and Clickhouse.

All approaches were validated using Statsmodels `sm.PCA()`.

One more note about the implementations for Clickhouse and Snowflake: these implementations use some cool pre-hook injection tricks to work.
Basically, when you call `{{ dbt_pca.pca() }}`, under the hood, a `config()` call occurs which injects a pre-hook that generates SQL to create temp tables (Clickhouse) or a Python UDF (Snowflake).
Due to weird limitations of how dbt works, these calculations require reading from a comment injected into the compiled SQL: basically, all the args to `pca()` are serialized as a JSON and then deserialized during pre-hook evaluation.
Amusingly and perhaps counterintuitively, I found that making this under-the-hood process smooth and user-friendly was a lot harder and more time consuming than the SQL-based PCA implementation itself.

### Should I pre-process my wide data into long data (or vice-versa)?

**dbt_pca**'s implementation of PCA is optimized for long-formatted data in DuckDB and Clickhouse, and optimized for wide-formatted data in Snowflake. That said, I generally recommend not bothering to preprocess your data into long format if it's naturally wide, unless you have a good reason. **dbt_pca**'s wide-to-long conversion is perfectly optimal as-is, so just do whatever is easiest.

# Development

## Running tests

Run tests with `./run test {target}`.

The default target is `duckdb`, which runs locally.

The `clickhouse` target requires Docker.

# Trademark & Copyright

dbt is a trademark of dbt Labs.

This package is **unaffiliated** with dbt Labs.
