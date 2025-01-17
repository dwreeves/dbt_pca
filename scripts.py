"""
This file is used for generation of CSV files for integration test cases,
and also for manual verification + generation of test case values,
and a few other things.

PLEASE NOTE:

This code is genuinely pretty bad and messy. 😅
It was rushed together to generate the test cases easily via the CLI.
It could be a lot nicer, I know.
"""
import json
import os
import os.path as op
import re
import sys
import warnings
from typing import NamedTuple
from typing import Optional
from typing import Protocol

import numpy as np
import pandas as pd
import rich_click as click
import yaml
from statsmodels.multivariate.pca import PCA
from tabulate import tabulate


# Suppress iteritems warning
warnings.simplefilter("ignore", category=FutureWarning)

# No scientific notation
np.set_printoptions(suppress=True)


DIR = op.dirname(__file__)

DEFAULT_SIZE = 10_000
DEFAULT_SEED = 9434874


class TestCase(NamedTuple):
    df: pd.DataFrame
    index_column: Optional[str] = None
    weights: Optional[pd.Series] = None


class TestCaseCallable(Protocol):
    def __call__(self, size: int, seed: int) -> TestCase:
        ...


def collinear_matrix(size: int = DEFAULT_SIZE, seed: int = DEFAULT_SEED) -> TestCase:
    rs = np.random.RandomState(seed=seed)
    df = pd.DataFrame(index=range(size))
    df["idx"] = df.index
    df["x1"] = 2 + rs.normal(0, 1, size=size)
    df["x2"] = 1 - df["x1"] + rs.normal(0, 3, size=size)
    df["x3"] = 3 + 2 * df["x2"] + rs.normal(0, 1, size=size)
    df["x4"] = -3 + 0.5 * (df["x1"] * df["x3"]) + rs.normal(0, 1, size=size)
    df["x5"] = 4 + 0.5 * np.sin(3 * df["x2"]) + rs.normal(0, 1, size=size)

    weights = pd.Series([1, 3, 3, 2, 4], index=["x1", "x2", "x3", "x4", "x5"])

    return TestCase(df=df, index_column="idx", weights=weights)


def missing_data_matrix():
    pass


def write_sql_of_dataframe(df: pd.DataFrame) -> str:
    rows = []
    for row in df.itertuples():
        cells = [
            f"  {getattr(row, c)!r} as {c}"
            for c in df.columns
        ]
        rows.append(",\n".join(cells))
    sql = "\nunion all\n".join([f"select\n{row}" for row in rows])
    return "with expected as (\n  " + sql.replace("\n", "\n  ") + "\n)\n\nselect * from expected"


ALL_TEST_CASES: dict[str, TestCaseCallable] = {
    "collinear_matrix": collinear_matrix,
}


def click_option_seed(**kwargs):
    return click.option(
        "--seed", "-s",
        default=DEFAULT_SEED,
        show_default=True,
        help="Seed used to generate data.",
        **kwargs
    )


def click_option_size(**kwargs):
    return click.option(
        "--size", "-n",
        default=DEFAULT_SIZE,
        show_default=True,
        help="Number of rows to generate.",
        **kwargs
    )


@click.group("main")
def cli():
    """CLI for manually testing the code base."""


@cli.command("pca")
@click.option("--table", "-t",
              required=True,
              type=click.Choice(list(ALL_TEST_CASES.keys())),
              help="Table to regress against.")
@click.option("--weights/--no-weights",
              default=False,
              type=click.BOOL,
              show_default=True,
              help="If true, use weights. NOTE: Weights not currently supported in ")
@click.option("--columns", "-c",
              default=None,
              type=click.INT,
              show_default=True,
              help="Number of columns to use.")
@click.option("--demean/--no-demean", "-d",
              default=True,
              type=click.BOOL,
              show_default=True,
              help="If true, demean the data in PCA() call.")
@click.option("--normalize/--no-normalize",
              default=True,
              type=click.BOOL,
              show_default=True,
              help="If true, normalize the data in PCA() call.")
@click.option("--standardize/--no-standardize",
              default=True,
              type=click.BOOL,
              show_default=True,
              help="If true, standardize the data in PCA() call.")
@click.option("--missing", "-m",
              default=None,
              type=click.Choice(["drop-row", "drop-col", "drop-min", "fill-em"]),
              show_default=True,
              help="Method for handling missing data.")
@click.option("--ncomp", "-c",
              default=None,
              type=click.INT,
              help="If set, select a specific number of components to calculate.")
@click.option("--method", "-m",
              default="nipals",
              type=click.Choice(["nipals"]),
              show_default=True,
              help="Select a method for computing PCA. Only NIPALS supported today.")
@click.option("--missing",
              default=None,
              type=click.Choice(["drop-row", "drop-col", "drop-min", "fill-em", "zero"]),
              show_default=True,
              help="Select a method for computing PCA. Only NIPALS supported today.")
@click.option("--tol", "-t",
              default=5e-8,
              type=click.FLOAT,
              show_default=True,
              help="Tolerance for convergence with NIPALS.")
@click_option_size()
@click_option_seed()
@click.option("--output-sql",
              type=click.Choice([
                  "eigenvectors",
                  "eigenvectors-wide",
                  "eigenvectors-wide-transposed",
                  "coefficients-wide-transposed",
                  "factors",
                  "factors-wide",
                  "projections",
                  "projections-wide",
                  "projections-untransformed-wide"
              ]),
              default=False,
              help="If set, output as sql")
def pca_(
        table: str,
        weights: bool,
        columns: Optional[int],
        demean: bool,
        normalize: bool,
        standardize: bool,
        missing: Optional[str],
        ncomp: int,
        method: str,
        size: int,
        seed: int,
        tol: float,
        output_sql: str
):
    """
    Generate integration test cases.

    (All numeric values for test cases were generated using this CLI.)
    """
    callback = ALL_TEST_CASES[table]

    click.echo(click.style("=" * 80, fg="blue"), file=sys.stderr)
    click.echo(
        click.style("Test case: ", fg="blue", bold=True)
        +
        click.style(table, fg="blue"), file=sys.stderr
    )
    click.echo(click.style("=" * 80, fg="blue"), file=sys.stderr)

    test_case = callback(size, seed)

    if columns is None:
        cols = test_case.df.columns
        wts = test_case.weights
    else:
        cols = test_case.df.columns[:columns]
        wts = test_case.weights.iloc[:columns]

    pca = PCA(
        test_case.df[
            [i for i in cols if i not in {test_case.index_column}]
        ],
        ncomp=ncomp,
        method=method,
        demean=demean,
        standardize=standardize,
        normalize=normalize,
        weights=wts if weights else None,
        missing=missing,
        tol=tol
    )
    eigenvalues = pd.Series(
        dict(zip(pca.loadings.columns, pca.eigenvals)),
        name="eigenvalue"
    ).to_frame()
    if output_sql in {"eigenvectors", "eigenvectors-wide", "eigenvectors-wide-transposed"}:
        res = pca.loadings.stack().reset_index()
        res = res.rename(columns={"level_0": "col", "level_1": "comp", 0: "eigenvector"})
        res = res.merge(right=eigenvalues, right_index=True, left_on="comp")
        res["comp"] = res["comp"].apply(lambda _: int(re.search(r"comp_(\d+)", _).group(1)))
        res = res.sort_values(["comp", "col"])
        if output_sql == "eigenvectors-wide":
            res: pd.DataFrame = res.pivot(index="col", columns="comp", values="eigenvector")
            res.columns = [f"comp_{i}" for i in res.columns]  # noqa
            coeffs = pca.coeff.T
            coeffs.columns = [c.replace("comp_", "coefficient_") for c in coeffs.columns]  # noqa
            coeffs.index.name = "col"
            res = res.merge(coeffs, left_index=True, right_index=True)
            res = res.reset_index()
            print(write_sql_of_dataframe(res))
        elif output_sql == "eigenvectors-wide-transposed":
            res = res.pivot(index="comp", columns="col", values="eigenvector").reset_index()
            print(write_sql_of_dataframe(res))
        else:
            print(write_sql_of_dataframe(res[["comp", "col", "eigenvector", "eigenvalue"]]))
    elif output_sql == "coefficients-wide-transposed":
        res = pca.coeff
        res.index.name = "comp"
        res = res.reset_index()
        res["comp"] = res["comp"].apply(lambda _: int(re.search(r"comp_(\d+)", _).group(1)))
        print(write_sql_of_dataframe(res))
    elif output_sql == "factors":
        res = pca.factors.stack().reset_index().rename(columns={"level_0": "idx", "level_1": "comp", 0: "factor"})
        res["comp"] = res["comp"].apply(lambda _: int(re.search(r"comp_(\d+)", _).group(1)))
        res = res.groupby("comp").head(5)
        print(write_sql_of_dataframe(res))
    elif output_sql == "factors-wide":
        res = pca.factors.copy()
        res.index.name = "idx"
        res.columns = [i.replace("comp_", "factor_") for i in res.columns]  # noqa
        res = res.reset_index().head(n=10)
        print(write_sql_of_dataframe(res))
    elif output_sql == "projections":
        res = pca.projection.stack().reset_index().rename(
            columns={"level_0": "idx", "level_1": "col", 0: "projection"}
        )
        res = res.groupby("col").head(5)
        print(write_sql_of_dataframe(res))
    elif output_sql == "projections-wide":
        res = pca.projection.stack().reset_index().rename(
            columns={"level_0": "idx", "level_1": "col", 0: "projection"}
        )
        res = res.pivot(index="idx", columns="col", values="projection").reset_index()
        res = res.head(n=10)
        print(write_sql_of_dataframe(res))
    elif output_sql == "projections-untransformed-wide":
        res = pca.project(transform=False).stack().reset_index().rename(
            columns={"level_0": "idx", "level_1": "col", 0: "projection"}
        )
        res = res.pivot(index="idx", columns="col", values="projection").reset_index()
        res = res.head(n=10)
        print(write_sql_of_dataframe(res))
    else:
        echo_table_name("Factors (first 10 rows)")
        click.echo(
            tabulate(
                pca.factors.head(n=10),
                headers=pca.factors.columns,
                disable_numparse=True,
                tablefmt="psql",
            )
        )
        echo_table_name("Loadings")
        click.echo(
            tabulate(
                pca.loadings,
                headers=pca.loadings.columns,
                disable_numparse=True,
                tablefmt="psql",
            )
        )
        echo_table_name("Eigenvalues")
        click.echo(
            tabulate(
                eigenvalues,
                headers=["eigenvalues"],
                disable_numparse=True,
                tablefmt="psql",
            )
        )


def echo_table_name(s: str):
    click.echo(click.style("=" * 80, fg="green"))
    click.echo(
        click.style("Table: ", fg="green", bold=True)
        +
        click.style(s, fg="green")
    )
    click.echo(click.style("=" * 80, fg="green"))


@cli.command("gen-test-cases")
@click.option("--table", "-t", "tables",
              multiple=True,
              default=None,
              show_default=True,
              help="Generate a specific table. If None, generate all tables.")
@click_option_size()
@click_option_seed()
@click.option("--skip-if-exists",
              is_flag=True,
              help="Skip if the file exists. Otherwise, overwrite.")
def gen_test_cases(tables: list[str], size: int, seed: int, skip_if_exists: bool):
    """Generate integration test cases (CSV files)."""
    if not tables:
        tables = ALL_TEST_CASES
    for table_name in tables:
        file_name = f"{DIR}/integration_tests/seeds/{table_name}.csv"
        if skip_if_exists and op.exists(file_name):
            click.echo("File " + click.style(file_name, fg="blue") + " already exists; skipping.")
            continue

        callback = ALL_TEST_CASES[table_name]

        echo_table_name(table_name)

        test_case = callback(size, seed)
        test_case.df.to_csv(file_name, index=False)

        click.echo(
            click.style(f"Wrote DataFrame to file {file_name!r}", fg="yellow")
        )
        click.echo("")
    click.echo(click.style("Done!", fg="green"))


@cli.command("gen-hide-macros-yaml")
@click.option("--parse/--no-parse", is_flag=True, default=True)
def gen_hide_args_yaml(parse: bool) -> None:
    """Generates the YAML that hides the macros from the docs.

    Requires the `manifest.json` to be available.
    (`dbt parse --profiles-dir ./tests/dbt_project/profiles`)

    Recommended to `| pbcopy` this command, then paste in `macros/schema.yml`.

    This is not enforced during CICD, beware!
    """

    if parse:
        from dbt.cli.main import dbtRunner
        os.environ["DO_NOT_TRACK"] = "1"
        dbtRunner().invoke(
            [
                "parse",
                "--profiles-dir", op.join(op.dirname(__file__), "integration_tests", "profiles"),
                "--project-dir", op.dirname(__file__)
            ]
        )

    exclude_from_hiding = ["pca", "materialization_pca_default"]
    with open("target/manifest.json") as f:
        manifest = json.load(f)

    macros = [
        data["name"] for fqn, data
        in manifest["macros"].items()
        if data.get("package_name", "") == "dbt_pca"
        and data.get("name") not in exclude_from_hiding
    ]

    out = [
        {"name": macro, "docs": {"show": False}}
        for macro in sorted(macros)
    ]

    print("  " + yaml.safe_dump(out, sort_keys=False).replace("\n", "\n  "))


if __name__ == "__main__":
    cli()
