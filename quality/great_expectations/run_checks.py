"""Great Expectations suites for WikiPulse (PRD §8), run daily by the quality_report DAG.

Suites:
  silver_hourly_volume   each completed hour's edit count is within ±50% of the mean of the
                         same hour on up to 7 previous days (WARN: volume shifts with news).
  silver_distributions   last 24 h: unique ids, valid types, sane bot/anonymous shares,
                         byte-delta median, vandalism score range.

Reads fct_edits from the dbt DuckDB warehouse (one row per Silver edit, local and fast)
instead of rescanning Silver's Delta files over S3. Builds Data Docs into
gx/uncommitted/data_docs/. Exits 1 only with --fail-on-error and a failed ERROR-level suite.

    python run_checks.py [--warehouse ../../warehouse/wikipulse.duckdb] [--fail-on-error]
"""

import argparse
import json
import sys
from pathlib import Path

import duckdb
import great_expectations as gx
import great_expectations.expectations as gxe

ROOT = Path(__file__).resolve().parent
DEFAULT_WAREHOUSE = ROOT.parents[1] / "warehouse" / "wikipulse.duckdb"

HOURLY_SQL = """
with hourly as (
    select date_trunc('hour', event_time) as hour_start, count(*) as edits
    from fct_edits
    where event_time >= now() - interval 8 day
    group by 1
),
complete as (  -- leave out the hour still in progress
    select * from hourly where hour_start < date_trunc('hour', now())
)
select
    c.hour_start,
    c.edits,
    avg(p.edits) as same_hour_mean,
    count(p.edits) as baseline_days,
    c.edits / avg(p.edits) as ratio_to_same_hour_mean
from complete c
left join complete p
  on extract(hour from p.hour_start) = extract(hour from c.hour_start)
 and p.hour_start < c.hour_start
 and p.hour_start >= c.hour_start - interval 7 day
where c.hour_start >= date_trunc('hour', now()) - interval 24 hour
group by c.hour_start, c.edits
order by c.hour_start
"""

RECENT_SQL = """
select edit_id, edit_type, cast(is_bot as int) as is_bot, cast(is_anonymous as int) as is_anonymous,
       byte_delta, vandalism_score
from fct_edits
where event_time >= now() - interval 24 hour
"""

SUITES = {
    "silver_hourly_volume": ("WARN", [
        gxe.ExpectColumnValuesToNotBeNull(column="edits"),
        # Only hours with at least one earlier same-hour day have a baseline (ratio not null).
        gxe.ExpectColumnValuesToBeBetween(column="ratio_to_same_hour_mean", min_value=0.5, max_value=1.5),
    ]),
    "silver_distributions": ("ERROR", [
        gxe.ExpectColumnValuesToBeUnique(column="edit_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="edit_id"),
        gxe.ExpectColumnValuesToBeInSet(column="edit_type", value_set=["edit", "new"]),
        gxe.ExpectColumnMeanToBeBetween(column="is_bot", min_value=0.02, max_value=0.8),
        gxe.ExpectColumnMeanToBeBetween(column="is_anonymous", min_value=0.0, max_value=0.5),
        gxe.ExpectColumnMedianToBeBetween(column="byte_delta", min_value=-100, max_value=500),
        gxe.ExpectColumnValuesToBeBetween(column="vandalism_score", min_value=0, max_value=100),
    ]),
}


def validation_definition(context, source, name: str, level: str, expectations: list):
    try:
        asset = source.get_asset(name)
    except LookupError:
        asset = source.add_dataframe_asset(name=name)
    try:
        batch = asset.get_batch_definition("all")
    except KeyError:
        batch = asset.add_batch_definition_whole_dataframe("all")
    suite = context.suites.add_or_update(gx.ExpectationSuite(
        name=name, expectations=expectations, meta={"level": level}))
    return context.validation_definitions.add_or_update(
        gx.ValidationDefinition(name=name, data=batch, suite=suite))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--warehouse", default=str(DEFAULT_WAREHOUSE))
    parser.add_argument("--fail-on-error", action="store_true")
    args = parser.parse_args()

    con = duckdb.connect(args.warehouse, read_only=True)
    con.execute("SET TimeZone = 'UTC'")
    frames = {"silver_hourly_volume": con.execute(HOURLY_SQL).df(),
              "silver_distributions": con.execute(RECENT_SQL).df()}
    con.close()

    context = gx.get_context(mode="file", project_root_dir=str(ROOT))
    source = context.data_sources.add_or_update_pandas(name="warehouse")

    report, failed_errors = {}, []
    for name, (level, expectations) in SUITES.items():
        definition = validation_definition(context, source, name, level, expectations)
        result = definition.run(batch_parameters={"dataframe": frames[name]}, result_format="SUMMARY")
        failures = [r.expectation_config.type for r in result.results if not r.success]
        report[name] = {"level": level, "rows": len(frames[name]), "success": result.success,
                        "evaluated": len(result.results), "failed": failures}
        if level == "ERROR" and not result.success:
            failed_errors.append(name)
    context.build_data_docs()

    print(json.dumps(report, indent=2))
    if args.fail_on_error and failed_errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
