import pathlib
import polars
import plotext
import datetime

audits_source = pathlib.Path("../../../Documents/deflockchampaign.github.io/foias/champaign_police_department/1/audits")


def main(
        audits_source_dir: pathlib.Path = audits_source,
        consolidated: pathlib.Path = audits_source / "full.parquet",
        fresh: bool = False,
) -> None:

    # Mostly:
    # ""

    # sketchy practice:
    # myoc: Make your own case

    # not a crime:
    # city planning/traffic analysis - deployment testing

    # minor crimes:
    # welfare
    # shoplifting
    # vandalism

    # Other unspecific reasons:
    # "unk"
    # sctf: Street Crimes Task Force
    # "suspicious vehicle"

    polars.Config.set_tbl_rows(200)
    polars.Config.set_tbl_width_chars(200)
    polars.Config.set_fmt_str_lengths(80)

    if fresh:
        df = clean_log_dir(audits_source)

        buckets = {
            ".stolen": {
                "606 e white theft suspect",
                "atm theft",
                "bpd ulta theft",
                "ccso stolen",
                "civil/theft/dus",
                "larceny/theft offenses - suspect vehicle"
                "larceny/theft offenses - suspect vehicle",
                "larceny/theft offenses - suspect",
                "larceny/theft offenses - theft",
                "larceny/theft offenses",
                "motor vehicle theft/stolen - ccso stolen veh",
                "robbery",
                "stolen ccso",
                "stolen property offenses - poss stolen property",
                "stolen property offenses - stolen veh",
                "stolen property offenses - stolen",
                "stolen property offenses",
                "stolen",
                "stolen?",
                "theft suspect",
                "theft",
                "theftt",
                "u of i stolen",
                "urbana stolen",
            },
            ".vandalism": {
                "destruction/damage/vandalism of property",
            },
            ".traffic-inf": {
                "traffic infraction",
                "traffic infraction - 2026-1253",
            },
            ".burglary": {
                "36898 burg",
                "95able for burglary",
                "burg",
                "burglary",
                "burglary/breaking & entering - burlgary",
                "burglary/breaking & entering",
                "vcso burg suspect",
                "vcso burgs",
            },
            ".homicide": {"homicide/death investigation", "homicide"},
            ".assault/battery": {
                "assault/battery offenses (domestic) - domestic",
                "assault/battery offenses (domestic) - interview ro for domestic",
                "assault/battery offenses (domestic) - stabbing",
                "assault/battery offenses (domestic) - threats",
                "assault/battery offenses (domestic)",
                "assault/battery offenses - armed subject",
                "assault/battery offenses - domestic cfs",
                "assault/battery offenses - ro  95able",
                "assault/battery offenses - ro arrestable",
                "assault/battery offenses - stabbijg",
                "assault/battery offenses - suspect",
                "assault/battery offenses - wanted ro",
                "assault/battery offenses - wanted subject",
                "assault/battery offenses",
                "battery",
                "stabbing suspect",
                "threats/harassment",
                "wanted person (arrest warrant/fugitive) - agg batt to p/o",
            },
            ".wanted": {
                "atl",
                "warrant",
                "wanted person (arrest warrant/fugitive) - ccso 99 for ro",
                "wanted person (arrest warrant/fugitive) - persons wanted",
                "wanted person (arrest warrant/fugitive) - wanted",
                "wanted person (arrest warrant/fugitive)",
                "wanted subject",
                "wanted",
            },
            ".shooting": {
                "assault/battery offenses - disorderly subject/battery",
                "assault/battery offenses - shooting",
                "assault/battery offenses - shooting investigation",
                "assault/battery offenses - shots",
                "domestic battery suspect",
                "shooting suspect",
                "shooting",
                "shots fired",
                "rantoul shooting",
                "weapons offense (guns/shots fired) - armed subject",
                "weapons offense (guns/shots fired) - armed subjects",
                "weapons offense (guns/shots fired) - poss susp veh",
                "weapons offense (guns/shots fired) - poss susp veh",
                "weapons offense (guns/shots fired) - shooter car",
                "weapons offense (guns/shots fired) - shooting in county jurisdiction",
                "weapons offense (guns/shots fired) - shooting incident",
                "weapons offense (guns/shots fired) - shooting investigation",
                "weapons offense (guns/shots fired) - shooting suspect",
                "weapons offense (guns/shots fired) - shooting",
                "weapons offense (guns/shots fired) - shots fired",
                "weapons offense (guns/shots fired) - shots",
                "weapons offense (guns/shots fired) - suspect vehicle",
                "weapons offense (guns/shots fired) - suspect veh",
                "weapons offense (guns/shots fired) - urbana shooting",
                "weapons offense (guns/shots fired) - unk vehicle blk suv",
                "weapons offense (guns/shots fired) - wanted subject",
                "weapons offense (guns/shots fired)",
            },
            ".sex crimes": {
                "sex offenses - case followup",
                "sex offenses",
            },
            ".hit/run": {
                "hit and run",
                "hit and run/car accident - 726 faust",
                "hit and run/car accident - accident",
                "hit and run/car accident - atl",
                "hit and run/car accident - hit and run suspect",
                "hit and run/car accident - hit and run",
                "hit and run/car accident - hit run",
                "hit and run/car accident - suspect vehicle",
                "hit and run/car accident - suspect",
                "hit and run/car accident",
                "hit run",
                "hit/run",
                "other image search from alerts page associated with alert: hit and run",
            },
            ".vehicle theft": {
                "hit and run/car accident - locate vehicle",
                "hit and run/car accident - looking for suspect vehicle",
                "hit and run/car accident - warrants",
                "motor theft",
                "motor vehicle theft/stolen - carjacking",
                "motor vehicle theft/stolen - locate stolen vehicle 789",
                "motor vehicle theft/stolen - stolen vehicle",
                "motor vehicle theft/stolen - stolen vehicle cfs",
                "motor vehicle theft/stolen - stolen",
                "motor vehicle theft/stolen - theft",
                "motor vehicle theft/stolen",
                "mv theft",
                "recovered stolen vehicle",
                "stolen property offenses - case review",
                "stolen property offenses - stolen vehicle",
                "stolen veh",
                "stolen vehicle",
                "upd stolen veh",
                "vehicle theft",
            },
            ".fled": {
                "8298 flee",
                "agg fleeing",
                "criminal motor vehicle offense (incl. road rage/reckless) - agg fleeing - ccso",
                "criminal motor vehicle offense (incl. road rage/reckless) - fleeing",
                "fled from upd",
                "fled upd",
                "fled",
                "flee and elude",
                "flee",
                "fleeing vehicle",
                "fleeing",
                "isp flee/elude",
                "isp+flee/elude",
                "obstructing the police (fleeing/eluding) - ccso pursuit",
                "obstructing the police (fleeing/eluding) - flea",
                "obstructing the police (fleeing/eluding) - fled from 728",
                "obstructing the police (fleeing/eluding) - offender vehicle",
                "obstructing the police (fleeing/eluding) - suspec",
                "obstructing the police (fleeing/eluding) - wanted",
                "obstructing the police (fleeing/eluding)",
                "traffic infraction - fleeing",
            },
            ".assist other agency": {"assist other", "assist other agency"},
            ".welfare check": {
                "check welfare",
                "welfare",
                "welfare check",
                "welfare check - check welfare",
                "welfare check - missin person",
                "welfare check - welfare",
            },
            ".shoplifting": {
                "burglary/breaking & entering - retail theft",
                "789 retail theft",
                "larceny/theft offenses - retail theft",
                "larceny/theft offenses - shoplifter",
                "larceny/theft offenses - shoplifter not in custody",
                "larceny/theft offenses - shoplifting suspect",
                "lowes retail theft",
                "retail theft / impound",
                "retail theft at ulta",
                "retail theft investigation",
                "retail theft offender",
                "retail theft suspect",
                "retail theft",
                "retain theft",
                "shoplifeter",
                "shoplifter at target",
                "shoplifter not in custody",
                "shoplifter",
                "shoplifter/warrant",
                "shoplifting",
                "shopliter",

            },
            ".drugs": {
                "drugs/narcotics",
                "drugs/narcotics - case followup",
                "drugs/narcotics - drugs",
            },
        }
        case_no = r"(c|s|so|i|u)[0-9-]+"
        df = df.with_columns(
            polars.col("reason")
            # Strip case nos. They will be saved in "case" column
            .str.replace(case_no, "")
            # Strip suspect's name
            .str.replace("john.*", "")
            # Strip trailing -
            .str.replace(" - $", "")
            # Strip trailing whitespace
            .str.strip_chars()
            # Bucket-ize reasons
            .replace({
                reason: group
                for group, reasons in buckets.items()
                for reason in [*reasons, group]
            })
            .alias("reason"),

            # When case is missing, find it in reason
            polars.when(polars.coalesce(polars.col("case"), polars.lit("")).len().gt(1))
            .then(polars.col("case"))
            .otherwise(
                polars.when(polars.col("reason").str.contains(case_no))
                .then(polars.col("reason").str.replace("(" + case_no + ")", r"\1"))
                .otherwise(polars.lit(None))
            )
            .alias("case"),
        )

        df.write_parquet(consolidated)
    else:
        df = polars.read_parquet(consolidated)

    print(f"{consolidated.name}: {len(df)} rows, {consolidated.stat().st_size} bytes")
    print(df.schema)

    print_most_common(df)
    # sus_plates(df)
    # interactive_console(df)
    # altair_plot(df)


def clean_log_dir(dir: pathlib.Path) -> polars.DataFrame:
    df = None
    column_list = None
    for file in sorted([*dir.glob("*.xlsx"), *dir.glob("*.csv")]):
        file_df = clean_log_file(file)
        print(f"{file.name}: {len(file_df)} rows, {file.stat().st_size} bytes")
        if df is None:
            df = file_df
            column_list = file_df.columns
        else:
            df = polars.concat([df, file_df.select(column_list)])
    assert df is not None
    return df


def clean_log_file(path: pathlib.Path) -> polars.DataFrame:
    if path.suffix == ".csv":
        df = polars.read_csv(path)
    elif path.suffix == ".xlsx":
        df = polars.read_excel(path)
    else:
        raise RuntimeError(f"Not implemented for {path}")
    df = df.rename({
        column: column.lower().strip().replace(" ", "_").replace("_#", "")
        for column in df.columns
    })
    necessary_columns = ["org_name", "search_type"]
    for column in necessary_columns:
        if column not in df.columns:
            df = df.with_columns(polars.lit("").alias(column))
    time_fmt = "%m/%d/%Y, %I:%M:%S %p UTC"
    delim = "\x0d"
    df = df.with_columns(
        polars.col("time_frame")
        .str
        .split_exact(delim, 1)
    )
    return df.with_columns(
        polars.col("time_frame")
        .struct
        .field("field_0")
        .str
        .strptime(
            polars.Datetime,
            format=time_fmt,
            strict=False,
            exact=True,
        ).alias("time_frame_start"),
        polars.col("time_frame")
        .struct
        .field("field_1")
        .str
        .strptime(
            polars.Datetime,
            format=time_fmt,
            strict=False,
            exact=True,
        ).alias("time_frame_end"),
        polars.col("search_time")
        .str
        .strptime(
            polars.Datetime,
            format="%m/%d/%Y, %I:%M:%S %p UTC",
            strict=True,
            exact=True,
        ),
        polars.col("name").cast(polars.Categorical),
        polars.col("org_name").cast(polars.Categorical),
        polars.col("reason").str.to_lowercase().str.strip_chars().str.replace(" -$", ""),
        polars.col("case").str.to_lowercase().str.strip_chars(),
        polars.col("filters").str.to_lowercase().str.strip_chars(),
        polars.lit(path.name).alias("source"),
    ).drop("time_frame").filter(
        polars.col("total_networks_searched").is_not_null()
    )


def altair_plot(
        df: polars.DataFrame,
        period: str = "30d",
) -> None:
    import altair
    reasons = (
        df["reason"]
        .value_counts()
        .sort(polars.col("count"))
        .tail(4)
    )["reason"].to_list()
    time_df = (
        df.select(
            *[
                polars.col("reason").eq(reason).alias(reason)
                for reason in reasons
            ],
            polars.col("search_time").dt.round(every=period),
            polars.col("case").is_not_null().alias("has_case"),
        )
        .group_by("search_time")
        .agg(
            *[
                polars.col(reason).sum()
                for reason in reasons
            ],
            polars.col("has_case").sum(),
            (polars.len().alias("all") - polars.col("has_case").sum()).alias("has_no_case"),
            polars.len().alias("all"),
        )
        .sort("search_time")
    )
        # .unpivot(index="search_time", variable_name="reason", value_name="count")
    (
        altair.Chart(time_df)
        # .mark_line()
        # .encode(
        #     x="search_time:T",
        #     y="count:Q",
        #     color="reason:N",
        # )
        .mark_line()
        .encode(
            x="search_time:T",
            y="has_case:Q",
        )
        .mark_line()
        .encode(
            x="search_time:T",
            y="all:Q",
        )
        .mark_line()
        .encode(
            x="search_time:T",
            y="has_no_case:Q",
        )
        .interactive()
        .save("chart.html")
    )


def print_most_common(df) -> None:
    print("Reasons < 2026")
    print(
        df.filter(polars.col("search_time") < polars.datetime(2026, 1, 1))
        ["reason"]
        .value_counts()
        .with_columns((polars.col("count") / len(df) * 100).round().cast(int).alias("%"))
        .sort(by="count")
        .tail(50)
    )
    print("Reasons >= 2026")
    print(
        df.filter(polars.col("search_time") >= polars.datetime(2026, 1, 1))
        ["reason"]
        .value_counts()
        .with_columns((polars.col("count") / len(df) * 100).round().cast(int).alias("%"))
        .sort(by="count")
        .tail(50)
    )
    print("Officer")
    print(
        df["name"]
        .value_counts()
        .with_columns((polars.col("count") / len(df) * 100).round().cast(int).alias("%"))
        .sort(by="count")
        .tail(10)
    )
    print("Org name")
    print(
        df["org_name"]
        .value_counts()
        .with_columns((polars.col("count") / len(df) * 100).round().cast(int).alias("%"))
        .sort(by="count")
        .tail(10)
    )
    # ignore_filters = ["all images", "illinois", "indiana", "red", "white"]
    print("Filters")
    print(
        df
        .select(
            polars.col("filters")
            .str.replace("all images", "")
            .str.replace(",", " ")
            .str.replace(" +", " ")
            .str.strip_chars()
        )
        ["filters"]
        .value_counts()
        .with_columns((polars.col("count") / len(df) * 100).round().cast(int).alias("%"))
        .sort(by="count")
        .tail(50)
    )
    print("Searches by month")
    print(
        df.select(
            polars.date(polars.col("search_time").dt.year(), polars.col("search_time").dt.month(), 1).alias("search_time"),
            polars.col("case").is_not_null().alias("has_case"),
            polars.col("reason").str.len_chars().gt(3).alias("has_reason"),
        )
        .group_by("search_time")
        .agg(
            polars.len().alias("total"),
            polars.col("has_case").sum(),
            polars.col("has_reason").sum(),
        )
        .sort(by="search_time")
    )

def sus_plates(df) -> None:
    plates = (
        df
        ["license_plate"]
        .value_counts()
        .with_columns((polars.col("count") / len(df) * 100).round().cast(int).alias("%"))
        .sort(by="count")
        .reverse()
        .head(100)
        ["license_plate"]
        .to_list()
    )
    for plate in plates:
        if plate is not None:
            filtered = df.filter(polars.col("license_plate").eq(plate))
            duration = filtered["search_time"].max() - filtered["search_time"].min()
            month_duration = int(round(duration / datetime.timedelta(days=30)))
            reasons = set(filtered["reason"].unique().to_list())
            if month_duration > 3 or len(reasons) > 4:
                print(plate, month_duration, reasons)
        


def interactive_console(df) -> None:
    first_time = True
    while True:
        reason = input("> ")
        if reason == "q":
            break
        period = "30d"
        reason_df = df.filter(polars.col("reason").cast(polars.String).str.contains(reason))
        if reason_df.is_empty():
            print("No searches found")
        else:
            print(f"Searches for {reason}")
            print(
                reason_df["reason"]
                .value_counts()
                .with_columns((polars.col("count") / len(df) * 100).round().cast(int).alias("%"))
                .sort(by="count")
                .tail(10)
            )
            agged_reason_df = (
                reason_df
                .with_columns(polars.col("search_time").dt.round(every=period))
                .group_by("search_time")
                .agg(polars.len().alias("count"))
                .sort("search_time")
            )
            dates = plotext.datetimes_to_string(agged_reason_df["search_time"].to_list())
            values = agged_reason_df["count"].to_list()
            if first_time:
                first_time = False
            else:
                plotext.clf()
            plotext.plot_size(80, plotext.terminal_height() - 5)
            plotext.plot(dates, values, label=reason)
            plotext.ylim(0, None)
            xmin = agged_reason_df["search_time"].to_list()[0] - datetime.timedelta(days=15)
            xmax = agged_reason_df["search_time"].to_list()[0] + datetime.timedelta(days=15)
            plotext.xlim(
                plotext.datetimes_to_string([xmin])[0],
                plotext.datetimes_to_string([xmax])[0],
            )
            plotext.show()


if __name__ == "__main__":
    main()
