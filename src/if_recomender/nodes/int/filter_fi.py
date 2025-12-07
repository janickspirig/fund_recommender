import calendar
from datetime import datetime

import polars as pl
import pandas as pd
from typing import Dict, Tuple

from if_recomender.utils import pl_cnpj_to_numeric


def int_filter_fixed_income_funds(
    num_period_months: int,
    cvm_fi_fund_types: str,
    anbima_fi_fund_types: str,
    anbima_accessability: list,
    min_data_period_per_fund: int,
    remove_funds_w_negative_cvm_pl_values: bool,
    max_period: int,
    anbima_fund_characteristics: pd.DataFrame,
    cvm_monthly_fund_data: Dict[str, pl.DataFrame],
) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Filter fixed income funds present in both CVM and ANBIMA data.

    Applies fund type, accessibility, and negative NAV filters to build
    the universe of funds in scope for analysis.

    Args:
        num_period_months: Number of recent months to include (null = all).
        cvm_fi_fund_types: CVM fund types to include.
        anbima_fi_fund_types: ANBIMA categories to include.
        anbima_accessability: Allowed investor accessibility types.
        min_data_period_per_fund: Minimum periods required per fund.
        remove_funds_w_negative_cvm_pl_values: Whether to exclude negative NAV funds.
        max_period: Most recent period in YYYYMM format.
        anbima_fund_characteristics: ANBIMA fund metadata (pandas DataFrame).
        cvm_monthly_fund_data: CVM monthly data partitioned by period.

    Returns:
        Tuple of (cvm_period_data, anbima_filtered, funds_in_scope) DataFrames.
    """
    # no Polars ExcelDataset in kedro, thus we load with pandas
    anbima_fund_characteristics = pl.from_pandas(anbima_fund_characteristics)

    available_months = sorted(
        [int(key.replace(".csv", "")) for key in cvm_monthly_fund_data.keys()],
        reverse=True,
    )
    if num_period_months:
        selected_months = available_months[:num_period_months]
    else:
        selected_months = available_months

    cvm_dfs = []
    for month in selected_months:
        file_name = f"{month}.csv"
        df = cvm_monthly_fund_data[file_name]()
        if "TP_FUNDO" in df.columns:
            df = df.rename(
                {"TP_FUNDO": "TP_FUNDO_CLASSE", "CNPJ_FUNDO": "CNPJ_FUNDO_CLASSE"}
            )
        cvm_dfs.append(df)

    cvm_all_data = pl.concat(cvm_dfs)

    cvm_fi_data = cvm_all_data.filter(
        pl.col("TP_FUNDO_CLASSE").is_in(cvm_fi_fund_types)
    )

    if remove_funds_w_negative_cvm_pl_values:
        dt = datetime.strptime(str(max_period), "%Y%m")
        last_day = calendar.monthrange(dt.year, dt.month)[1]
        latest_period = dt.replace(day=last_day).strftime("%Y-%m-%d")
        funds_to_remove = cvm_fi_data.filter(
            pl.col("DT_COMPTC") == latest_period, pl.col("VL_PATRIM_LIQ") <= 0
        )["CNPJ_FUNDO_CLASSE"]
        cvm_fi_data = cvm_fi_data.filter(
            ~pl.col("CNPJ_FUNDO_CLASSE").is_in(funds_to_remove)
        )

    cvm_fi_data = cvm_fi_data.with_columns(cnpj=pl_cnpj_to_numeric("CNPJ_FUNDO_CLASSE"))

    cvm_cnpjs = cvm_fi_data.select("cnpj").unique()

    anbima_fi = anbima_fund_characteristics.filter(
        pl.col("Categoria ANBIMA").is_in(anbima_fi_fund_types)
    )

    anbima_fi = anbima_fi.filter(
        pl.col("Tipo de Investidor").is_in(anbima_accessability)
    )

    anbima_fi = anbima_fi.with_columns(
        pl.col("CNPJ do Fundo").cast(pl.UInt64).alias("cnpj")
    )

    anbima_cnpjs = anbima_fi.select("cnpj").unique()

    funds_in_scope = (
        cvm_cnpjs.join(anbima_cnpjs, on="cnpj", how="inner")
        .select(pl.col("cnpj"))
        .sort("cnpj")
    )

    cvm_period_fi_data = (
        cvm_fi_data.join(funds_in_scope, left_on="cnpj", right_on="cnpj", how="inner")
        .select(
            pl.col("cnpj"),
            pl.col("DENOM_SOCIAL"),
            pl.col("DT_COMPTC"),
            pl.col("VL_PATRIM_LIQ"),
            pl.col("TP_FUNDO_CLASSE"),
        )
        .unique(subset=["cnpj", "DT_COMPTC"])
        .sort(["cnpj", "DT_COMPTC"])
    )

    anbima_filtered = anbima_fi.join(
        funds_in_scope, left_on="cnpj", right_on="cnpj", how="inner"
    )

    return cvm_period_fi_data, anbima_filtered, funds_in_scope
