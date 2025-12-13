"""Determine funds in scope from CVM and ANBIMA intersection."""

import calendar
from datetime import datetime
from typing import Dict

import pandas as pd
import polars as pl

from if_recomender.utils import pl_cnpj_to_numeric


def int_determine_funds_in_scope(
    cvm_fi_fund_types: list,
    anbima_fi_fund_types: list,
    anbima_accessability: list,
    remove_funds_w_negative_cvm_pl_values: bool,
    max_period: int,
    anbima_fund_characteristics: pd.DataFrame,
    cvm_monthly_fund_data: Dict[str, callable],
) -> pl.DataFrame:
    """Determine funds in scope from CVM ∩ ANBIMA intersection.

    Applies FI type filter, accessibility filter, and negative NAV exclusion.

    Args:
        cvm_fi_fund_types: CVM fund types to include (e.g., ["FI"]).
        anbima_fi_fund_types: ANBIMA categories to include.
        anbima_accessability: Allowed investor accessibility types.
        remove_funds_w_negative_cvm_pl_values: Exclude funds with negative NAV.
        max_period: Most recent period in YYYYMM format.
        anbima_fund_characteristics: ANBIMA fund metadata (pandas DataFrame).
        cvm_monthly_fund_data: CVM monthly PL data partitioned by period.

    Returns:
        DataFrame with single 'cnpj' column (UInt64) of funds in scope.
    """
    anbima_df = pl.from_pandas(anbima_fund_characteristics)

    cvm_dfs = []
    for period_key, loader in cvm_monthly_fund_data.items():
        df = loader()
        if "TP_FUNDO" in df.columns and "TP_FUNDO_CLASSE" not in df.columns:
            df = df.rename(
                {"TP_FUNDO": "TP_FUNDO_CLASSE", "CNPJ_FUNDO": "CNPJ_FUNDO_CLASSE"}
            )
        cvm_dfs.append(df)

    cvm_all = pl.concat(cvm_dfs, how="diagonal_relaxed")
    cvm_fi = cvm_all.filter(pl.col("TP_FUNDO_CLASSE").is_in(cvm_fi_fund_types))

    if remove_funds_w_negative_cvm_pl_values:
        dt = datetime.strptime(str(max_period), "%Y%m")
        last_day = calendar.monthrange(dt.year, dt.month)[1]
        latest_date = dt.replace(day=last_day).strftime("%Y-%m-%d")
        funds_to_remove = cvm_fi.filter(
            (pl.col("DT_COMPTC") == latest_date) & (pl.col("VL_PATRIM_LIQ") <= 0)
        )["CNPJ_FUNDO_CLASSE"]
        cvm_fi = cvm_fi.filter(~pl.col("CNPJ_FUNDO_CLASSE").is_in(funds_to_remove))

    cvm_fi = cvm_fi.with_columns(cnpj=pl_cnpj_to_numeric("CNPJ_FUNDO_CLASSE"))
    cvm_cnpjs = cvm_fi.select("cnpj").unique()

    anbima_fi = anbima_df.filter(
        pl.col("Categoria ANBIMA").is_in(anbima_fi_fund_types)
    ).filter(pl.col("Tipo de Investidor").is_in(anbima_accessability))

    anbima_fi = anbima_fi.with_columns(
        pl.col("CNPJ do Fundo").cast(pl.UInt64).alias("cnpj")
    )
    anbima_cnpjs = anbima_fi.select("cnpj").unique()

    funds_in_scope = (
        cvm_cnpjs.join(anbima_cnpjs, on="cnpj", how="inner").select("cnpj").sort("cnpj")
    )

    return funds_in_scope
