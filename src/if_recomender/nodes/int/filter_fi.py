import polars as pl
import pandas as pd
from typing import Dict, Tuple


def int_filter_fixed_income_funds(
    num_period_months: int,
    cvm_fi_fund_types: str,
    anbima_fi_fund_types: str,
    anbima_accessability: list,
    min_data_period_per_fund: int,
    anbima_fund_characteristics: pd.DataFrame,
    cvm_monthly_fund_data: Dict[str, pl.DataFrame],
) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Filter fixed income funds present in both CVM and ANBIMA data.

    Args:
        num_period_months: Number of recent months to include
        cvm_fi_fund_types: CVM fund types to filter
        anbima_fi_fund_types: ANBIMA categories to filter
        anbima_accessability: Accessibility types allowed
        min_data_period_per_fund: Minimum periods required
        anbima_fund_characteristics: ANBIMA data (pandas)
        cvm_monthly_fund_data: CVM monthly data dict (polars)

    Returns:
        Tuple of (period_data, characteristics, spine) DataFrames
    """
    # no Polars ExcelDataset in kedro, thus we load with pandas
    anbima_fund_characteristics = pl.from_pandas(anbima_fund_characteristics)

    available_months = sorted(
        [int(key.replace(".csv", "")) for key in cvm_monthly_fund_data.keys()],
        reverse=True,
    )
    selected_months = available_months[:num_period_months]

    cvm_dfs = []
    for month in selected_months:
        file_name = f"{month}.csv"
        df = cvm_monthly_fund_data[file_name]()
        cvm_dfs.append(df)

    cvm_all_data = pl.concat(cvm_dfs)

    cvm_fi_data = cvm_all_data.filter(
        pl.col("TP_FUNDO_CLASSE").is_in(cvm_fi_fund_types)
    )

    cvm_fi_data = cvm_fi_data.with_columns(
        [
            pl.col("CNPJ_FUNDO_CLASSE")
            .str.replace_all(r"\D", "")
            .cast(pl.UInt64)
            .alias("cnpj")
        ]
    )

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
        .sort(["cnpj", "DT_COMPTC"])
    )

    anbima_filtered = anbima_fi.join(
        funds_in_scope, left_on="cnpj", right_on="cnpj", how="inner"
    )

    return cvm_period_fi_data, anbima_filtered, funds_in_scope
