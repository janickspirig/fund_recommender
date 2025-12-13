from typing import Dict

import polars as pl

from if_recomender.utils import pl_cnpj_to_numeric


def int_normalize_monthly_pl(
    cvm_monthly_fund_data: Dict[str, callable],
    funds_in_scope: pl.DataFrame,
) -> pl.DataFrame:
    """Concatenate and normalize monthly PL data, filter for funds in scope.

    Args:
        cvm_monthly_fund_data: Partitioned dataset of monthly PL files.
        funds_in_scope: DataFrame with 'cnpj' column of funds to include.

    Returns:
        DataFrame with standardized columns, filtered for funds in scope.
    """
    cnpjs_in_scope = funds_in_scope["cnpj"].to_list()

    all_partitions = []
    for period_key, loader in cvm_monthly_fund_data.items():
        df = loader()

        rename_map = {}
        if "CNPJ_FUNDO" in df.columns and "CNPJ_FUNDO_CLASSE" not in df.columns:
            rename_map["CNPJ_FUNDO"] = "CNPJ_FUNDO_CLASSE"
        if "TP_FUNDO" in df.columns and "TP_FUNDO_CLASSE" not in df.columns:
            rename_map["TP_FUNDO"] = "TP_FUNDO_CLASSE"

        if rename_map:
            df = df.rename(rename_map)

        df = df.with_columns(cnpj=pl_cnpj_to_numeric("CNPJ_FUNDO_CLASSE"))
        df = df.filter(pl.col("cnpj").is_in(cnpjs_in_scope))

        all_partitions.append(df)

    return pl.concat(all_partitions, how="diagonal_relaxed")
