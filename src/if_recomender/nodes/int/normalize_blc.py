"""Normalize BLC data from CVM - concat partitions and standardize schema."""

from typing import Dict

import polars as pl

from if_recomender.utils import pl_cnpj_to_numeric


def int_normalize_blc(
    cvm_blc_1_data: Dict[str, callable],
    cvm_blc_2_data: Dict[str, callable],
    cvm_blc_3_data: Dict[str, callable],
    cvm_blc_4_data: Dict[str, callable],
    cvm_blc_5_data: Dict[str, callable],
    cvm_blc_6_data: Dict[str, callable],
    cvm_blc_7_data: Dict[str, callable],
    funds_in_scope: pl.DataFrame,
) -> pl.DataFrame:
    """Concatenate and normalize all BLC data, filter for funds in scope.

    Args:
        cvm_blc_X_data: Partitioned datasets for each BLC type (1-7).
        funds_in_scope: DataFrame with 'cnpj' column of funds to include.

    Returns:
        DataFrame with all BLC data, standardized columns, blc_type, period, cnpj.
    """
    cnpjs_in_scope = funds_in_scope["cnpj"].to_list()

    blc_sources = [
        ("blc_1", cvm_blc_1_data),
        ("blc_2", cvm_blc_2_data),
        ("blc_3", cvm_blc_3_data),
        ("blc_4", cvm_blc_4_data),
        ("blc_5", cvm_blc_5_data),
        ("blc_6", cvm_blc_6_data),
        ("blc_7", cvm_blc_7_data),
    ]

    all_partitions = []
    for blc_type, data_dict in blc_sources:
        for period_key, loader in data_dict.items():
            df = loader()

            # Normalize column names
            rename_map = {}
            if "CNPJ_FUNDO" in df.columns and "CNPJ_FUNDO_CLASSE" not in df.columns:
                rename_map["CNPJ_FUNDO"] = "CNPJ_FUNDO_CLASSE"
            if "TP_FUNDO" in df.columns and "TP_FUNDO_CLASSE" not in df.columns:
                rename_map["TP_FUNDO"] = "TP_FUNDO_CLASSE"

            if rename_map:
                df = df.rename(rename_map)

            # Add numeric CNPJ and filter for funds in scope
            df = df.with_columns(cnpj=pl_cnpj_to_numeric("CNPJ_FUNDO_CLASSE"))
            df = df.filter(pl.col("cnpj").is_in(cnpjs_in_scope))

            # Add metadata columns
            period = period_key.replace(".csv", "")
            df = df.with_columns(
                [
                    pl.lit(blc_type).alias("blc_type"),
                    pl.lit(period).alias("period"),
                ]
            )

            all_partitions.append(df)

    # Concatenate all partitions (diagonal to handle different schemas per BLC)
    return pl.concat(all_partitions, how="diagonal")
