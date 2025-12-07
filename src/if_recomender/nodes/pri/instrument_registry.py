"""Instrument registry with unique IDs for all BLC positions."""

from typing import Dict
import polars as pl

from if_recomender.utils import pl_cnpj_to_numeric


def pri_create_instrument_registry(
    spine_funds: pl.DataFrame,
    cvm_blc_1_data: Dict[str, callable],
    cvm_blc_2_data: Dict[str, callable],
    cvm_blc_3_data: Dict[str, callable],
    cvm_blc_4_data: Dict[str, callable],
    cvm_blc_5_data: Dict[str, callable],
    cvm_blc_6_data: Dict[str, callable],
    cvm_blc_7_data: Dict[str, callable],
    # cvm_blc_8_data: Dict[str, callable],
) -> pl.DataFrame:
    """Concatenate all BLC files and assign unique instrument_id per position.

    Preserves all original CVM columns and adds instrument_id, blc_type, period, cnpj.

    Args:
        spine_funds: Funds in scope with cnpj column.
        cvm_blc_X_data: PartitionedDatasets for each BLC type (1-7).

    Returns:
        DataFrame with all BLC columns plus instrument_id, blc_type, period, cnpj.
    """
    blc_sources = [
        ("blc_1", cvm_blc_1_data),
        ("blc_2", cvm_blc_2_data),
        ("blc_3", cvm_blc_3_data),
        ("blc_4", cvm_blc_4_data),
        ("blc_5", cvm_blc_5_data),
        ("blc_6", cvm_blc_6_data),
        ("blc_7", cvm_blc_7_data),
        # ("blc_8", cvm_blc_8_data),
    ]

    all_dfs = []
    instrument_counter = 0

    for blc_type, data_dict in blc_sources:
        for period_key, loader in data_dict.items():
            df = loader()

            cnpj_col = (
                "CNPJ_FUNDO_CLASSE"
                if "CNPJ_FUNDO_CLASSE" in df.columns
                else "CNPJ_FUNDO"
            )

            df = df.with_columns(pl_cnpj_to_numeric(cnpj_col).alias("cnpj")).filter(
                pl.col("cnpj").is_in(spine_funds["cnpj"])
            )

            period = period_key.replace(".csv", "")

            instrument_ids = [
                f"{blc_type}_{instrument_counter + i}" for i in range(df.height)
            ]

            instrument_counter += df.height

            df = df.with_columns(
                [
                    pl.lit(blc_type).alias("blc_type"),
                    pl.lit(period).alias("period"),
                    pl.Series("instrument_id", instrument_ids),
                ]
            )

            all_dfs.append(df)

    registry = pl.concat(all_dfs, how="diagonal")

    return registry
