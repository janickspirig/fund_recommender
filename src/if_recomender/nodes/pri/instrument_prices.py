from typing import Dict
import polars as pl
from .instrument_id_utils import create_instrument_id


def pri_create_instrument_prices(
    spine_funds: pl.DataFrame,
    cvm_blc_1_data: Dict[str, callable],
    cvm_blc_2_data: Dict[str, callable],
    cvm_blc_3_data: Dict[str, callable],
    cvm_blc_4_data: Dict[str, callable],
    cvm_blc_5_data: Dict[str, callable],
    cvm_blc_6_data: Dict[str, callable],
    cvm_blc_7_data: Dict[str, callable],
    cvm_blc_8_data: Dict[str, callable],
) -> pl.DataFrame:
    """
    Creates time series of position values.
    """
    blc_sources = [
        ("blc_1", cvm_blc_1_data),
        ("blc_2", cvm_blc_2_data),
        ("blc_3", cvm_blc_3_data),
        ("blc_4", cvm_blc_4_data),
        ("blc_5", cvm_blc_5_data),
        ("blc_6", cvm_blc_6_data),
        ("blc_7", cvm_blc_7_data),
        ("blc_8", cvm_blc_8_data),
    ]
    all_prices = []

    for blc_type, data_dict in blc_sources:
        for period_key, loader in data_dict.items():
            df = loader()
            cnpj_col = (
                "CNPJ_FUNDO_CLASSE"
                if "CNPJ_FUNDO_CLASSE" in df.columns
                else "CNPJ_FUNDO"
            )
            df = df.with_columns(
                pl.col(cnpj_col)
                .str.replace_all(r"\D", "")
                .cast(pl.UInt64)
                .alias("cnpj")
            ).filter(pl.col("cnpj").is_in(spine_funds["cnpj"]))
            df = create_instrument_id(df, blc_type)
            period = period_key.replace(".csv", "")
            prices = df.select(
                "cnpj",
                "instrument_id",
                pl.lit(period).alias("period"),
                pl.col("VL_MERC_POS_FINAL").cast(pl.Float64).alias("position_value"),
            )
            all_prices.append(prices)

    all_instrument_data = pl.concat(all_prices, how="vertical")

    return all_instrument_data
