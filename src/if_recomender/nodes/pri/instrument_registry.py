import polars as pl


def pri_create_instrument_registry(
    int_blc: pl.DataFrame,
) -> pl.DataFrame:
    """Assign unique instrument_id per position in BLC data.

    The BLC data is already normalized and filtered in the intermediate layer.

    Args:
        int_blc: Normalized BLC data with cnpj, blc_type, period columns.

    Returns:
        DataFrame with all BLC columns plus instrument_id.
    """
    registry = (
        int_blc.with_row_index("row_idx")
        .with_columns(
            (pl.col("blc_type") + "_" + pl.col("row_idx").cast(pl.Utf8)).alias(
                "instrument_id"
            )
        )
        .drop("row_idx")
    )

    return registry
