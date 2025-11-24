from typing import Dict
import polars as pl
from .instrument_id_utils import create_instrument_id


def pri_create_instrument_rating(
    spine_funds: pl.DataFrame,
    cvm_blc_5_data: Dict[str, callable],
) -> pl.DataFrame:
    """
    Extract credit ratings from BLC_5 over time.

    Args:
        spine_funds: DataFrame with cnpj column (funds in scope)
        cvm_blc_5_data: PartitionedDataset for BLC_5 (private credit)

    Returns:
        DataFrame with columns:
        - cnpj: Fund identifier
        - instrument_id: Unique instrument identifier
        - period: YYYYMM
        - issuer_cnpj: Issuer CNPJ (normalized)
        - credit_rating: Rating grade
        - rating_agency: Rating agency name
    """

    all_ratings = []

    for period_key, loader in cvm_blc_5_data.items():
        df = loader()

        cnpj_col = (
            "CNPJ_FUNDO_CLASSE" if "CNPJ_FUNDO_CLASSE" in df.columns else "CNPJ_FUNDO"
        )

        df = df.with_columns(
            [pl.col(cnpj_col).str.replace_all(r"\D", "").cast(pl.UInt64).alias("cnpj")]
        )

        df = df.filter(pl.col("cnpj").is_in(spine_funds["cnpj"]))

        df = create_instrument_id(df, "blc_5")

        period = period_key.replace(".csv", "")

        ratings = df.select(
            [
                pl.col("cnpj"),
                pl.col("instrument_id"),
                pl.lit(period).alias("period"),
                pl.col("CNPJ_EMISSOR").str.replace_all(r"\D", "").alias("issuer_cnpj"),
                pl.col("GRAU_RISCO").alias("credit_rating"),
                pl.col("AG_RISCO").alias("rating_agency"),
            ]
        )

        ratings = ratings.filter(pl.col("credit_rating").is_not_null())

        all_ratings.append(ratings)

    instrument_rating = pl.concat(all_ratings, how="vertical")

    return instrument_rating
