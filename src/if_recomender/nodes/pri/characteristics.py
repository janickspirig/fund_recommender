import polars as pl
import unicodedata


def make_safe_column_name(value: str) -> str:
    """Convert category value to safe is_* column name."""
    value = unicodedata.normalize("NFD", value)
    value = "".join(c for c in value if unicodedata.category(c) != "Mn")

    value = value.lower().replace(" ", "_").replace("-", "_")

    value = "".join(c if c.isalnum() or c == "_" else "" for c in value)

    return f"is_{value}"


def pri_create_fund_characteristics(
    period_fi_fund_data: pl.DataFrame,
    spine_funds: pl.DataFrame,
    fund_characteristics: pl.DataFrame,
) -> pl.DataFrame:
    """Build fund characteristics with one-hot encoded ANBIMA category flags.

    Deduplicates share classes by keeping earliest inception date per CNPJ.
    Creates is_* columns for each unique category value across all levels.

    Args:
        period_fi_fund_data: CVM period data (unused, for pipeline compatibility).
        spine_funds: Funds in scope with cnpj column.
        fund_characteristics: ANBIMA fund metadata with categories and attributes.

    Returns:
        DataFrame with cnpj, category columns, fund_manager, redemption_days,
        inception_date, is_active, and ~30-40 is_* one-hot columns.
    """

    fund_characteristics_dedup = (
        fund_characteristics.select(
            [
                "cnpj",
                pl.col("Categoria ANBIMA").alias("anbima_category_main"),
                pl.col("nivel_1_categoria").alias("anbima_category_level_1"),
                pl.col("nivel_2_categoria").alias("anbima_category_level_2"),
                pl.col("nivel_3_subcategoria").alias("anbima_category_level_3"),
                pl.col("Característica do Investidor").alias("target_investor_type"),
                pl.col("Tipo ANBIMA").alias("fund_subtype"),
                pl.col("Tipo de Investidor").alias("accessability"),
                pl.col("Gestor Principal").alias("fund_manager"),
                pl.col("Nome Comercial").alias("commercial_name"),
                pl.col("Prazo Pagamento Resgate em dias").alias("redemption_days"),
                pl.col("Status").alias("status"),
                pl.col("Data de Início de Atividade").alias("inception_date"),
            ]
        )
        .sort("inception_date")
        .group_by("cnpj")
        .first()
    )

    master_df = spine_funds.join(
        fund_characteristics_dedup,
        on="cnpj",
        how="left",
    )

    nivel_1_unique = (
        master_df["anbima_category_level_1"].unique().drop_nulls().to_list()
    )
    nivel_2_unique = (
        master_df["anbima_category_level_2"].unique().drop_nulls().to_list()
    )
    nivel_3_unique = (
        master_df["anbima_category_level_3"].unique().drop_nulls().to_list()
    )

    for cat_value in nivel_1_unique:
        col_name = make_safe_column_name(cat_value)
        master_df = master_df.with_columns(
            [
                (pl.col("anbima_category_level_1") == cat_value)
                .cast(pl.Int8)
                .alias(col_name)
            ]
        )

    for cat_value in nivel_2_unique:
        col_name = make_safe_column_name(cat_value)
        master_df = master_df.with_columns(
            [
                (pl.col("anbima_category_level_2") == cat_value)
                .cast(pl.Int8)
                .alias(col_name)
            ]
        )

    for cat_value in nivel_3_unique:
        col_name = make_safe_column_name(cat_value)
        master_df = master_df.with_columns(
            [
                (pl.col("anbima_category_level_3") == cat_value)
                .cast(pl.Int8)
                .alias(col_name)
            ]
        )

    master_df = master_df.with_columns(
        [(pl.col("status") == "Ativo").cast(pl.Int8).alias("is_active")]
    )

    master_df = master_df.with_columns(
        [
            pl.when(pl.col("redemption_days").is_null())
            .then(90.0)
            .otherwise(pl.col("redemption_days"))
            .alias("redemption_days")
        ]
    )

    master_df = master_df.drop(["status"])

    n_rows = len(master_df)
    n_unique_cnpjs = master_df["cnpj"].n_unique()
    if n_rows != n_unique_cnpjs:
        raise ValueError(
            f"Duplicate CNPJs detected in characteristics table! "
            f"Total rows: {n_rows}, Unique CNPJs: {n_unique_cnpjs}"
        )

    return master_df
