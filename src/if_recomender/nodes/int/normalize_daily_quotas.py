import polars as pl

from if_recomender.utils import pl_cnpj_to_numeric


def int_normalize_daily_quotas(
    daily_quotas: dict[str, callable],
    funds_in_scope: pl.DataFrame,
) -> pl.DataFrame:
    """Concatenate and normalize daily quota partitions, filter for funds in scope.

    Handles schema inconsistency between older and newer CVM files:
    - Older files: CNPJ_FUNDO, TP_FUNDO
    - Newer files: CNPJ_FUNDO_CLASSE, TP_FUNDO_CLASSE

    Args:
        daily_quotas: Partitioned dataset of daily quota files (YYYYMM.csv).
        funds_in_scope: DataFrame with 'cnpj' column of funds to include.

    Returns:
        DataFrame with standardized columns, filtered for funds in scope.
    """
    cnpjs_in_scope = funds_in_scope["cnpj"].to_list()

    all_partitions = []
    for partition_name, load_fn in daily_quotas.items():
        partition_df = load_fn()

        # Rename old column names to new standard names
        rename_map = {}
        if (
            "CNPJ_FUNDO" in partition_df.columns
            and "CNPJ_FUNDO_CLASSE" not in partition_df.columns
        ):
            rename_map["CNPJ_FUNDO"] = "CNPJ_FUNDO_CLASSE"
        if (
            "TP_FUNDO" in partition_df.columns
            and "TP_FUNDO_CLASSE" not in partition_df.columns
        ):
            rename_map["TP_FUNDO"] = "TP_FUNDO_CLASSE"

        if rename_map:
            partition_df = partition_df.rename(rename_map)

        # Add numeric CNPJ and filter for funds in scope
        partition_df = partition_df.with_columns(
            cnpj=pl_cnpj_to_numeric("CNPJ_FUNDO_CLASSE")
        )
        partition_df = partition_df.filter(pl.col("cnpj").is_in(cnpjs_in_scope))

        all_partitions.append(partition_df)

    # Concatenate all partitions (relaxed to handle schema differences)
    return pl.concat(all_partitions, how="diagonal_relaxed")
