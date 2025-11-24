import polars as pl


def create_instrument_id(df: pl.DataFrame, blc_type: str) -> pl.DataFrame:
    """
    Create unique instrument_id based on BLC type.

    Logic per BLC type:
    - blc_1: CD_ISIN or CD_SELIC (government bonds)
    - blc_2: CNPJ_FUNDO_CLASSE_COTA or CNPJ_FUNDO_COTA (fund quotas)
    - blc_3: CD_SWAP (swap contracts)
    - blc_4: CD_ISIN (fixed income securities)
    - blc_5: CNPJ_EMISSOR + DT_VENC (private credit by issuer/maturity)
    - blc_6: CNPJ_EMISSOR or CPF_CNPJ_EMISSOR + DT_VENC (bank deposits)
    - blc_7: CD_ISIN or CD_ATIVO (foreign assets)
    - blc_8: CD_ATIVO or DS_ATIVO (other assets)

    Args:
        df: DataFrame with BLC-specific columns
        blc_type: Type of BLC file (blc_1 to blc_8)

    Returns:
        DataFrame with added 'instrument_id' and 'instrument_name' columns
    """

    if blc_type == "blc_1":
        # Government bonds: Use ISIN or SELIC code
        instrument_col = pl.coalesce(["CD_ISIN", "CD_SELIC"])
        name_col = pl.col("TP_TITPUB")  # Bond type (LFT, NTN-B, etc.)

    elif blc_type == "blc_2":
        # Fund quotas: Use underlying fund CNPJ (handle both old and new column names)
        if "CNPJ_FUNDO_CLASSE_COTA" in df.columns:
            instrument_col = pl.col("CNPJ_FUNDO_CLASSE_COTA").str.replace_all(r"\D", "")
            name_col = pl.col("NM_FUNDO_CLASSE_SUBCLASSE_COTA")
        else:
            instrument_col = pl.col("CNPJ_FUNDO_COTA").str.replace_all(r"\D", "")
            name_col = pl.col("NM_FUNDO_COTA")

    elif blc_type == "blc_3":
        # Swaps: Use swap code
        instrument_col = pl.col("CD_SWAP")
        name_col = pl.col("DS_SWAP")

    elif blc_type == "blc_4":
        # Fixed income: Use ISIN or asset code
        if "CD_ISIN" in df.columns:
            instrument_col = pl.col("CD_ISIN")
        else:
            instrument_col = pl.col("CD_ATIVO")
        name_col = pl.col("DS_ATIVO")

    elif blc_type == "blc_5":
        # Private credit: Issuer + maturity date
        instrument_col = pl.concat_str(
            [pl.col("CNPJ_EMISSOR").str.replace_all(r"\D", ""), pl.col("DT_VENC")],
            separator="_",
        )
        name_col = pl.concat_str(
            [pl.col("EMISSOR"), pl.col("TP_ATIVO")], separator=" - "
        )

    elif blc_type == "blc_6":
        # Bank deposits: Issuer + maturity (handle different issuer column names)
        if "CNPJ_EMISSOR" in df.columns:
            issuer_col = pl.col("CNPJ_EMISSOR").str.replace_all(r"\D", "")
        else:
            issuer_col = pl.col("CPF_CNPJ_EMISSOR").str.replace_all(r"\D", "")

        instrument_col = pl.concat_str([issuer_col, pl.col("DT_VENC")], separator="_")
        name_col = pl.col("EMISSOR")

    elif blc_type == "blc_7":
        # Foreign assets: ISIN, CD_ATIVO, or CD_ATIVO_BV_MERC (ticker)
        if "CD_ISIN" in df.columns:
            instrument_col = pl.coalesce(["CD_ISIN", "CD_ATIVO"])
        elif "CD_ATIVO" in df.columns:
            instrument_col = pl.col("CD_ATIVO")
        elif "CD_ATIVO_BV_MERC" in df.columns:
            instrument_col = pl.col("CD_ATIVO_BV_MERC")
        else:
            # Fallback: use EMISSOR + TP_ATIVO if available
            if "EMISSOR" in df.columns and "TP_ATIVO" in df.columns:
                instrument_col = pl.concat_str(
                    [pl.col("EMISSOR"), pl.col("TP_ATIVO")], separator="_"
                )
            else:
                # Last resort: use row number
                instrument_col = pl.int_range(pl.len()).cast(pl.Utf8)

        # Name column: DS_ATIVO_EXTERIOR or DS_ATIVO
        if "DS_ATIVO_EXTERIOR" in df.columns:
            name_col = pl.col("DS_ATIVO_EXTERIOR")
        elif "DS_ATIVO" in df.columns:
            name_col = pl.col("DS_ATIVO")
        else:
            name_col = pl.col("EMISSOR")

    else:  # blc_8
        # Other: Use CD_ATIVO or DS_ATIVO
        if "CD_ATIVO" in df.columns:
            instrument_col = pl.col("CD_ATIVO")
        else:
            instrument_col = pl.col("DS_ATIVO")
        name_col = pl.col("DS_ATIVO")

    # Create composite instrument_id: blc_type + specific_id
    df = df.with_columns(
        [
            pl.concat_str([pl.lit(blc_type), instrument_col], separator="_").alias(
                "instrument_id"
            ),
            name_col.alias("instrument_name"),
        ]
    )

    return df
