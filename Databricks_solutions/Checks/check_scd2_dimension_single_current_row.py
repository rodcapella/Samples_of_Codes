# Garantir que existam somente um registro vigente (current_flag=True) por chave natural na dimensão.
def check_scd2_dimension_single_current_row(df: 'DataFrame', natural_keys: List[str], logger = None) -> bool:
    """
    Última revisão: 24/10/2025
    Testado em: 22/11/2025

    Garantir que existam somente um registro vigente (Flg_Is_Current=True) por chave natural.

    Parâmetros:
    -----------
      df : DataFrame
        DataFrame do PySpark contendo as colunas Start_Date e End_Date.
      natural_keys : List[str]
        Lista de colunas chave natural.
      logger : Logger, optional
        Logger para registrar informações

    Retorno:
    --------
      flg_ok : bool
        True se não há violações, False caso contrário.

    Use esta função se:
    -------------------
      - Deseja garantir que existam somente um registro vigente (Flg_Is_Current=True)
      - Deseja realizar validações de SCD2 em um DataFrame PySpark.

    Exemplo de uso:
    ----------------
      df = spark.createDataFrame([("A", "2023-01-01"), ("A", "2023-02-01"), ("A", "2023-03-01")], ["NaturalKey", "Flg_Is_Current"])
      ok = check_scd2_dimension_single_current_row(df, ["NaturalKey"])
    """
    current_flag_col = "Flg_Is_Current"

    # Validação centralizada do DataFrame
    flg_ok_df = chk_validate_dataframe(df = df, required_columns = natural_keys, logger = logger)
    if not flg_ok_df:
        msg = "O DataFrame não é válido ou não contém as colunas necessárias."
        if logger:
            logger.error(msg)    
        raise ValueError(msg)

    try:
        df_agg = (
            df.groupBy(*natural_keys)
                .agg(F.sum(F.col(current_flag_col).cast("int")).alias("current_count"))
                .filter(F.col("current_count") != F.lit(1))
        )

        flg_ok = df_agg.rdd.isEmpty()
        if flg_ok:
            if logger:
                logger.info("SCD2 current-row: OK")

        else:
            if logger:
                logger.error("SCD2 current-row: chaves com 0 ou >1 registros vigentes encontradas.")

        return flg_ok
    
    except Exception as e:
        msg = f"Erro ao verificar SCD2 current-row: {e}"
        if logger:
            logger.exception(msg)
        raise Exception(msg)

    finally:
        try:
            # Limpa dataframe da memória imediatamente após uso
            flg_ok_clear = utl.util_delete_dataframe_variable_from_scope(
                ["df", "df_agg"],
                "both",
                logger
            )
        except Exception:
            pass  
