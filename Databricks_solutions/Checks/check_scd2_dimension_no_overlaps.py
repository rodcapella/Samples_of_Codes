# Garantir que não existam intervalos sobrepostos por chave natural em dimensões SCD2.
def check_scd2_dimension_no_overlaps(df: 'DataFrame', natural_keys: List[str], logger = None) -> bool:
    """
    Última revisão: 26/09/2025
    Testado em: 22/11/2025

    Garantir que não existam intervalos sobrepostos por chave natural em SCD2.
    Critério: ordena por Start_Date; se Start atual < End anterior (ou se End anterior é NULL e existe próxima linha), é overlap.

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
        flg_no_overlaps : bool
            True se não há overlaps, False caso contrário. 

    Use esta função se:
    -------------------
        - Deseja garantir que não existam intervalos sobrepostos por chave natural em SCD2.
        - Deseja realizar validações de SCD2 em um DataFrame PySpark.
        - Deseja garantir que os intervalos não se sobreponham por chave natural.

    Exemplo de uso:
    ----------------
        df = spark.createDataFrame([("A", "2023-01-01", "2023-01-31"), ("A", "2023-02-01", "2023-02-28"), ("A", "2023-03-01", None)], ["NaturalKey", "Start_Date", "End_Date"])
        ok, overlaps_df = check_scd2_dimension_no_overlaps(df, ["NaturalKey"])
    """
    # Validação centralizada do DataFrame
    flg_ok_df = chk_validate_dataframe(df = df, required_columns = natural_keys, logger = logger)
    if not flg_ok_df:
        msg = "O DataFrame não é válido ou não contém as colunas necessárias."
        if logger:
            logger.error(msg)    
        raise ValueError(msg)

    start_col = "Start_Date"
    end_col = "End_Date"

    try:
        w = Window.partitionBy(*natural_keys).orderBy(F.col(start_col).asc())
        prev_end = F.lag(F.col(end_col)).over(w)
        prev_start = F.lag(F.col(start_col)).over(w)

        # overlap se:
        # 1) End anterior é NULL (registro aberto) e existe um próximo com start > prev_start  -> viola
        # 2) Start atual < End anterior
        overlaps_df = (
            df
            .withColumn("__prev_end", prev_end)
            .withColumn("__prev_start", prev_start)
            .withColumn(
                "__has_overlap",
                F.when(F.col("__prev_end").isNull() & F.col("__prev_start").isNotNull(), F.lit(True))
                .when(F.col(start_col) < F.col("__prev_end"), F.lit(True))
                .otherwise(F.lit(False))
            )
            .filter(F.col("__has_overlap"))
            .drop("__has_overlap")
        )

        # testa se o dataframe de overlaps está vazio
        flg_no_overlaps = overlaps_df.rdd.isEmpty()
        if flg_no_overlaps:
            if logger:
                logger.info("SCD2 overlap: OK")
        
        else:
            if logger:
                logger.error("SCD2 overlap: foram detectados intervalos sobrepostos.")

        return flg_no_overlaps
    
    except Exception as e:
        msg = f"Erro ao verificar SCD2 overlap: {e}"
        if logger:
            logger.exception(msg)
        raise Exception(msg)

    finally:
        try:
            # Limpa dataframe da memória imediatamente após uso
            flg_ok_clear = utl.util_delete_dataframe_variable_from_scope(
                ["df", "overlaps_df"],
                "both",
                logger
            )
        except Exception:
            pass  
