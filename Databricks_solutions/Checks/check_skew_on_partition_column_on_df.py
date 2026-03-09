# Verificar skew (desequilíbrio) em uma coluna de particionamento em um DataFrame
def check_skew_on_partition_column_on_df(df: 'DataFrame', partition_col: str | list, skew_threshold: float = 3.0, logger = None) -> dict:
    """
    Última revisão: 28/08/2025
    Testado em: 

    Verificar skew (desequilíbrio) em uma coluna de particionamento em um DataFrame Spark.

    O skew acontece quando alguns valores de uma coluna de particionamento 
    concentram registros demais em relação aos outros, causando partições 
    desbalanceadas e problemas de performance (shuffle muito pesado, tasks lentas).

    Parâmetros
    ----------
        df : pyspark.sql.DataFrame
            DataFrame Spark a ser analisado.
        partition_col : str ou list[str]
            Nome da coluna (ou lista de colunas) usada para particionamento (ex: "customer_id" ou ["ano","mes"]).
        skew_threshold : float, padrão 3.0
            Valor do "skew ratio" acima do qual será considerado skew.
            - skew_ratio = (maior número de registros em uma partição) / (média de registros por partição).
        logger : logging.Logger, opcional
            Logger para registrar mensagens.

    Retorno:
    -------
        result : dict
                    {
                        "partition_col": list[str],
                        "max_count": int,
                        "avg_count": float,
                        "skew_ratio": float,
                        "is_skewed": bool,
                        "error": str ou None
                    }

    Use esta função se:
    ------------------
        - Quer verificar se uma coluna de particionamento está desbalanceada.
        - Deseja evitar problemas de performance causados por partições desbalanceadas.
        - Quer identificar colunas de particionamento que podem ser otimizadas

    Exemplos de uso:
    ----------------
        result = check_skew_on_partition_column(df, "customer_id", skew_threshold=5.0)
        print(result)
        {
            "partition_col": ["ano", "mes"],
            "max_count": 120000,
            "avg_count": 23000.5,
            "skew_ratio": 5.21,
            "is_skewed": True,
            "error": None
        }
    """
    # Validação centralizada do DataFrame
    flg_ok_df = chk.chk_validate_dataframe(df = df, required_columns = partition_cols, logger = logger)
    if not flg_ok_df:
        msg = "O DataFrame não é válido ou não contém as colunas necessárias."
        if logger:
            logger.error(msg)    
        raise ValueError(msg)

    # Valida se skew_threshold é um float
    if not chk.check_if_is_valid_float(skew_threshold):
        msg = f"Parâmetro skew_threshold deve ser um float."
        if logger:
            logger.error(msg)
        raise ValueError(msg)

    result = result or {}
    try:
        partition_col_lst = utl.util_validate_and_get_columns_list(partition_col)
    
        stats = (df.groupBy(partition_col_lst)
                .count()
                .agg(F.max("count").alias("max"), F.avg("count").alias("avg"))
                .collect()[0])
        
        skew_ratio = stats["max"] / stats["avg"] if stats["avg"] > 0 else float("inf")
        is_skewed = skew_ratio > skew_threshold
        
        # Resultado estruturado
        result.update({
            "partition_col": [partition_col] if isinstance(partition_col, str) else partition_col,
            "max_count": stats["max"],
            "avg_count": stats["avg"], 
            "skew_ratio": skew_ratio,
            "is_skewed": is_skewed
        })
        
        if logger:
            status = "SKEW" if is_skewed else "OK"
            logger.info(f"{status} {partition_col_lst} | skew={skew_ratio:.2f} > {skew_threshold}")      
        return result
    
    except Exception as e:
        if logger:
            logger.exception(f"Erro ao verificar desbalanceamento de coluna de particionamento.")
        raise e

    finally:
        try:
            # Limpa dataframe da memória imediatamente após uso
            flg_ok_clear = utl.util_delete_dataframe_variable_from_scope(
                ["df"],
                "both",
                logger
            )
        except Exception:
            pass    
