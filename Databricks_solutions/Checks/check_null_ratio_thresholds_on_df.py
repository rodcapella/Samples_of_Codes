# Comparar % de nulos por coluna com thresholds fornecidos (0.0 a 1.0) no dataframe
def check_null_ratio_thresholds_on_df(df: 'DataFrame', thresholds: dict, logger = None) -> Tuple[bool, 'DataFrame']:
    """
    Última revisão: 26/09/2025


    Compara % de nulos por coluna com thresholds fornecidos (0.0 a 1.0).
    `thresholds` exemplo: {'Email': 0.02, 'Zip_Code': 0.0}

    Parâmetros:
    -----------
        df : DataFrame 
            DataFrame do PySpark.
        thresholds : dict
            Dicionário com colunas e seus thresholds.
        logger : Logger, opcional
            Logger para registrar informações.

    Retorno:
    ........
        flg_has_fails : bool
            True se todas as colunas <= seus limites, False caso contrário.
        metrics_df : DataFrame
            DataFrame com coluna, null_count, total_count, null_ratio, threshold, status
    
    Use esta função se:
    -------------------
        - Deseja garantir que nenhuma coluna exceda seu threshold de nulos.
        - Deseja realizar validações de qualidade em um DataFrame PySpark.
    
    Exemplo de uso:
    ----------------
        df = spark.createDataFrame([("A", "2023-01-01"), ("A", "2023-02-01"), ("A", "2023-03-01")], ["NaturalKey", "Flg_Is_Current"])
        ok, metrics_df = check_null_ratio_thresholds_on_df(df, {'NaturalKey': 0.0, 'Flg_Is_Current': 0.0})
    """
    # Valida se o dataframe é válido e não vazio
    if not check_if_is_valid_dataframe(df): 
        msg = "Dataframe inválido ou vazio"
        if logger:
            logger.error(msg)
        raise ValueError(msg)
    
    # Valida se thresholds é um dicionário válido
    if not check_if_is_valid_dict(thresholds):
        msg = "Parâmetro thresholds é um dicionário inválido ou vazio"
        if logger:
            logger.error(msg)
        raise ValueError(msg)
    
    try:
        # Conta a quantidade de linhas do dataframe
        total = df.count()  
        if total == 0:
            if logger:
                logger.warning("DF vazio; retornando EMPTY_DF.")
            empty_metrics = [(c, 0, 0, None, thr, "EMPTY_DF") for c, thr in thresholds.items()]

            return False, df.sparkSession.createDataFrame(
                empty_metrics, 
                ["Column", "Null_Count", "Total_Count", "Null_Ratio", "Threshold", "Status"]
            )
        
        #  Nulls + total para TODAS colunas
        null_stats = df.select(
            *[F.count(F.when(F.col(c).isNull(), c)).alias(f"nulls_{c}") 
            for c in thresholds.keys()]
        ).collect()[0]
        
        # Monta métricas
        metrics = []
        for c, thr in thresholds.items():
            nulls = null_stats[f"nulls_{c}"] or 0
            ratio = nulls / total
            status = "FAIL" if ratio > thr else "OK"
            
            # Missing column check
            if c not in df.columns:
                status = "MISSING_COLUMN"
                ratio = None
            
            metrics.append((c, nulls, total, ratio, thr, status))
        
        metrics_df = df.sparkSession.createDataFrame(
            metrics, 
            ["Column", "Null_Count", "Total_Count", "Null_Ratio", "Threshold", "Status"]
        )
        
        # 1 linha: tem FAIL?
        flg_has_fails = metrics_df.filter(F.col("Status") == "FAIL").count() > 0
        
        if logger:
            status = "OK" if not flg_has_fails else "FAIL"
            logger.info(f"Null ratios {status}")
        
        return not flg_has_fails, metrics_df
    
    except Exception as e:
        msg = f"Erro ao verificar thresholds de nulos: {e}"
        if logger:
            logger.exception(msg)
        raise Exception(msg)

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
