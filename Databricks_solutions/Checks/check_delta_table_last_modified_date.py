# Retornar a última data de modificação de uma tabela Delta Lake, consultando o histórico de transações.
def check_delta_table_last_modified_date(env: str, spark_session: SparkSession, delta_path: str, valid_environments, logger = None) -> datetime.datetime:
    """
    Última revisão: 28/08/2025
    Testado em: 28/08/2025

    Retornar a última data de modificação de uma tabela Delta Lake, consultando o histórico de transações.

    Parâmetros:
    -----------
        env : str
            Ambiente atual (ex: 'dev', 'prod').
        spark_session : SparkSession
            Sessão Spark ativa.
        delta_path : str
            Caminho físico da tabela Delta (ex: 'abfss://container@storageaccount.dfs.core.windows.net/tabela')
            ou nome da tabela registrada no metastore (ex: 'bronze.tabela').
        valid_environments : Interable
            Interável com os ambientes válidos (ex: ['dev', 'prod'])
        logger : logging.Logger, opcional
            Logger para registrar mensagens de debug ou erros.

    Retorno:
    --------
        last_modified : datetime.datetime:
            Timestamp da última modificação da tabela. Retorna None se não for Delta Table.

    Use esta função se:
    ------------------
        - Quer saber a última vez que uma tabela Delta foi alterada.
        - Deseja monitorar atualizações para triggers de pipelines ou auditoria.
    
    Exemplo de uso:
    ---------------
        last_mod = check_delta_table_last_modified_date(env, spark, "/mnt/bronze/minha_tabela", valid_environments)
        last_mod = check_delta_table_last_modified_date(env, spark, "bronze.minha_tabela", valid_environments)
    """
    # Validar o ambiente de execução e garantir uma SparkSession válida.
    spark_session = chk.chk_validate_environment(env = env, spark_session = spark_session, valid_environments = valid_environments, logger = logger)

    try:
        # Remove barra final do path físico
        delta_path = delta_path.rstrip("/")

        # Determina se é caminho físico ou metastore
        if check_if_is_physical_path(delta_path, logger):
            # Caminho físico
            if not DeltaTable.isDeltaTable(spark_session, delta_path):
                if logger:
                    logger.warning(f"Não é uma Delta Table no caminho: {delta_path}")

                return None
            
            dt = DeltaTable.forPath(spark_session, delta_path)

        else:
            # Tabela no metastore
            dt = DeltaTable.forName(spark_session, delta_path)

        # Consulta histórico
        history_df = dt.history(1)  # última transação
        last_modified = history_df.select(F.col("timestamp")).collect()[0][0]

        if logger:
            logger.info(f"Última modificação da tabela '{delta_path}': {last_modified}")
        return last_modified

    except Exception as e:
        if logger:
            logger.exception(f"Erro ao obter última modificação de '{delta_path}': {e}")
        raise e

    finally:
        try:
            # Limpa dataframe da memória imediatamente após uso
            flg_ok_clear = utl.util_delete_dataframe_variable_from_scope(
                ["df", "history_df", "dt"],
                "both",
                logger
            )
        except Exception:
            pass    
