# Verifica se já foram aplicadas operações de Z-ORDER em uma Delta Table.
def check_delta_table_zorder_status(env: str, spark_session: SparkSession, valid_environments, valid_schemas, delta_table_path: str, logger = None) -> bool:
    """
    Última revisão: 21/08/2025


    Verifica se já foram aplicadas operações de Z-ORDER em uma Delta Table.

    Parâmetros:
    -----------
        env : str
            Ambiente atual.
        spark_session : SparkSession
            Sessão Spark ativa.
        valid_environments : Iterable
            Iterável contendo os ambientes válidos.
        valid_schemas : Iterable
            Iterável contendo os esquemas válidos.
        delta_table_path : str 
            Caminho da tabela Delta.
        logger : Logger, opcional) 
            Logger para registrar mensagens.

    Returno:
    --------
        has_zorder: bool 
            True se houver registro de Z-ORDER no histórico, False caso contrário.
    
    Use esta função se:
    -------------------
        - Precisa verificar se uma tabela Delta possui Z-ORDER aplicado.
        - Quer analisar o histórico de operações para identificar operações de Z-ORDER.
        - Deseja tomar decisões com base na presença ou ausência de Z-ORDER.

    Exemplo de uso:
    ----------------
        delta_table_path = "/path/to/delta/table"
        has_zorder = check_delta_table_zorder_status(env, spark, valid_environments, delta_table_path)
    """
    # Valida iteráveis
    for name, value in [("valid_environments", valid_environments),
                        ("valid_schemas", valid_schemas)]:
        if not hasattr(value, "__iter__"):
            raise TypeError(f"Esperava um iterável para {name}, mas recebeu {type(value)}")

    # Validar o ambiente de execução e garantir uma SparkSession válida.
    spark_session = chk.chk_validate_environment(env = env, spark_session = spark_session, valid_environments = valid_environments, logger = logger)

    # Valida se delta_table_path é um caminho físico válido
    if not check_if_is_physical_path(delta_table_path, logger):
        msg = "delta_table_path deve ser um caminho físico"
        if logger:
            logger.error(msg)
        raise ValueError(msg)

    # Valida se a Delta Table existe
    if not check_if_table_exists_in_delta_table(env = env,
                                                spark_session = spark_session,
                                                full_delta_path = delta_table_path,
                                                valid_environments = valid_environments,
                                                valid_schemas = valid_schemas,
                                                logger = logger
    ):
        msg = "Delta Table não encontrada"
        if logger:
            logger.error(msg)
        raise ValueError(msg)
    
    try:
        delta_table = DeltaTable.forPath(spark_session, delta_table_path)
        latest_op = delta_table.history(1).collect()[0]
        
        has_zorder = 'ZORDER' in latest_op.get('operation', '').upper()
        op_params = latest_op.get('operationParameters', '{}')
        
        if has_zorder and logger:
            logger.info(f"Z-ORDER detectado: {delta_table_path}")

        elif logger:
            logger.warning(f"Sem Z-ORDER: {delta_table_path}")
            
        return has_zorder
    
    except Exception as e:
        msg = f"Erro ao verificar Z-ORDER: {e}"
        if logger:
            logger.exception(msg)
        raise Exception(msg)

    finally:
        try:
            # Limpa dataframe da memória imediatamente após uso
            flg_ok_clear = utl.util_delete_dataframe_variable_from_scope(
                ["hist_df"],
                "both",
                logger
            )
        except Exception:
            pass    
