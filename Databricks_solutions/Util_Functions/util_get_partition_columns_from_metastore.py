# Retornar a lista de colunas de particionamento de uma tabela Delta registrada no metastore.
def util_get_partition_columns_from_metastore(  env: str, 
                                                spark_session: SparkSession, 
                                                schema_name: str, 
                                                table_name: str,
                                                valid_environments, 
                                                valid_schemas,
                                                logger = None
    ) -> list:
    """
    Última revisão: 17/10/2025
    Testado em: 22/10/2025
    
    Retornar a lista de colunas de particionamento de uma tabela Delta registrada no metastore.

    Parâmetros:
    -----------
        env : str
            Ambiente de execução.
        spark_session : SparkSession
            Sessão Spark ativa.
        schema_name : str
            Nome do schema da tabela (ex: 'silver').
        table_name : str
            Nome da tabela (ex: 'clientes').
        valid_environments : Iterable
            Iterável com os ambientes válidos.
        valid_schemas : Iterable
            Iterável com os schemas válidos.
        logger : logging.Logger, opcional
            Logger para registro.

    Retorno:
    --------
        partition_columns : list
            Lista de strings com as colunas utilizadas para particionamento.
            Lista vazia se a tabela não for particionada ou tabela não existir.

    Use esta função se:
    -------------------
        - Quer obter a lista de colunas de particionamento de uma tabela Delta registrada no metastore.

    Exemplo de uso:
    ---------------
        spark = SparkSession.builder.getOrCreate()  
        partition_columns = util_get_partition_columns_from_metastore(env, spark, "silver", "clientes", valid_environments, valid_schemas)
    """
    # Valida iteráveis
    for name, value in [("valid_schemas", valid_schemas),
                        ("valid_environments", valid_environments)]:
        if not hasattr(value, "__iter__"):
            raise TypeError(f"Esperava um iterável para {name}, mas recebeu {type(value)}")

    # Validar o ambiente de execução e garantir uma SparkSession válida.
    spark_session = chk.chk_validate_environment(env = env, spark_session = spark_session, valid_environments = valid_environments, logger = logger)

    # Validar opcionalmente o nome do schema e/ou da tabela para operações em Metastore, Delta Lake ou fontes externas.
    flg_ok_schema_table = chk.chk_validate_schema_and_table(schema_name = schema_name, table_name = table_name, valid_schemas = valid_schemas, logger = logger)
    if not flg_ok_schema_table:
        msg = f"Schema ou tabela inválidos: {schema_name}, {table_name}"
        if logger:
            logger.error(msg)
        raise ValueError(msg)
    
    # Define o nome completo da tabela no metastore
    full_table_name = hlp.hlp_create_full_table_name(schema_name, table_name)

    try:
        # Verificar se tabela existe no metastore (não no path físico).
        flg_exists = infra.check_if_table_exists_in_metastore(  env = env, 
                                                                spark_session = spark_session, 
                                                                schema_name = schema_name, 
                                                                table_name = table_name,
                                                                valid_environments = valid_environments, 
                                                                valid_schemas = valid_schemas, 
                                                                logger = logger
        ) 
        if not flg_exists:
            msg = f"Tabela {full_table_name} não existe no metastore."
            if logger:
                logger.error(msg)
            raise ValueError

        desc_df = spark_session.sql(f"DESCRIBE DETAIL {full_table_name}")

        # O campo partitionColumns contém as colunas de particionamento (lista)
        partition_columns = desc_df.collect()[0]["partitionColumns"]

        if logger:
            logger.info(f"Colunas de particionamento para {full_table_name}: {partition_columns}")

        return partition_columns if partition_columns else []
    
    except Exception as e:
        msg = f"Erro ao obter colunas de particionamento da tabela {full_table_name}: {e}"
        if logger:
            logger.exception(msg)
        raise e

    finally:
        try:
            # Limpa dataframe da memória imediatamente após uso
            flg_ok_clear = util_delete_dataframe_variable_from_scope(
                ["desc_df"],
                "both",
                logger
            )
        except Exception:
            pass
