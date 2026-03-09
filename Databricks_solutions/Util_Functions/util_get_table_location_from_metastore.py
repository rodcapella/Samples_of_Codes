# Retornar o caminho (location) físico no storage da tabela especificada registrada no metastore
def util_get_table_location_from_metastore( env: str,
                                            spark_session: SparkSession,
                                            full_table_name: str,
                                            valid_environments,
                                            valid_schemas,
                                            logger = None
    ) -> str:
    """
    Última revisão: 22/10/2025
    Testado em: 22/10/2025

    Retornar o caminho (location) físico no storage da tabela especificada registrada no metastore do Spark/Delta Lake.

    Parâmetros:
    -----------
        env : str
            Ambiente atual (ex: dev, prod).
        spark_session : SparkSession
            Sessão Spark ativa (obrigatório).
        full_table_name: str
            Nome completo da tabela no formato <schema>.<nome_tabela>.
        valid_environments: Iterable
            Iterável contendo os ambientes válidos para validação.
        valid_schemas: Iterable
            Iterável contendo os schemas válidos para validação
        logger : logging.Logger, opcional
            Logger para registrar mensagens informativas ou erros.

    Retorno:
    --------
        delta_path : str
            Caminho físico (location) onde a tabela está armazenada no storage.

    Exemplos de uso:
    ---------------
        location = util_get_table_location_from_metastore(
            env="dev",
            spark_session=spark,
            full_table_name="silver.clientes",
            valid_environments=["dev", "prod"],
            valid_schemas=["bronze", "silver", "gold"],
            logger=logger)
        print(location)
        # Output exemplo: abfss://silver@storageaccount.dfs.core.windows.net/silver/clientes
    """
    # Valida iteráveis
    for name, value in [("valid_environments", valid_environments),
                        ("valid_schemas", valid_schemas)]:
        if not hasattr(value, "__iter__"):
            raise TypeError(f"Esperava um iterável para {name}, mas recebeu {type(value)}")

    # Validar o ambiente de execução e garantir uma SparkSession válida.
    spark_session = chk.chk_validate_environment(env = env, spark_session = spark_session, valid_environments = valid_environments, logger = logger)

    # Valida full_table_name
    if not chk.check_if_is_valid_string(full_table_name, False):
        msg = f"Nome da tabela '{full_table_name}' inválido."
        if logger:
            logger.error(msg)
        raise ValueError(msg)

    # Separa schema e table name
    schema_name, table_name = hlp.hlp_parse_table_name(full_table_name)

    # Validar opcionalmente o nome do schema e/ou da tabela para operações em Metastore, Delta Lake ou fontes externas.
    flg_ok_schema_table = chk.chk_validate_schema_and_table(schema_name = schema_name, table_name = table_name, valid_schemas = valid_schemas, logger = logger)
    if not flg_ok_schema_table:
        msg = f"Schema ou tabela inválidos: {schema_name}, {table_name}"
        if logger:
            logger.error(msg)
        raise ValueError(msg)

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
        
        # Obter location físico da tabela
        delta_path = (
            spark_session.sql(f"DESCRIBE DETAIL {full_table_name}")
            .select("location")
            .collect()[0][0]
        )

        if logger:
            logger.info(f"Localização da tabela {full_table_name}: {delta_path}")
        return delta_path
    
    except Exception as e:
        msg = f"Erro ao obter location da tabela {full_table_name}: {str(e)}"
        if logger:
            logger.exception(msg)
        raise e
