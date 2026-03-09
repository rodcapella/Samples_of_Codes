# Retorna a lista de colunas de uma tabela específica do metastore (Delta Lake).
def util_get_columns_from_metastore_table(env: str,
                                         spark_session: SparkSession,
                                         schema_name: str,
                                         table_name: str,
                                         valid_environments,
                                         valid_schemas,
                                         logger = None
    ) -> list:
    """
    Última revisão: 28/01/2026
    Testado em:
    
    Retorna a lista de colunas de uma tabela específica do metastore (Delta Lake).
    
    Parâmetros:
    -----------
        env : str
            Ambiente atual (ex: 'dev', 'prod').
        spark_session : SparkSession
            Sessão Spark ativa.
        schema_name : str
            Nome do schema no metastore (ex: 'bronze').
        table_name : str
            Nome da tabela no metastore 
        valid_environments : Iterable
            Iterável de ambientes válidos.
        valid_schemas : Iterable
            Iterável de esquemas válidos.
        logger : logging.Logger, opcional
            Logger para registrar ações.
    
    Retorno:
    --------
        columns_list: list[str]
            Lista com nomes das colunas na ordem do esquema.
    
    Use esta função se:
    -------------------
        - Precisa obter a lista de colunas de uma tabela específica no metastore para realizar análises ou operações.

    Exemplo de uso:
    ---------------
        columns = util_get_columns_from_metastore_table(
            env, spark, schema_table, table_name, valid_environments, valid_schemas, logger
        )
        print(columns)  # ['id', 'placa', 'ano', ...]
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

        columns_df = spark_session.sql(f"DESCRIBE TABLE {full_table_name}")
        
        # Extrai apenas nomes das colunas (ignora colunas de metadados como # col_name)
        columns_list = []
        for row in columns_df.collect():
            col_name = row.col_name
            if col_name and not col_name.startswith('#'):  # Ignora linhas de metadados
                columns_list.append(col_name)
        
        if logger:
            logger.info(f"Tabela '{table_name}': {len(columns_list)} colunas - {columns_list}")    
        return columns_list
        
    except Exception as e:
        if logger:
            logger.exception(f"Erro ao obter colunas da tabela metastore '{table_name}': {e}")
        raise e
