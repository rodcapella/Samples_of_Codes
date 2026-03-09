# Listar todas as tabelas existentes em um database do metastore, ordenadas pelo nome em ordem crescente.
def util_list_tables_in_database_metastore(env: str, 
                                           spark_session: SparkSession, 
                                           schema_name: str, 
                                           valid_environments, 
                                           valid_schemas, 
                                           logger = None
    ) -> list[str]:
    """
    Última revisão: 26/08/2025
    Testado em: 26/08/2025

    Listar todas as tabelas existentes em um database do metastore, ordenadas pelo nome em ordem crescente.

    Parâmetros:
    -----------
        env : str
            Ambiente de execução (ex: 'dev', 'prod').
        spark_session : SparkSession
            Sessão Spark ativa.
        schema_name : str
            Nome do schema para listar tabelas.
        valid_environments : Iterable
            Interável (Lista, tupla, set) com nomes de ambientes válidos.
        valid_schemas : Iterable
            Interável (Lista, tupla, set) com nomes de schemas válidos.
        logger : logging.Logger
            Objeto de log para registrar informações.

    Retorno:
    --------
        tables_list: list[str]
            Listar com nomes das tabelas no database ordenadas em ordem crescente.
    
    Use esta função se:
    -------------------
        - Precisa recuperar a Lista de tabelas existentes em um database do metastore Spark para análises ou operações.
        - Quer garantir que a Lista esteja ordenada para facilitar visualização ou processamento subsequente.

    Exemplo de uso:
    ---------------
        tables = util_list_tables_in_database_metastore(env, spark_session, "silver", valid_environments, valid_schemas)
        print(tables)
        ['clientes', 'produtos', 'vendas']
    """
    # Valida iteráveis
    for name, value in [("valid_environments", valid_environments),
                        ("valid_schemas", valid_schemas)]:
        if not hasattr(value, "__iter__"):
            raise TypeError(f"Esperava um iterável para {name}, mas recebeu {type(value)}")

    # Validar o ambiente de execução e garantir uma SparkSession válida.
    spark_session = chk.chk_validate_environment(env = env, spark_session = spark_session, valid_environments = valid_environments, logger = logger)

    # Valida se o nome do schema é válido e não vazio
    if not chk.check_if_is_valid_schema_name(schema_name, valid_schemas):
        msg = f"O nome do schema é inválido ou vazio"
        if logger:
            logger.error(msg)
        raise ValueError(msg)  

    try:
        # Executa comando para obter tabelas do database em um Dataframe   
        # 1 QUERY → 1 DataFrame → 1 collect()
        tables = (spark_session.sql(f"SHOW TABLES IN `{schema_name}`")
                 .filter("isTemporary = false")
                 .select("tableName")
                 .orderBy("tableName")
                 .rdd.flatMap(lambda x: x)
                 .collect())
        
        if logger:
            logger.info(f"Schema '{schema_name}': {len(tables)} tabelas")      
        return tables

    except Exception as e:
        msg = f"Erro ao listar tabelas no database '{schema_name}'"
        if logger:
            logger.exception(f"{msg}. Detalhes: {str(e)}")
        raise Exception(f"{msg}. Causa original: {str(e)}") from e
