# Valida se a localização da tabela no metastore existe no storage do Data Lake.
def check_metastore_table_location_exists(env: str, spark_session: SparkSession, full_table_name: str, valid_environments, logger = None) -> dict:
    """
    Última revisão: 22/10/2025
    Testado em: 22/10/2025

    Valida se a localização da tabela no metastore existe no storage do Data Lake.

    Parâmetros:
    -----------
        env : str
            Ambiente de execução (ex.: "dev", "prod").
        spark_session : SparkSession
            Sessão Spark ativa.
        full_table_name : str
            Nome totalmente qualificado da tabela no metastore (ex.: "silver.clientes").
        valid_environments : Iterable
            Iterável de ambientes válidos (ex.: ["dev", "prod"]).
        logger : logging.Logger, opcional
            Logger para registrar mensagens.

    Retorno:
    --------
        result : dict
            Relatório com:
            - "table": nome da tabela
            - "format": formato lógico (delta/parquet/etc.)
            - "location": path físico registrado no metastore
            - "location_accessible": "true" | "false"
            - "error": mensagem de erro (se houver)

    Use esta função se:
    -------------------
        - Precisa garantir que o LOCATION registrado no metastore aponta para um path existente e acessível no storage.
        - Quer diagnosticar tabelas “órfãs” (metastore aponta para path removido).

    Exemplo de uso:
    ---------------
        report = check_metastore_table_location_exists(env, spark, "silver.clientes", valid_environments, logger)
        print(report)
    """
    # Validar o ambiente de execução e garantir uma SparkSession válida.
    spark_session = chk.chk_validate_environment(env = env, spark_session = spark_session, valid_environments = valid_environments, logger = logger)

    # Valida se a tabela é uma string válida e não vazia
    if not chk.check_if_is_valid_string(full_table_name, True):
        msg = "O nome da tabela é inválido ou vazio"
        if logger: 
            logger.error(msg)
        raise ValueError(msg)

    result = {
        "table": full_table_name,
        "format": "",
        "location": "",
        "location_accessible": "false",
        "error": ""
    }
    try:
        # Usa a função criada para obter a localização da tabela
        loc = utl.util_get_table_location_from_metastore(env, spark_session, full_table_name, valid_environments, logger)
        result["location"] = loc

        # Usa DESCRIBE DETAIL para obter metadados e formato
        detail = spark_session.sql(f"DESCRIBE DETAIL {full_table_name}").collect()[0].asDict()
        fmt = (detail.get("format") or "").lower()
        result["format"] = fmt

        if not loc:
            result["error"] = "LOCATION não encontrado no metastore."
            if logger: 
                logger.warning(f"{full_table_name}: LOCATION ausente no metastore.")

        # Tenta listar o path para validar se está acessível
        try:
            dbutils.fs.ls(loc)
            result["location_accessible"] = "true"
            if logger:
                logger.info(f"[Metastore] LOCATION acessível: {full_table_name} -> {loc}")

        except Exception as e:
            result["error"] = str(e)
            result["location_accessible"] = "false"
            if logger:
                logger.exception(f"[Metastore] LOCATION inacessível: {full_table_name} -> {loc} | {e}")
                   
        return result
    
    except Exception as e:
        msg = f"Falha ao inspecionar LOCATION de {full_table_name}: {e}"
        if logger: 
            logger.exception(msg)
        raise Exception(msg)
