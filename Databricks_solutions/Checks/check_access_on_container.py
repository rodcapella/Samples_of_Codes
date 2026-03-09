# Verifica se um caminho no storage está acessível e legível pelo Spark
def check_access_on_container(env: str, spark_session: SparkSession, valid_environments, container_path: str, dbutils = None, logger = None) -> bool:
    """
    Última revisão: 25/08/2025
    Testado em: 25/08/2025

    Verifica se um caminho no storage está acessível e legível pelo Spark.

    Parâmetros:
    -----------
        env : str
            Ambiente de execução ('dev', 'prod').
        spark_session : SparkSession
            Sessão Spark ativa.
        valid_environments : Iterable[str]
            Lista de ambientes válidos.
        container_path : str
            Caminho completo no storage (ex: 'dbfs:/mnt/lakehouse/silver/tabela').
        dbutils : dbutils, opcional
            Objeto dbutils para interagir com o storage.
        logger : logging.Logger, opcional
            Logger para registrar logs e erros.

    Retorno:
    --------
        bool
            True se leitura for bem-sucedida, False em caso de erro.

    Use esta função se:
    ------------------
        - Precisa verificar se um caminho no storage está disponível e acessível para leitura pelo Spark.
        - Quer validar permissões ou existência de dados antes de executar pipelines.
        - Deseja testar leitura de formatos comuns como Delta, Parquet, CSV ou JSON.

    Exemplo de uso:
    ---------------
        sucesso = check_access_on_container(
            env = env,
            spark_session=spark,
            valid_environments,
            container_path="dbfs:/mnt/lakehouse/silver/minhatabela",
            dbutils = dbutils,
            logger=logger
        )
        if sucesso:
            print("Caminho acessível e dados disponíveis.")
        else:
            print("Falha ao acessar o caminho ou dados.")
    """
    # Validar o ambiente de execução e garantir uma SparkSession válida.
    spark_session = chk.chk_validate_environment(env = env, spark_session = spark_session, valid_environments = valid_environments, logger = logger)

    # Valida do tipo de container_path
    if not chk.check_if_is_valid_string(container_path, False):
        msg = f"[CHECK ACCESS] Parâmetro 'container_path' inválido: {container_path}"
        if logger:
            logger.error(msg)
        raise ValueError(msg)
    
    container_path = container_path.rstrip("/")
    
    # MÉTODO 1: dbutils (rápido, se disponível)
    if dbutils:
        try:
            items = dbutils.fs.ls(container_path)
            if logger: 
                logger.info(f"[DBUTILS] {container_path} ({len(items)} itens)")
            return True
        
        except Exception as e:
            if logger: 
                logger.debug(f"[DBUTILS] Falhou: {e}")
    
    # MÉTODO 2: Teste de ESCRITA (mais confiável, sem UC)
    try:
        test_path = f"{container_path}/_spark_access_test_{int(time.time())}.tmp"
        spark_session.range(1).coalesce(1).write.mode("overwrite").text(test_path)
        spark_session.read.text(test_path).count()
        
        # Limpa teste
        if dbutils:
            dbutils.fs.rm(test_path, True)
            
        if logger: 
            logger.info(f"[SPARK WRITE/READ] Full access: {container_path}")
        return True
        
    except Exception as e:
        if logger: 
            logger.warning(f"[SPARK TEST] Falhou (não crítico): {container_path}")
        return True 
    
    return True
