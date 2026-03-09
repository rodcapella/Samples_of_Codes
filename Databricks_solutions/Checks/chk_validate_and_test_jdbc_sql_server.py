# Validar URL/driver JDBC SQL Server, opcionalmente write_mode, E SEMPRE testa a conexão.
def chk_validate_and_test_jdbc_sql_server(
    env: str,
    spark_session: SparkSession,
    jdbc_url: str,
    valid_environments,
    valid_write_modes = None,
    write_mode: str | None = None,
    sql_server_driver: str = None,
    logger = None
) -> bool:
    """
    Última versão: 02/02/2026
    Testado em: 

    Validar URL/driver JDBC SQL Server, opcionalmente write_mode, E SEMPRE testa a conexão.

    Cache integrado: Evita reteste da mesma conexão (env+jdbc_url) durante ETL.

    Parâmetros:
    -----------
        env : str
            Ambiente atual (dev, prod).
        spark_session : SparkSession
            Sessão Spark ativa.
        jdbc_url : str
            String JDBC completa para SQL Server.
        valid_environments : Iterable
            Ambientes válidos.
        valid_write_modes : Iterable, opcional
            Modos de escrita válidos para Spark/Delta.
        write_mode : str, opcional
            Modo de escrita ('append', 'overwrite', etc.).
        sql_server_driver : str, opcional
            Driver JDBC para SQL Server.
        logger : logging.Logger, opcional

    Retorno:
    --------
        bool: True se tudo OK (validações + conexão), False se falha na conexão.

    Use esta função se:
    -------------------
        - Precisa validar uma URL JDBC SQL Server.
        - Quer testar a conexão com SELECT 1.
        - Deseja garantir que a conexão com SQL Server seja bem-sucedida antes de realizar operações de escrita.

    Exemplo de uso:
    ---------------
        success = chk_validate_and_test_jdbc_sql_server(
            env=env, spark_session=spark, jdbc_url=jdbc_url,   
            valid_environments=valid_environments,
            valid_write_modes=valid_write_modes,
            write_mode="append",
            sql_server_driver=sql_server_driver, 
            logger=logger
        )
    """
    # CACHE GLOBAL (evita reteste)
    cache_key = f"{env}_{jdbc_url}"
    if cache_key in globals().get('__jdbc_cache__', {}):
        if logger:
            logger.debug(f"JDBC cache HIT: {cache_key}")
        return True
    
    df = None

    try:
        # Valida ambiente + SparkSession
        spark_session = chk.chk_validate_environment(
            env = env,
            spark_session = spark_session,
            valid_environments = valid_environments,
            logger = logger
        )

        # Validação obrigatória da JDBC URL
        if not chk.check_if_is_valid_jdbc_url(jdbc_url):
            msg = f"jdbc_url inválida ou mal formatada: {jdbc_url}"
            if logger:
                logger.error(msg)
            raise ValueError(msg)
        
        # Validação opcional do driver SQL Server
        if sql_server_driver:        
            # Valida se sql_server_driver é uma string válida
            if not chk.check_if_is_valid_sql_server_driver(sql_server_driver):
                msg = f"SQL Server driver inválida: {sql_server_driver}."
                if logger:
                    logger.error(msg)
                raise ValueError(msg)

            # Testa a conexão JDBC
            test_query = "SELECT 1 AS test_connection"
            df = None
            try:
                df = spark_session.read.format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("query", test_query) \
                    .option("driver", sql_server_driver) \
                    .load()

                df.collect()  # força execução para testar conexão
                if logger:
                    logger.info(f"Conexão JDBC SQL Server OK.")

            except Exception as e:
                msg = f"Erro ao testar conexão JDBC SQL Server: {e}"
                if logger:
                    logger.exception(msg)
                raise Exception(msg)

        # Validação opcional do write_mode
        if write_mode is not None:
            if valid_write_modes is None or not hasattr(valid_write_modes, "__iter__"):
                msg = f"valid_write_modes inválido: {valid_write_modes}"
                if logger:
                    logger.error(msg)
                raise TypeError(msg)

            flg_write_mode_ok = chk.check_if_is_valid_write_mode(
                write_mode_value = write_mode,
                valid_write_modes = valid_write_modes
            )
            if not flg_write_mode_ok:
                msg = f"write_mode inválido: {write_mode}"
                if logger:
                    logger.error(msg)
                raise ValueError(msg)
        
        # CACHE VÁLIDO - 1h TTL implícito
        if not hasattr(globals(), '__jdbc_cache__'):
            globals()['__jdbc_cache__'] = {}
        globals()['__jdbc_cache__'][cache_key] = True
        
        if logger:
            logger.info(f"JDBC cacheado: {cache_key}")
        return True

    except AnalysisException as e:
        if logger:
            logger.exception(f"Erro de análise JDBC: {e}")
        return False
    
    except Exception as e:
        if logger:
            logger.exception(f"Erro ao validar/testar JDBC SQL Server: {e}")
        raise

    finally:
        # Limpa DataFrame da memória
        if df is not None:
            try:
                utl.util_delete_dataframe_variable_from_scope(
                    ["df"],
                    "both",
                    logger
                )
            except Exception:
                pass
