# Verifica se a tabela Delta é compactada (OPTIMIZE) ou se recomenda compactação
def check_if_delta_table_needs_optimize(env: str,
                                        spark_session: SparkSession,
                                        path_or_table: str,
                                        valid_environments,
                                        valid_schemas,
                                        target_avg_file_mb: int = 128,
                                        max_files_threshold: int = 5000,
                                        logger = None
    ) -> dict:
    """
    Última revisão: 18/08/2025


    Verifica se a tabela Delta é compactada (OPTIMIZE) ou se recomenda compactação.

    Parâmetros:
    -----------
        env : str
            Ambiente atual (ex.: 'dev', 'prod').
        spark_session : SparkSession
            Instância do SparkSession.
        path_or_table : str
            Nome de tabela no metastore (ex.: 'silver.sales') **ou** caminho Delta (ex.: 'abfss://.../tabela/').
        valid_environments : Iterable
            Iterável contendo os domínios válidos.
        valid_schemas : Iterable
            Iterável contendo os schemas válidos.
        target_avg_file_mb : int
            Tamanho médio (MB) desejado por arquivo para considerar a tabela "compactada".
            Valores típicos: 64–256 MB conforme perfil de leitura.
        max_files_threshold : int
            Limite total de arquivos acima do qual recomendaremos compaction (OPTIMIZE).
        logger : logging.Logger, opcional
            Logger para registrar métricas e recomendações.

    Retorno:
    --------
        dict
            Dicionário com status e recomendações:
            {
                "exists": bool,
                "is_delta": bool,
                "optimized": bool,              # True se não houver recomendação de OPTIMIZE
                "avgFileSizeMB": float,
                "numFiles": int,
                "recommendations": list[str],   # mensagens de recomendação
                "suggestedCommands": list[str]  # comandos SQL úteis (ex.: OPTIMIZE <tabela>)
            }

    Use esta função se:
    -------------------
        - Deseja avaliar fragmentação de arquivos (small files) e decidir se compaction (OPTIMIZE) é necessária.
        - Quer um relatório objetivo para acionar rotinas de manutenção de Delta.

    Exemplo de uso:
    ---------------
        res = check_if_delta_table_needs_optimize(env, spark_session, "silver.sales", valid_environments, valid_schemas, target_avg_file_mb=128, logger=logger)
        if not res["optimized"]:
            for cmd in res["suggestedCommands"]:
                print(cmd)  # OPTIMIZE silver.sales;
    """
    # Valida iteráveis
    for name, value in [("valid_environments", valid_environments),
                        ("valid_schemas", valid_schemas)]:
        if not hasattr(value, "__iter__"):
            raise TypeError(f"Esperava um iterável para {name}, mas recebeu {type(value)}")

    # Validar o ambiente de execução e garantir uma SparkSession válida.
    spark_session = chk.chk_validate_environment(env = env, spark_session = spark_session, valid_environments = valid_environments, logger = logger)

    # Valida se target_avg_file_mb é um inteiro positivo e não nulo
    if not chk.check_if_is_valid_integer(target_avg_file_mb):
        msg = "target_avg_file_mb deve ser um inteiro positivo."
        if logger:
            logger.error(msg)
        raise ValueError(msg)

    # Valida se max_files_threshold é um inteiro positivo e não nulo
    if not chk.check_if_is_valid_integer(max_files_threshold):
        msg = "max_files_threshold deve ser um inteiro positivo."
        if logger:
            logger.error(msg) 
        raise ValueError(msg)
    
    try:
        # Detecta se é um caminho físico ou nome lógico de tabela
        flg_is_path = check_if_is_physical_path(path_or_table, logger)

        # Para DESCRIBE DETAIL, caminho físico precisa ser anotado como delta.`<path>`
        ident = f"delta.`{path_or_table}`" if flg_is_path else path_or_table

        flg_table_exists = check_if_table_exists_in_delta_table(env = env,
                                                                spark_session = spark_session,
                                                                full_delta_path = path_or_table,
                                                                valid_environments = valid_environments,
                                                                valid_schemas = valid_schemas,
                                                                logger = logger
        )      
        # Verifica se a tabela existe em Delta Lake
        if not flg_table_exists:
            if logger:
                logger.error(f"Tabela não existe: {path_or_table}")             
            return {
                "exists": False, "is_delta": False, "optimized": False,
                "avgFileSizeMB": None, "numFiles": None,
                "recommendations": ["Tabela não existe."], "suggestedCommands": []
            }

        # Extrai métricas diretas do DESCRIBE
        flg_is_delta = detail.get("format", "").lower() == "delta"
        
        if not flg_is_delta:
            if logger:
                logger.error(f"Não é Delta: {path_or_table}")
            return {
                "exists": True, "is_delta": False, "optimized": False,
                "avgFileSizeMB": None, "numFiles": None,
                "recommendations": ["Não é tabela Delta."], "suggestedCommands": []
            }

        # Calcula tamanho médio por arquivo
        num_files = int(detail.get("numFiles") or 0)
        size_bytes = int(detail.get("sizeInBytes") or 0)
        avg_mb = (size_bytes / num_files) / (1024 * 1024) if num_files > 0 else 0.0

        # Aplica heurísticas de recomendação
        recommendations = []
        if avg_mb < float(target_avg_file_mb):
            recommendations.append(
                f"Tamanho médio por arquivo {avg_mb:.1f} MB < alvo {target_avg_file_mb} MB → recomendar OPTIMIZE."
            )
        if num_files > max_files_threshold:
            recommendations.append(
                f"Número total de arquivos ({num_files}) > limite ({max_files_threshold}) → recomendar OPTIMIZE."
            )

        # Considera otimizado quando não há recomendações
        optimized = len(recommendations) == 0
        commands = [f"OPTIMIZE {ident};"] if not optimized else []

        if logger:
            logger.info(f"[OPTIMIZE CHECK] files={num_files} avgMB={avg_mb:.1f} optimized={optimized}")
            for rec in recommendations:
                logger.warning(f"[Recomendação] {rec}")

        # Retorno padronizado
        return {
            "exists": True,
            "is_delta": True,
            "optimized": optimized,
            "avgFileSizeMB": avg_mb,
            "numFiles": num_files,
            "recommendations": recommendations or ["Sem recomendações: tabela parece otimizada."],
            "suggestedCommands": commands
        }

    except Exception as e:
        if logger:
            logger.exception(f"{e}")
        # Falhas: tabela inexistente, permissões, sintaxe
        return {
            "exists": False, "is_delta": False, "optimized": False,
            "avgFileSizeMB": None, "numFiles": None,
            "recommendations": [f"Erro ao avaliar necessidade de OPTIMIZE: {e}"],
            "suggestedCommands": []
        }
