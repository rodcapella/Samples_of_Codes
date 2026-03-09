# Verifica se a tabela Delta é otimizada (sem recomendações) ou não de execução do VACUUM
def check_if_delta_table_needs_vacuum(env: str,
                                      spark_session: SparkSession,
                                      path_or_table: str,
                                      valid_environments,
                                      vacuum_if_last_run_older_than_days: int = 14,
                                      min_vacuum_retain_hours: int = 168,                                     
                                      logger = None
    ) -> dict:
    """
    Última revisão: 11/08/2025


    Verifica se a tabela Delta é otimizada (sem recomendações) ou não de execução do VACUUM.

    Parâmetros:
    -----------
        env : str
            Ambiente de execução do notebook.
        spark_session : SparkSession
            Instância do SparkSession.
        path_or_table : str
            Nome de tabela no metastore (ex.: 'silver.sales') **ou** caminho Delta (ex.: 'abfss://.../tabela/').
        valid_environments : Iterable
            Iterável contendo os domínios válidos para o ambiente.
        vacuum_if_last_run_older_than_days : int
            Se o último VACUUM for mais antigo que esse número de dias (ou nunca executado), recomenda VACUUM.
        min_vacuum_retain_hours : int
            Retenção mínima recomendada (em horas) ao executar o VACUUM. Padrão: 168h (7 dias).
        logger : logging.Logger, opcional
            Logger para registrar resultados e recomendações.

    Retorno:
    --------
        dict
            {
                "exists": bool,
                "is_delta": bool,
                "needsVacuum": bool,               # True se recomendar VACUUM
                "lastVacuumAt": str | None,        # timestamp do último VACUUM (se houver)
                "deletedFileRetention": str | None,# valor de delta.deletedFileRetentionDuration
                "recommendations": list[str],
                "suggestedCommands": list[str]
            }

    Use esta função se:
    -------------------
        - Precisa avaliar a “saúde” de retenção e manutenção da Delta Table (VACUUM).
        - Quer automatizar alertas para execuções periódicas de limpeza de arquivos não referenciados.

    Exemplo de uso:
    ---------------
        res = check_if_delta_table_needs_vacuum(env, spark_session, "silver.sales", valid_environments, vacuum_if_last_run_older_than_days=21, logger=logger)
        if res["needsVacuum"]:
            for cmd in res["suggestedCommands"]:
                print(cmd)  # VACUUM silver.sales RETAIN 168 HOURS;
    """
    # Validar o ambiente de execução e garantir uma SparkSession válida.
    spark_session = chk.chk_validate_environment(env = env, spark_session = spark_session, valid_environments = valid_environments, logger = logger)

    # Valida parâmetros numéricos ou vazios
    if not chk.check_if_is_valid_integer(vacuum_if_last_run_older_than_days):
        msg = "vacuum_if_last_run_older_than_days deve ser um número inteiro."
        if logger:
            logger.error(msg)
        raise ValueError(msg)

    if not chk.check_if_is_valid_integer(min_vacuum_retain_hours):
        msg = "min_vacuum_retain_hours deve ser um número inteiro."
        if logger:
            logger.error(msg)
        raise ValueError(msg)

    # Validação básica do parâmetro
    if not chk.check_if_is_valid_string(path_or_table, False):
        msg = f"O parâmetro path_or_table='{path_or_table}' não é uma string válida."
        if logger:
            logger.error(msg)
        raise ValueError(msg)
    
    try:
        # Resolve identificador (tabela vs. caminho)
        flg_is_path = check_if_is_physical_path(path_or_table, logger)
        
        ident = f"delta.`{path_or_table}`" if flg_is_path else path_or_table

        flg_tbl_exists = check_if_table_exists_in_delta_table(  env = env,
                                                                spark_session = spark_session,
                                                                full_delta_path = path_or_table,
                                                                valid_environments = valid_environments,
                                                                valid_schemas = valid_schemas,
                                                                logger = logger
        )      
        # Verifica se a tabela existe em Delta Lake
        if not flg_tbl_exists:
            if logger:
                logger.error(f"Tabela não existe: {path_or_table}")
                
            return {
                "exists": False, "is_delta": False, "optimized": False,
                "avgFileSizeMB": None, "numFiles": None,
                "recommendations": ["Tabela não existe."], "suggestedCommands": []
            }

        # Lê propriedades da tabela (ex.: delta.deletedFileRetentionDuration)
        props_rows = spark_session.sql(f"SHOW TBLPROPERTIES {ident}").collect()
        tblprops = {r.key: r.value for r in props_rows if hasattr(r, "key")}
        deleted_ret_raw = tblprops.get("delta.deletedFileRetentionDuration")  # pode ser None
        deleted_ret_h = _parse_interval_hours(deleted_ret_raw) if deleted_ret_raw else None

        # Analisa histórico para encontrar último VACUUM
        hist_df = spark_session.sql(f"DESCRIBE HISTORY {ident}")
        hist = hist_df.toPandas() if hist_df is not None else pd.DataFrame()
        last_vacuum_ts = None

        if not hist.empty and "operation" in hist.columns and "timestamp" in hist.columns:
            vacs = hist[hist["operation"].str.lower().str.contains("vacuum", na=False)]
            if not vacs.empty:
                last_vacuum_ts = vacs["timestamp"].max()

        # Define recomendações com base em tempo desde último VACUUM e política de retenção
        now_utc = datetime.now(timezone.utc)
        flg_needs_vacuum = False
        recommendations = []

        if last_vacuum_ts is None:
            # Nunca executou VACUUM → recomenda executar com retenção mínima
            recommendations.append(f"Nenhum VACUUM encontrado no histórico → executar VACUUM RETAIN {min_vacuum_retain_hours} HOURS.")
            flg_needs_vacuum = True

        else:
            # Verifica idade do último VACUUM
            days_since = (now_utc - pd.to_datetime(last_vacuum_ts, utc=True)).days
            if days_since >= vacuum_if_last_run_older_than_days:
                recommendations.append(f"Último VACUUM há {days_since} dias → executar novamente.")
                flg_needs_vacuum = True

        # Se a retenção configurada é menor que o mínimo recomendado, sugere ajuste/execução
        if deleted_ret_h is not None and deleted_ret_h < min_vacuum_retain_hours:
            recommendations.append(
                f"delta.deletedFileRetentionDuration={deleted_ret_raw} < {min_vacuum_retain_hours}h (mínimo recomendado)."
            )
            flg_needs_vacuum = True

        # Monta comandos sugeridos
        commands = [f"VACUUM {ident} RETAIN {min_vacuum_retain_hours} HOURS;"] if needflg_needs_vacuums_vacuum else []

        if logger:
            logger.info(f"[VACUUM CHECK] needsVacuum={flg_needs_vacuum} lastVacuum={last_vacuum_ts} deletedRetention={deleted_ret_raw}")

            for rec in recommendations:
                logger.warning(f"[Recomendação] {rec}")

        # Retorno padronizado
        return {
            "exists": True,
            "is_delta": True,
            "needsVacuum": flg_needs_vacuum,
            "lastVacuumAt": str(last_vacuum_ts) if last_vacuum_ts is not None else None,
            "deletedFileRetention": deleted_ret_raw,
            "recommendations": recommendations or ["VACUUM em dia: nenhuma ação necessária."],
            "suggestedCommands": commands
        }

    except Exception as e:
        if logger:
            logger.exception(f"Erro ao verificar necessidade de VACUUM: {e}")
        return {
            "exists": False, "is_delta": False, "needsVacuum": False,
            "lastVacuumAt": None, "deletedFileRetention": None,
            "recommendations": [f"Erro ao avaliar necessidade de VACUUM: {e}"],
            "suggestedCommands": []
        }

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
