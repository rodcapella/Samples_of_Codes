# Retornar as configurações ativas da SparkSession.
def util_get_active_configs_on_spark_session(spark_session: SparkSession, prefix: str = None, show_as_df: bool = True, limit: int = None):
    """
    Última revisão: 20/08/2025
    Testado em: 27/08/2025

    Retornar as configurações ativas da SparkSession.

    Parâmetros:
    -----------
        spark_session : SparkSession
            Sessão Spark ativa.
        prefix : str, opcional
            Filtra apenas configurações que contenham o texto informado (ex: "azure", "delta").
        show_as_df : bool, default=True
            Se True, Retornar como DataFrame Pandas para melhor leitura.
            Se False, imprime em formato chave=valor.
        limit : int, opcional
            Número máximo de configs a mostrar (default: todas).

    Retorno:
    --------
        Se show_as_df=True:
            DataFrame Pandas com as configs ativas.
        Caso contrário:
            None (imprime diretamente no console).

    Use esta função se:
    -------------------
        - Você deseja exibir as configurações ativas da SparkSession.
        - Você deseja filtrar as configurações por um prefixo específico.
        - Você deseja visualizar as configurações ativas em um DataFrame Pandas para melhor leitura.

    Exemplo de uso:
    ---------------
        util_get_active_configs_on_spark_session(spark)                     # mostra todas configs
        util_get_active_configs_on_spark_session(spark, prefix="azure")     # filtra configs de azure
        util_get_active_configs_on_spark_session(spark, prefix="delta", show_as_df=False)
    """
    try:
        configs = spark_session.sparkContext.getConf().getAll()

        if prefix:
            prefix_lower = prefix.lower()
            configs = [ (k, v) for k, v in configs if prefix_lower in k.lower() ]

        if limit:
            configs = configs[:limit]

        if show_as_df:
            df = pd.DataFrame(configs, columns=["config_key", "config_value"])
            df.show(truncate=False)
            return df
        
        else:
            for k, v in configs.items():
                print(f"{k} = {v}")

    except Exception as e:
        msg = f"Erro ao obter configurações ativas do SparkSession na função util_get_active_configs_on_spark_session: {e}"
        if logger:
            logger.exception(msg)
        raise Exception(msg)
