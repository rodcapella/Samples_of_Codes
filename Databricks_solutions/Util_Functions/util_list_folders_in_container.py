# Obtém uma lista de pastas dentro de um container no ADLS
def util_list_folders_in_container(env : str, 
                                   spark_session: SparkSession,  
                                   container_name: str, 
                                   azure_storage_account_name : str, 
                                   valid_environments, 
                                   valid_containers_names,
                                   dbutils = None, 
                                   logger = None
    ) -> list:
    """
    Última revisão: 18/08/2025
    Testado em: 25/08/2025

    Obtém uma lista de pastas dentro de um container no ADLS.

    Parâmetros:
    -----------
        env: str
            Ambiência atual (ex.: dev, prod).
        spark_session : SparkSession
            Sessão Spark ativa.
        container_name : str
            Nome do container no Data Lake.
        azure_storage_account_name : str
            Nome da conta de armazenamento no ADLS.
        valid_environments : Iterable
            Interável (Listar, tupla, set) com nomes de ambientes válidos.
        valid_containers_names: Iterable
            Interável (Listar, tupla, set) com nomes de containers válidos.
        dbutils : dbutils, opcional
            Objeto dbutils para interagir com o ambiente Databricks
        logger : logging.Logger, opcional
            Logger para registrar informações e erros.

    Retorno:
    --------
        list
            Listar com os nomes das pastas encontradas no container.
            Retornar Listar vazia se não encontrar pastas ou se ocorrer erro.

    Use esta função se:
    -------------------
        - Precisa listar todos os diretórios (pastas) de um container no ADLS via ABFSS.
        - Deseja integrar com pipelines que Verificarm a existência de estruturas esperadas (ex.: Bronze/Silver/Gold).

    Exemplo de uso:
    ---------------
        folders = util_list_folders_in_container(env, SPARK_SESSION, "bronze-dev", azure_storage_account_name, valid_environments, valid_containers_names, dbutils, logger)
        print(folders)
    """
    # Valida iteráveis
    for name, value in [("valid_environments", valid_environments),
                        ("valid_containers_names", valid_containers_names)]:
        if not hasattr(value, "__iter__"):
            raise TypeError(f"Esperava um iterável para {name}, mas recebeu {type(value)}")

    # Validar o ambiente de execução e garantir uma SparkSession válida.
    spark_session = chk.chk_validate_environment(env = env, spark_session = spark_session, valid_environments = valid_environments, logger = logger)

    # Valida se container_name é válido e não vazio
    if not chk.check_if_is_valid_container_name(container_name, valid_containers_names):
        msg = f"O nome do container '{container_name}' é inválido ou vazio"
        if logger:
            logger.error(msg)
        raise ValueError(msg)

    # Valida se storage_account_name é válido e não vazio
    if not chk.check_if_is_valid_string(azure_storage_account_name, True):
        msg = f"O nome da conta de armazenamento '{azure_storage_account_name}' é inválido ou vazio"
        if logger:
            logger.error(msg)
        raise ValueError(msg)

    try:
        path = util_get_abfss_full_path_on_adls(container_name = container_name, 
                                                storage_account_name = azure_storage_account_name, 
                                                valid_container_names = valid_containers_names, 
                                                logger = logger
        )
        files = dbutils.fs.ls(path)

        if logger:
            for f in files:
                logger.info(f"Encontrado: {f.path} | isDir={f.isDir()}")

        # filtra apenas diretórios de primeiro nível
        folders = [f.name for f in files if f.isDir()]

        if logger:
            logger.info(f"Conexão com o container '{container_name}' estabelecida com sucesso.")
            logger.info(f"Pastas encontradas: {folders}")
        return folders

    except PermissionError as e:
        if logger:
            logger.exception(f"Erro de permissão/acesso no container '{container_name}'.")
        raise e 

    except Exception as e:
        if logger:
            logger.exception(f"Erro inesperado ao acessar '{container_name}': {e}")
        raise e
