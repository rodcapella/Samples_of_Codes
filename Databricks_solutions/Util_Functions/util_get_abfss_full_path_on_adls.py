# Montar paths principais de containers no padrão definido de acesso ao Data Lake
def util_get_abfss_full_path_on_adls(container_name: str, storage_account_name: str, valid_container_names, logger = None) -> str:
    """
    Última revisão: 14/08/2025
    Testado em: 20/08/2025

    Montar paths principais de containers no padrão definido de acesso ao Data Lake

    Parâmetros:
    ----------
        container_name: str
            Nome do container a ser montado.
        storage_account_name: str
            Nome da conta de armazenamento.
        valid_container_names : iterable
            Iterável (Listar, tupla, set) com nomes de contêineres válidos.
        logger: logging.Logger
            Objeto de log para registrar informações.

    Retorno:
    --------
        full_path: str
            Path do container montado.

    Use esta função se:
    ------------------
        - Quer Montarr um path completo para um container no Data Lake.
        - Deseja Montarr um path completo para um container no Data Lake usando um dicionário de domínios válidos.

    Exemplo de uso:
    ---------------
        full_path = util_get_abfss_full_path_on_adls(container_name, storage_account_name, valid_container_names, logger)
        print(full_path)
    """
    # Valida iteráveis
    if not hasattr(valid_container_names, "__iter__"):
        raise TypeError(
            f"Esperava um iterável para {valid_container_names}, "
            f"mas recebeu {type(valid_container_names)}"
        )

    # Valida se o container_name é válido e não vazio
    if not chk.check_if_is_valid_container_name(container_name, valid_container_names):
        msg = f"Nome do container '{container_name}' inválido ou vazio"
        if logger:
            logger.error(msg) 
        raise ValueError(msg)

    # Valida se o storage_account_name é válido e não vazio
    if not chk.check_if_is_valid_string(storage_account_name, True):
        msg = f"Nome da conta de armazenamento '{storage_account_name}' inválido ou vazio"
        if logger:
            logger.error(msg)
        raise ValueError(msg)
    
    try:
        # Montar o path do container
        full_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"

        return full_path

    except Exception as e:
        msg = f"Ocorreu um erro ao montar o path do container: {e}"
        if logger:
            logger.exception(msg)
        raise e
