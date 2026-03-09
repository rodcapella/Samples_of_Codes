# Valida se os valores de uma coluna numérica estão dentro de um intervalo permitido no dataframe.
def check_validate_column_number_range_on_df(df: 'DataFrame', column_name: str, min_value: int or float, max_value: int or float, logger = None) -> bool: 
    """
    Última revisão: 06/08/2025
    Testado em: 13/10/2025

    Valida se os valores de uma coluna numérica estão dentro de um intervalo permitido no dataframe.

    Parâmetros:
    ----------
        df : pyspark.sql.DataFrame
            DataFrame que contém os dados.
        column_name : str
            Nome da coluna a ser validada.
        min_value : int ou float
            Valor mínimo permitido (inclusive).
        max_value : int ou float
            Valor máximo permitido (inclusive).
        logger : logging.Logger, opcional
            Objeto logger para registrar mensagens (se fornecido).

    Retorno:
    -------
        bool: 
            Verdadeiro se todos os valores na coluna estão dentro do intervalo permitido, caso contrário, False.  

    Use esta função se:
    ------------------
        - Deseja garantir que os valores numéricos em uma coluna estejam dentro de um intervalo específico.
        - Precisa evitar valores extremos ou inválidos antes de persistir dados.
        - Quer registrar alertas ou erros para valores fora do esperado.

    Exemplo de uso:
    ---------------
        valid = check_validate_column_number_range_on_df(
                    df=my_df,
                    column_name="idade",
                    min_value=18,
                    max_value=65,
                    logger=my_logger
                )
        if valid:
            print("Todos os valores estão dentro do intervalo permitido.")
        else:
            print("Existem valores fora do intervalo permitido.")
    """
    # Validação centralizada do DataFrame
    flg_ok_df = chk_validate_dataframe(df = df, required_columns = column_name, logger = logger)
    if not flg_ok_df:
        msg = "O DataFrame não é válido ou não contém as colunas necessárias."
        if logger:
            logger.error(msg)    
        raise ValueError(msg)

    # Validação do tipo do valor mínimo
    if not check_if_is_valid_integer(min_value):
        if not check_if_is_valid_float(min_value):
            msg = "O valor mínimo deve ser um número inteiro ou float não negativo."
            if logger:
                logger.error(msg)
            raise ValueError(msg)

    # Validação do tipo do valor máximo
    if not check_if_is_valid_integer(max_value):
        if not check_if_is_valid_float(max_value):
            msg = "O valor máximo deve ser um número inteiro ou float."
            if logger:
                logger.error(msg)
            raise ValueError(msg)
    
    try:
        # conta violações + extremos
        stats = (df.agg(
            F.sum(F.when(F.col(column_name) < min_value, 1).otherwise(0)).alias("below_min"),
            F.sum(F.when(F.col(column_name) > max_value, 1).otherwise(0)).alias("above_max"),
            F.min(column_name).alias("actual_min"),
            F.max(column_name).alias("actual_max")
        ).collect()[0])
        
        below_min = stats["below_min"] or 0
        above_max = stats["above_max"] or 0
        
        # Verifica violações
        if below_min > 0 or above_max > 0:
            if logger:
                logger.warning(
                    f"Range inválido '{column_name}': "
                    f"{below_min} < {min_value}, {above_max} > {max_value} "
                    f"[real: {stats['actual_min']:.2f} - {stats['actual_max']:.2f}]"
                )
            return False
        
        # Sucesso
        if logger:
            logger.info(f"Range OK '{column_name}': [{min_value} - {max_value}]")
        return True
    
    except Exception as e:
        msg = f"Erro durante a validação de intervalo na função check_validate_column_number_range_on_df: {str(e)}"
        if logger:
            logger.exception(msg)
        raise Exception(msg)

    finally:
        try:
            # Limpa dataframe da memória imediatamente após uso
            flg_ok_clear = utl.util_delete_dataframe_variable_from_scope(
                ["df"],
                "both",
                logger
            )
        except Exception:
            pass 
