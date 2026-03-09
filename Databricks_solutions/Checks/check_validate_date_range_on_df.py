#   Valida se todos os valores de uma coluna estão dentro do range de datas definido no dataframe, convertendo a coluna e os parâmetros min_date e max_date para date.
def check_validate_date_range_on_df(df: 'DataFrame', column_name: str, min_date, max_date, date_format: str ="yyyy-MM-dd", logger = None):
    """
    Última revisão: 06/08/2025


    Valida se todos os valores de uma coluna estão dentro do range de datas definido no dataframe, convertendo a coluna e os parâmetros min_date e max_date para date.

    Parâmetros:
    -----------
        df : pyspark.sql.DataFrame
            DataFrame Spark de entrada.
        column_name : str
            Nome da coluna de data (string, timestamp ou date).
        min_date : str, datetime.date ou datetime.datetime
            Data mínima permitida.
        max_date : str, datetime.date ou datetime.datetime
            Data máxima permitida.
        date_format : str, opcional
            Formato da data em caso de string. Default 'yyyy-MM-dd'.
        logger : logging.Logger, opcional
            Logger para logs.

    Retorno:
    --------
        bool: 
            True se todos os valores estão dentro do range, False caso contrário.

    Use esta função se:
    ------------------
        - Precisa garantir que as datas em uma coluna estejam dentro de um intervalo válido.
        - Quer evitar registros com datas inválidas que possam prejudicar análises ou processos.
        - Deseja registrar alertas ou falhas no logger para facilitar o monitoramento.

    Exemplo de uso:
    ---------------
        valid = check_validate_date_range_on_df(
                            df=my_df,
                            column_name="data_evento",
                            min_date="2023-01-01",
                            max_date=datetime.date(2025, 12, 31),
                            logger=my_logger
                )
        if valid:
            print("Todas as datas estão dentro do intervalo permitido.")
        else:
            print("Existem datas fora do intervalo permitido.")
    """
    # Validação centralizada do DataFrame
    flg_ok_df = chk_validate_dataframe(df = df, required_columns = column_name, logger = logger)
    if not flg_ok_df:
        msg = "O DataFrame não é válido ou não contém as colunas necessárias."
        if logger:
            logger.error(msg)    
        raise ValueError(msg)

    # Valida se o parâmetro min_date é uma string ou datetime.date ou datetime.datetime
    if not min_date or not isinstance(min_date, (str, datetime.date, datetime.datetime)):
        msg = "O parâmetro 'min_date' deve ser uma string ou datetime.date ou datetime.datetime"
        if logger:
            logger.error(msg)
        raise ValueError(msg)

    else:
        min_date_conv = ops.operation_convert_param_to_date(min_date, "min_date") # Retorna um datetime.date

    # Valida se o parâmetro max_date é uma string ou datetime.date ou datetime.datetime
    if not max_date or not isinstance(max_date, (str, datetime.date, datetime.datetime)):
        msg = "O parâmetro 'max_date' deve ser uma string ou datetime.date ou datetime.datetime"
        if logger:      
            logger.error(msg)
        raise ValueError(msg)
    
    else:
        max_date_conv = ops.operation_convert_param_to_date(max_date, "max_date") # Retorna um datetime.date

    try:
        # Converte a coluna para date (sem alterar o DataFrame original)
        df_date = ops.operation_convert_column_to_date_on_df(df, column_name)

        # Conta registros fora do range definido
        out_of_range = df_date.filter(
            (col(column_name) < str(min_date_conv)) | (col(column_name) > str(max_date_conv))
        ).count()

        if out_of_range > 0:
            msg = (f"{out_of_range} datas fora do range [{min_date_conv}, {max_date_conv}] na coluna '{column_name}'. "
                "Validação de datas falhou!")
            if logger: 
                logger.error(msg)
            raise Exception(msg)

        if logger:
            logger.info(f"Validação de datas concluída para coluna '{column_name}'. Todos os valores estão no intervalo permitido.")      
        return True

    except Exception as e:
        msg = f"Erro durante a validação de datas na função check_validate_date_range_on_df: {str(e)}"
        if logger:
            logger.exception(msg)
        raise Exception(msg)

    finally:
        try:
            # Limpa dataframe da memória imediatamente após uso
            flg_ok_clear = utl.util_delete_dataframe_variable_from_scope(
                ["df", "df_date"],
                "both",
                logger
            )
        except Exception:
            pass 
