SELECT
  Nome AS descNomeCliente,
  Gasto_Total AS descGastoTotal,
  Tipo_de_Cartao AS descTipoCartao,
  Bandeira AS descBandeira,
  Limite_de_Credito AS nrLimiteCredito,
  Status_do_Cartao AS descStatusCartao,
  Data_de_Emissao AS dtEmissao,
  Categoria_de_Gasto_Mais_Frequente AS descCategoriaGasto,
  Media_Mensal_de_Gasto AS nrMediaMensalGasto

FROM bronze.sys_transacao.transacao