SELECT
  Nome AS descNomeCliente,
  Data_da_Reclamacao AS dtDataReclamacao,
  Categoria_da_Queixa AS descCategoriaQueixa,
  Status_da_Reclamacao AS descStatusReclamacao,
  Canal_da_Reclamacao AS descCanalReclamacao,
  Valor_Envolvido_na_Reclamacao AS nrValorReclamacao

FROM bronze.sys_reclamacao.reclamacao