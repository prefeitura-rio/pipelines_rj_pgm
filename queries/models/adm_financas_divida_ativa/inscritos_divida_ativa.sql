SELECT
  SAFE_CAST(anoinscricao AS INT64) AS ano_inscricao,

  CASE
    WHEN nome = '-' THEN NULL
    ELSE SAFE_CAST(nome AS STRING)
  END AS nome,

  CASE
    WHEN LENGTH(cpf_cnpj) = 11 THEN 'CPF'
    WHEN LENGTH(cpf_cnpj) = 14 THEN 'CNPJ'
    ELSE NULL
  END AS tipo_documento,

  CASE
    WHEN cpf_cnpj = '-' THEN NULL
    ELSE SAFE_CAST( cpf_cnpj AS STRING)
   END AS cpf_cnpj,

  CASE
    WHEN cpf_cnpj_formatado = '-' THEN NULL
    ELSE SAFE_CAST( cpf_cnpj_formatado AS STRING)
  END AS cpf_cnpj_formatado,

  SAFE_CAST(valdebito AS FLOAT64) AS valor_debito,

  CAST(PARSE_DATETIME("%d/%m/%Y %H:%M", REPLACE(ultima_atualizacao, 'h', ':')) AS DATETIME )AS data_ultima_atualizacao
FROM `rj-pgm.adm_financas_divida_ativa_staging.inscritos_divida_ativa`
ORDER BY
  ano_inscricao,
  nome



