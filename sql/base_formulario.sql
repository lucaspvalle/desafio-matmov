CREATE VIEW base_formulario AS
WITH base AS (
SELECT
    'formulario_' || id AS cod,
    id,
    nome,
    cpf,
    escola_id,
    serie_id + ({ano_planejamento} - ano_referencia) * (1 - {otimiza_dentro_do_ano}) AS serie_id,
    data_inscricao,
    email_aluno,
    telefone_aluno,
    nome_responsavel,
    telefone_responsavel,
    nome_escola_origem,
    NULL AS status_id
FROM formulario_inscricao
GROUP BY cpf)

SELECT base.* FROM base
LEFT JOIN serie USING (serie_id)
WHERE serie.ativa = 1