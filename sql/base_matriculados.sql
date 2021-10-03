CREATE VIEW base_matriculados AS
WITH base AS (
    SELECT
        'matriculado_' || id AS cod,
        id,
        aluno.nome,
        cpf,
        turma_id AS cluster,
        escola_id,
        turma.serie_id + (1 - aluno.reprova) * (1 - {otimiza_dentro_do_ano}) AS serie_id,
        email_aluno,
        telefone_aluno,
        nome_responsavel,
        telefone_responsavel,
        nome_escola_origem
    FROM aluno
    LEFT JOIN turma USING (turma_id)
    WHERE aluno.continua = 1
    GROUP BY cpf)

SELECT base.* FROM base
LEFT JOIN serie USING (serie_id)
WHERE serie.ativa = 1