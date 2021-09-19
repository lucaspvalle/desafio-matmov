CREATE VIEW base_alunos AS
WITH base AS (
    SELECT
        'matriculado_' || aluno.id AS cod,
        aluno.nome,
        aluno.id AS id,
        aluno.cpf,
        turma_id AS cluster,
        escola_id,
        turma.serie_id + (1 - aluno.reprova) * (1 - {otimiza_dentro_do_ano}) AS serie_id,
        NULL AS data_inscricao,
        1 AS peso_inscricao,
        0 AS formulario,
        email_aluno,
        telefone_aluno,
        nome_responsavel,
        telefone_responsavel,
        nome_escola_origem
    FROM aluno
    LEFT JOIN turma USING (turma_id)
    WHERE aluno.continua = 1
    GROUP BY cpf

    UNION ALL

    SELECT
        'formulario_' || formulario.id AS cod,
        formulario.id AS id,
        formulario.nome,
        formulario.cpf,
        0 AS cluster,
        escola_id,
        serie_id + ({ano_planejamento} - ano_referencia) * (1 - {otimiza_dentro_do_ano}) AS serie_id,
        data_inscricao,
        0 AS peso_inscricao,
        1 AS formulario,
        email_aluno,
        telefone_aluno,
        nome_responsavel,
        telefone_responsavel,
        nome_escola_origem
    FROM formulario_inscricao formulario
    GROUP BY cpf)

SELECT base.* FROM base
LEFT JOIN serie USING (serie_id)
WHERE serie.ativa = 1