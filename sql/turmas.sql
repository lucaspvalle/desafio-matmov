WITH turmas_atuais AS (
    SELECT
        escola_id,
        serie_id,
        count(1) AS quantidade,
        count(1) * {qtd_max_alunos} AS capacidade
    FROM turma GROUP BY turma_id),

alunos_x_escola_x_serie AS (
    SELECT escola_id, serie_id FROM base_matriculados
        UNION ALL
    SELECT escola_id, serie_id FROM base_formulario
        WHERE serie_id <= 100 * {possibilita_abertura_novas_turmas}),

demanda_atual AS (
    SELECT
        escola_id,
        serie_id,
        count(1) AS demanda
    FROM alunos_x_escola_x_serie
    GROUP BY escola_id, serie_id)

SELECT
    regiao.nome || '_' || substr(serie.nome, 1, 1) AS nome,
    escola_id,
    serie_id,
    NULL AS aprova,
    {qtd_max_alunos} AS qtd_max_alunos,
    {qtd_professores_acd} AS qtd_professores_acd,
    {qtd_professores_pedagogico} AS qtd_professores_pedagogico,
    COALESCE(demanda / capacidade, 1) + 1 AS salas
FROM demanda_atual
LEFT JOIN turmas_atuais USING (escola_id, serie_id)
LEFT JOIN escola USING (escola_id)
LEFT JOIN serie USING (serie_id)
LEFT JOIN regiao USING (regiao_id)