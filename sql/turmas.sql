WITH turmas_atuais AS (
    SELECT
        escola_id,
        serie_id,
        count(1) AS quantidade,
        count(1) * {qtd_max_alunos} AS capacidade
    FROM turma GROUP BY turma_id),

demanda_atual AS (
    SELECT
        escola_id,
        serie_id,
        count(1) AS demanda
    FROM base_alunos
    WHERE formulario <= {possibilita_abertura_novas_turmas}
    GROUP BY escola_id, serie_id)

SELECT
    regiao.nome || '_' || substr(serie.nome, 1, 1) AS nome,
    escola_id,
    serie_id,
    {qtd_max_alunos} AS qtd_max_alunos,
    {qtd_professores_acd} AS qtd_professores_acd,
    {qtd_professores_pedagogico} AS qtd_professores_pedagogico,
    COALESCE(demanda / capacidade, 1) + 1 AS salas
    --CASE WHEN turmas_atuais.quantidade > 0 THEN 1 ELSE 0 END AS aprova,
    --CASE WHEN COALESCE(demanda / capacidade, 1) + 1 > turmas_atuais.quantidade THEN 1 ELSE 0 END nao_aprova_todas
FROM demanda_atual
LEFT JOIN turmas_atuais USING (escola_id, serie_id)
LEFT JOIN escola USING (escola_id)
LEFT JOIN serie USING (serie_id)
LEFT JOIN regiao USING (regiao_id)