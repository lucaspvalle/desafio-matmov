CREATE VIEW indicadores AS
WITH base_sol_alunos AS (
    SELECT count(1) AS qtd_alunos_matriculados FROM sol_aluno),

    base_sol_formulario AS (
    SELECT count(1) AS qtd_alunos_formulario FROM sol_priorizacao_formulario),

    base_sol_turmas AS (
    SELECT count(1) AS qtd_turmas FROM sol_turma)

SELECT
    qtd_alunos_matriculados,
    qtd_alunos_formulario,
    qtd_alunos_matriculados + qtd_alunos_formulario AS qtd_alunos,
    qtd_turmas AS qtd_turmas_abertas,
    qtd_turmas * 20 - (qtd_alunos_matriculados + qtd_alunos_formulario) AS qtd_vagas_remanescentes,
    (qtd_alunos_matriculados + qtd_alunos_formulario) * 100 AS custo_alunos,
    qtd_turmas * 400 AS custo_professores,
    (qtd_alunos_matriculados + qtd_alunos_formulario) * 100 + qtd_turmas * 400
        AS custo_total
FROM base_sol_alunos
JOIN base_sol_formulario
JOIN base_sol_turmas