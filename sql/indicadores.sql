CREATE VIEW indicadores AS
WITH base_sol_alunos AS (
    SELECT count(1) AS qtd_alunos_matriculados FROM sol_aluno),

    base_sol_formulario AS (
    SELECT count(1) AS qtd_alunos_formulario FROM sol_priorizacao_formulario),

    base_sol_alunos_agregado AS (
    SELECT
        qtd_alunos_matriculados,
        qtd_alunos_formulario,
        qtd_alunos_matriculados + qtd_alunos_formulario AS qtd_alunos
    FROM base_sol_alunos
    JOIN base_sol_formulario),

    base_sol_turmas AS (
    SELECT count(1) AS qtd_turmas FROM sol_turma)

SELECT
    qtd_alunos_matriculados,
    qtd_alunos_formulario,
    qtd_alunos,

    qtd_turmas AS qtd_turmas_abertas,
    qtd_turmas * {qtd_max_alunos} - qtd_alunos AS qtd_vagas_remanescentes,

    qtd_alunos * {custo_aluno} AS custo_alunos,
    qtd_turmas * {custo_professor_por_turma} AS custo_professores,
    qtd_alunos * {custo_aluno} + qtd_turmas * {custo_professor_por_turma} AS custo_total

FROM base_sol_alunos_agregado
JOIN base_sol_turmas