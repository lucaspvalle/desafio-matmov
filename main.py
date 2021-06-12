from ortools.linear_solver import pywraplp
import data
import sqlite3

solver = pywraplp.Solver.CreateSolver('CBC')
cnx = sqlite3.connect("data.db")


# Importando dados
info = data.get_parametros(cnx)
alunos = data.get_alunos(cnx, info)
turmas = data.get_turmas(cnx, alunos, info)

# 1) Variáveis de decisão
# Abertura de turmas (prioriza turmas de 9 ano, isto é: serie_id = 2)
turmas["v_turma"] = turmas.apply(lambda r: solver.BoolVar(f"turma_{r.id}") if r['serie_id'] != 2 else 1, axis=1)

# Combinando alunos com possíveis turmas
alunos = (alunos
          .merge(turmas[['id', 'escola_id', 'serie_id', 'v_turma']], left_on=["escola_id", "nova_serie_id"],
                 right_on=["escola_id", "serie_id"], suffixes=["", "_turma"])
          .drop('nova_serie_id', axis=1))

# Alunos x turma
alunos["v_aluno"] = alunos.apply(lambda r: solver.BoolVar(f"aluno_{r.cluster}_{r.id}_{r.id_turma}"), axis=1)

# 2) Restrições
# Alunos matriculados devem ser alocados em uma turma
(alunos
 .query('cluster > 0')
 .groupby('id')
 .apply(lambda r: solver.Add(solver.Sum(r['v_aluno']) == 1, f"continua_{r.iloc[0].cluster}_{r.iloc[0].id}")))

# Alunos inscritos no formulário podem ser alocados em no máximo uma turma
(alunos
 .query('cluster == 0')
 .groupby('id')
 .apply(lambda r: solver.Add(solver.Sum(r['v_aluno']) <= 1, f"inscricao_{r.iloc[0].cluster}_{r.iloc[0].id}")))

# Alunos de mesma turma (cluster) devem continuar juntos
v_agregados_em_cluster = (alunos
                          .query('cluster > 0')
                          .groupby(['cluster', 'id_turma'])
                          .apply(lambda df: solver.Sum(df['v_aluno'])))
(alunos
 .merge(v_agregados_em_cluster.to_frame('v_cluster'), left_on=['cluster', 'id_turma'], right_index=True)
 .apply(lambda r: solver.Add(r['v_cluster'] <= 1000 * r['v_aluno'], f"mantem_junto_{r.id}_{r.id_turma}"), axis=1))

# A turma recebe alunos apenas se for aberta
(alunos
 .groupby('id_turma')
 .apply(lambda r: solver.Add(solver.Sum(r['v_aluno']) <= 1000 * r.iloc[0]['v_turma'], f"abre_{r.iloc[0].id_turma}")))

# Quantidade máxima de alunos por turma
(alunos
 .groupby('id_turma')
 .apply(lambda r: solver.Add(solver.Sum(r['v_aluno']) <= info['qtd_max_alunos'], f"max_alunos_{r.iloc[0].id_turma}")))

# Prioriza ordem de inscrição
v_alunos_agregados_em_turmas = alunos.query('cluster == 0').groupby('id').apply(lambda df: solver.Sum(df['v_aluno']))
(alunos
 .query('cluster == 0')
 .merge(v_alunos_agregados_em_turmas.to_frame('v_anterior_agregado'), left_on='inscricao_anterior', right_index=True)
 .groupby('id')
 .apply(lambda df: solver.Add(solver.Sum(df['v_aluno']) <= df.iloc[0]['v_anterior_agregado'],
                              f"prioriza_{df.iloc[0].id}_{df.iloc[0].id_turma}")))

# Modelo de custos da ONG
CUSTO_PROFESSOR = (info['qtd_professores_pedagogico'] + info['qtd_professores_acd']) * info['custo_professor']  # noqa
solver.Add(solver.Sum(alunos['v_aluno']) * info['custo_aluno'] + solver.Sum(turmas['v_turma']) * CUSTO_PROFESSOR <=
           info['limite_custo'], f"limite_custos")


# 3) Função Objetivo
turmas_vazias = (alunos
                 .groupby('id_turma')
                 .apply(lambda df: info['qtd_max_alunos'] * df.iloc[0]['v_turma'] - solver.Sum(df['v_aluno'])))

solver.Maximize(solver.Sum(alunos['v_aluno']) - solver.Sum(turmas_vazias))


# 4) Execução
status = solver.Solve()

if status == pywraplp.Solver.OPTIMAL or status == pywraplp.Solver.FEASIBLE:
    print("Alocação realizada com sucesso!\n")
    print("Custo total: ", solver.Objective().Value(), "\n")

    alunos['sol_alunos'] = alunos.apply(lambda r: r['v_aluno'].solution_value(), axis=1)
    turmas['sol_turmas'] = turmas.apply(lambda r: r['v_turma'].solution_value() if r['serie_id'] != 2 else 1, axis=1)

    data.sol_aluno(cnx, alunos)
    data.sol_priorizacao_formulario(cnx, alunos)
    data.sol_turma(cnx, turmas, info)

else:
    print("Não há solução!")
    data.truncate_tables(cnx)

cnx.close()
