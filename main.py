from ortools.linear_solver import pywraplp
from data import *
import sqlite3

solver = pywraplp.Solver.CreateSolver('CBC')
cnx = sqlite3.connect("data.db")

info = get_parametros(cnx)
alunos = get_alunos(cnx, info)
turmas = get_turmas(cnx, alunos, info)


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

# Alunos de mesmas turma devem continuar juntos
# (alunos
#  .query('cluster > 0')
#  .groupby(['cluster', 'id_turma'])
#  .apply(lambda r: solver.Add(solver.Sum(r['v_aluno']) == r['id'].count() * r.iloc[0]['v_turma'],
#                              f"mantem_juntos_{r.iloc[0].cluster}_{r.iloc[0].id_turma}")))

# A turma recebe alunos apenas se for aberta
(alunos
 .groupby('id_turma')
 .apply(lambda r: solver.Add(solver.Sum(r['v_aluno']) <= 1000 * r.iloc[0]['v_turma'],
                             f"turmas_abertas_{r.iloc[0].id_turma}")))

# Quantidade máxima de alunos por turma
(alunos
 .groupby('id_turma')
 .apply(lambda r: solver.Add(solver.Sum(r['v_aluno']) <= info['qtd_max_alunos'],
                             f"qtd_max_alunos_{r.iloc[0].id_turma}")))

# Prioriza ordem de inscrição (não está diferenciando alunos)
# (alunos
#  .query('data_inscricao.notnull()')
#  .merge(alunos[['id', 'cluster', 'v_aluno']], left_on=['inscricao_anterior', 'cluster'],
#         right_on=['id', 'cluster'], suffixes=['', '_anterior'])
#  .groupby(['id', 'id_turma'])
#  .apply(lambda r: solver.Add(solver.Sum(r['v_aluno']) <= 1000 * solver.Sum(r['v_aluno_anterior']),
#                              f"prioridade_formulario_{r.iloc[0].id}_{r.iloc[0].id_turma}")))

# Modelo de custos da ONG
CUSTO_PROFESSOR = (info['qtd_professores_pedagogico'] + info['qtd_professores_acd']) * info['custo_professor']  # noqa
solver.Add(solver.Sum(alunos['v_aluno']) * info['custo_aluno'] + solver.Sum(turmas['v_turma']) * CUSTO_PROFESSOR <=
           info['limite_custo'], f"limite_custos")


# 3) Função Objetivo
turmas_vazias = (alunos
                 .groupby('id_turma')
                 .apply(lambda r: info['qtd_max_alunos'] * r.iloc[0]['v_turma'] - solver.Sum(r['v_aluno'])))

solver.Maximize(solver.Sum(alunos['v_aluno']) - solver.Sum(turmas_vazias))


# 4) Execução
# with open("modelo.txt", "w") as f:
#     f.write(solver.ExportModelAsLpFormat(False))

status = solver.Solve()

if status == pywraplp.Solver.OPTIMAL or status == pywraplp.Solver.FEASIBLE:
    print("Alocação realizada com sucesso!\n")
    print("Custo total: ", solver.Objective().Value(), "\n")

    alunos['sol_alunos'] = alunos.apply(lambda r: r['v_aluno'].solution_value(), axis=1)
    turmas['sol_turmas'] = turmas.apply(lambda r: r['v_turma'].solution_value() if r['serie_id'] != 2 else 1, axis=1)

    sol_aluno(cnx, alunos)
    sol_priorizacao_formulario(cnx, alunos)
    sol_turma(cnx, turmas, info)

else:
    print("Não há solução!")
    truncate_tables(cnx)

cnx.close()
