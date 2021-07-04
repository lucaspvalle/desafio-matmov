from ortools.linear_solver import pywraplp
import pandas as pd

solver = pywraplp.Solver.CreateSolver('CBC')
big_M = 1000


def v_alunos(alunos: pd.DataFrame, turmas: pd.DataFrame) -> pd.DataFrame:
    """
    Combina alunos com possíveis turmas

    :param alunos: alunos cadastrados no sistema
    :param turmas: oferta vigente de turmas
    :return: variável de decisão binária de alunos x turmas
    """

    alunos = (alunos
              .merge(turmas[['id', 'escola_id', 'serie_id', 'v_turma']], left_on=["escola_id", "nova_serie_id"],
                     right_on=["escola_id", "serie_id"], suffixes=["", "_turma"])
              .drop('nova_serie_id', axis=1))

    alunos["v_aluno"] = alunos.apply(lambda r: solver.BoolVar(f"aluno_{r.cluster}_{r.id}_{r.id_turma}"), axis=1)
    return alunos


def v_turmas(turmas: pd.DataFrame) -> pd.DataFrame:
    """
    Define a abertura de turmas.
    Turmas de 9º ano (serie_id = 2) são abertas automaticamente.

    :param turmas: oferta vigente de turmas
    :return: variável de decisão binária para abertura de turmas
    """

    turmas["v_turma"] = turmas.apply(lambda r: solver.BoolVar(f"turma_{r.id}") if r['serie_id'] != 2 else 1, axis=1)
    return turmas


def c_alunos_matriculados(alunos: pd.DataFrame):
    """
    Alunos já matriculados na ONG, que desejam continuar, devem ser alocados em uma turma

    SUM(TURMA, v_alunos(ALUNO, TURMA) | (ALUNO in MATRICULADO)) = 1

    :param alunos: alunos já matriculados na ONG
    """

    (alunos
     .query('cluster > 0')
     .groupby('id')
     .apply(lambda r: solver.Add(solver.Sum(r['v_aluno']) == 1, f"continua_{r.iloc[0].cluster}_{r.iloc[0].id}")))


def c_alunos_de_formulario(alunos: pd.DataFrame):
    """
    Alunos inscritos no formulário podem ser alocados em uma (e somente uma) turma.

    SUM(TURMA, v_alunos(ALUNO, TURMA) | (ALUNO in FORMULARIO)) <= 1

    :param alunos: alunos na lista de espera da ONG
    """

    (alunos
     .query('cluster == 0')
     .groupby('id')
     .apply(lambda r: solver.Add(solver.Sum(r['v_aluno']) <= 1, f"inscricao_{r.iloc[0].cluster}_{r.iloc[0].id}")))


def c_agrupa_colegas(alunos: pd.DataFrame):
    """
    Alunos já matriculados na ONG, que estudavam em uma mesma turma, devem continuar juntos

    SUM(COLEGA, v_alunos(COLEGA, TURMA)) <= M * v_alunos(ALUNO, TURMA)

    :param alunos: alunos já matriculados na ONG
    """

    v_agregados_em_cluster = (alunos
                              .query('cluster > 0')
                              .groupby(['cluster', 'id_turma'])
                              .apply(lambda df: solver.Sum(df['v_aluno'])))
    (alunos
     .merge(v_agregados_em_cluster.to_frame('v_cluster'), left_on=['cluster', 'id_turma'], right_index=True)
     .apply(lambda r: solver.Add(r['v_cluster'] <= big_M * r['v_aluno'], f"mantem_junto_{r.id}_{r.id_turma}"), axis=1))


def c_abertura_de_turmas(alunos: pd.DataFrame):
    """
    A turma deve receber alunos apenas se o modelo decidir abri-la.

    SUM(ALUNO, v_alunos(ALUNO, TURMA)) <= M * v_turma(TURMA)

    :param alunos: alunos cadastrados no sistema
    """

    (alunos
        .groupby('id_turma')
        .apply(lambda r: solver.Add(solver.Sum(r['v_aluno']) <= big_M * r.iloc[0]['v_turma'],
                                    f"abre_{r.iloc[0].id_turma}")))


def c_maximo_de_alunos_por_turma(info: dict, alunos: pd.DataFrame):
    """
    Define a quantidade máxima de alunos por turma.

    SUM(ALUNO, v_alunos(ALUNO, TURMA)) <= qtd_max_alunos(TURMA)

    :param info: máximo de alunos definido pelo usuário
    :param alunos: alunos cadastrados no sistema
    """

    (alunos
        .groupby('id_turma')
        .apply(lambda r: solver.Add(solver.Sum(r['v_aluno']) <= info['qtd_max_alunos'],
                                    f"max_alunos_{r.iloc[0].id_turma}")))


def c_prioriza_ordem_de_inscricao(alunos: pd.DataFrame):
    """
    Os primeiros alunos da lista de espera devem ser priorizados.
    Alunos no fim da lista só podem ser alocados se os primeiros já estiverem com uma turma.

    SUM(TURMA, v_alunos(ALUNO, TURMA)) <= SUM((ALUNO_ATRAS_DA_LISTA, TURMA), v_alunos(ALUNO_ATRAS_DA_LISTA, TURMA))

    :param alunos: alunos cadastrados no sistema
    """

    v_alunos_agregados_em_turmas = (alunos
                                    .query('cluster == 0')
                                    .groupby('id')
                                    .apply(lambda df: solver.Sum(df['v_aluno'])))

    (alunos
     .query('cluster == 0')
     .merge(v_alunos_agregados_em_turmas.to_frame('v_anterior_agregado'), left_on='inscricao_anterior',
            right_index=True)
     .groupby('id')
     .apply(lambda df: solver.Add(solver.Sum(df['v_aluno']) <= df.iloc[0]['v_anterior_agregado'],
                                  f"prioriza_{df.iloc[0].id}_{df.iloc[0].id_turma}")))


def c_custos(info: dict, alunos: pd.DataFrame, turmas: pd.DataFrame):
    """
    Modelo de custos da ONG.

    SUM((ALUNO, TURMA), v_alunos(ALUNO, TURMA)) * custo(ALUNO) + SUM(TURMA, v_turmas(TURMA)) * custo(TURMA)
    <=
    limite_custo

    :param info: quantidade e custos de professores definidos pelo usuário
    :param alunos: alunos cadastrados no sistema
    :param turmas: oferta vigente de turmas
    """

    custo_professor = (info['qtd_professores_pedagogico'] + info['qtd_professores_acd']) * info['custo_professor']

    solver.Add(solver.Sum(alunos['v_aluno']) * info['custo_aluno'] + solver.Sum(turmas['v_turma']) * custo_professor <=
               info['limite_custo'], f"limite_custos")


def funcao_objetivo(info: dict, alunos: pd.DataFrame):
    """
    Função Objetivo: maximizar a quantidade de alunos assistidos, com a maior ocupação possível de turmas

    max SUM((ALUNO, TURMA), v_alunos(ALUNO, TURMA)) -
        SUM(TURMA, qtd_max_alunos * v_turmas(TURMA) - SUM(ALUNO, v_alunos(ALUNO, TURMA)))

    :param info: máximo de alunos definido pelo usuário
    :param alunos: alunos cadastrados no sistema
    """
    turmas_vazias = (alunos
                     .groupby('id_turma')
                     .apply(lambda df: info['qtd_max_alunos'] * df.iloc[0]['v_turma'] - solver.Sum(df['v_aluno'])))

    solver.Maximize(solver.Sum(alunos['v_aluno']) - solver.Sum(turmas_vazias))


def otimiza(alunos: pd.DataFrame, turmas: pd.DataFrame) -> (bool, pd.DataFrame, pd.DataFrame):
    """
    Executa o modelo de otimização.

    :param alunos: alunos cadastrados no sistema
    :param turmas: oferta vigente de turmas
    :return: status do resultado e as estruturas de dados
    """

    status = solver.Solve()

    if status == pywraplp.Solver.OPTIMAL or status == pywraplp.Solver.FEASIBLE:
        factivel = True

        print("Alocação realizada com sucesso!\n")
        print("Custo total: ", solver.Objective().Value(), "\n")

        alunos['sol_alunos'] = alunos.apply(lambda r: r['v_aluno'].solution_value(), axis=1)
        turmas['sol_turmas'] = turmas.apply(lambda r: r['v_turma'].solution_value() if r['serie_id'] != 2 else 1,
                                            axis=1)

    else:
        factivel = False

    return factivel, alunos, turmas
