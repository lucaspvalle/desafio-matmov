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

    tipo = {True: "formulario", False: "matriculado"}
    alunos["v_aluno"] = alunos.apply(lambda r: solver.BoolVar(f"{tipo[r.formulario]}_{r.id}_{r.id_turma}"), axis=1)

    return alunos


def v_turmas(turmas: pd.DataFrame) -> pd.DataFrame:
    """
    Define a abertura de turmas.

    :param turmas: oferta vigente de turmas
    :return: variável de decisão binária para abertura de turmas
    """

    turmas["v_turma"] = turmas.apply(lambda r: solver.BoolVar(f"turma_{r.id}"), axis=1)
    return turmas


def c_alunos_matriculados(alunos: pd.DataFrame):
    """
    Alunos já matriculados na ONG, que desejam continuar, devem ser alocados em uma turma

    SUM(TURMA, v_alunos(ALUNO, TURMA) | (ALUNO in MATRICULADO)) = 1

    :param alunos: alunos já matriculados na ONG
    """

    (alunos
     .query('~ formulario')
     .groupby('id')
     .apply(lambda r: solver.Add(r['v_aluno'].values.sum() == 1, f"continua_{r.iloc[0].id}")))


def c_alunos_de_formulario(alunos: pd.DataFrame):
    """
    Alunos inscritos no formulário podem ser alocados em uma (e somente uma) turma.

    SUM(TURMA, v_alunos(ALUNO, TURMA) | (ALUNO in FORMULARIO)) <= 1

    :param alunos: alunos na lista de espera da ONG
    """

    (alunos
     .query('formulario')
     .groupby('id')
     .apply(lambda r: solver.Add(r['v_aluno'].values.sum() <= 1, f"inscricao_{r.iloc[0].id}")))


def c_agrupa_colegas(alunos: pd.DataFrame):
    """
    Alunos já matriculados na ONG, que estudavam em uma mesma turma, devem continuar juntos

    SUM(COLEGA, v_alunos(COLEGA, TURMA)) <= M * v_alunos(ALUNO, TURMA)

    :param alunos: alunos já matriculados na ONG
    """

    v_agregados_em_cluster = (alunos
                              .query('~ formulario')
                              .groupby(['cluster', 'id_turma'])
                              .apply(lambda df: df['v_aluno'].values.sum()))
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
     .apply(lambda r: solver.Add(r['v_aluno'].values.sum() <= big_M * r.iloc[0]['v_turma'],
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
     .apply(lambda r: solver.Add(r['v_aluno'].values.sum() <= info['qtd_max_alunos'],
                                 f"max_alunos_{r.iloc[0].id_turma}")))


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

    solver.Add(alunos['v_aluno'].values.sum() * info['custo_aluno']
               + turmas['v_turma'].values.sum() * custo_professor <= info['limite_custo'], f"limite_custos")


def funcao_objetivo(info: dict, alunos: pd.DataFrame, turmas: pd.DataFrame):
    """
    Função Objetivo: maximizar a quantidade de alunos assistidos com a maior ocupação possível de turmas,
    priorizando a ordem de inscrição dos alunos de formulário e turmas de anos mais novos

    max SUM((ALUNO, TURMA), v_alunos(ALUNO, TURMA) * peso_inscricao(ALUNO))
        - SUM(TURMA, penaliza_vagas_remanescentes(TURMA))

    :param info: máximo de alunos definido pelo usuário
    :param alunos: alunos cadastrados no sistema
    :param turmas: oferta vigente de turmas
    """

    # Prioriza turmas de anos mais novos
    max_serie_id = turmas['serie_id'].values.max() + 1
    alunos['peso_serie'] = (max_serie_id - alunos['serie_id']) / (max_serie_id * 5)

    # Penaliza as vagas remanescentes quando uma turma é aberta
    penaliza_vagas_remanescentes = (alunos
                                    .groupby('id_turma')
                                    .apply(lambda df: info['qtd_max_alunos'] * df.iloc[0]['v_turma']
                                           - df['v_aluno'].values.sum()))

    # Função Objetivo
    solver.Maximize((alunos['v_aluno'] * alunos['peso_inscricao'] * alunos['peso_serie']).values.sum()
                    - penaliza_vagas_remanescentes.values.sum() * 0.01)


def otimiza(alunos: pd.DataFrame, turmas: pd.DataFrame) -> (bool, pd.DataFrame, pd.DataFrame):
    """
    Executa o modelo de otimização.

    :param alunos: alunos cadastrados no sistema
    :param turmas: oferta vigente de turmas
    :return: status do resultado e as estruturas de dados
    """

    status = (solver.Solve() == pywraplp.Solver.OPTIMAL or pywraplp.Solver.FEASIBLE)

    if status:
        print("Alocação realizada com sucesso!\n")
        print("Objetivo: ", solver.Objective().Value())

        alunos['sol_alunos'] = alunos.apply(lambda r: r['v_aluno'].solution_value(), axis=1) == 1
        turmas['sol_turmas'] = turmas.apply(lambda r: r['v_turma'].solution_value(), axis=1) == 1
    else:
        print("Não há solução!")

    return status, alunos, turmas
