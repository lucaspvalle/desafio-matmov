from ortools.linear_solver import pywraplp
import pandas as pd


class Otimizador:

    def __init__(self, quem: str, info: dict):
        self.solver = pywraplp.Solver.CreateSolver(quem)
        self.info = info

    def v_alunos(self, alunos: pd.DataFrame, turmas: pd.DataFrame) -> pd.DataFrame:
        """
        Combina alunos com possíveis turmas

        :param alunos: alunos cadastrados no sistema
        :param turmas: oferta vigente de turmas
        :return: variável de decisão binária de alunos x turmas
        """

        alunos = alunos.merge(turmas[['escola_id', 'serie_id', 'turma_id', 'v_turma']], on=['escola_id', 'serie_id'])
        alunos["v_aluno"] = alunos.apply(lambda r: self.solver.BoolVar(f"{r.cod}_{r.turma_id}"), axis=1)

        return alunos

    def v_turmas(self, turmas: pd.DataFrame) -> pd.DataFrame:
        """
        Define a abertura de turmas.

        :param turmas: oferta vigente de turmas
        :return: variável de decisão binária para abertura de turmas
        """

        turmas["v_turma"] = turmas.apply(lambda r: self.solver.BoolVar(f"turma_{r.turma_id}"), axis=1)
        return turmas

    def c_alunos_matriculados(self, alunos: pd.DataFrame):
        """
        Alunos já matriculados na ONG, que desejam continuar, devem ser alocados em uma turma

        SUM(TURMA, v_alunos(ALUNO, TURMA) | (ALUNO in MATRICULADO)) = 1

        :param alunos: alunos já matriculados na ONG
        """

        (alunos
         .query('~ formulario')
         .groupby('id')
         .apply(lambda r: self.solver.Add(r['v_aluno'].values.sum() == 1, f"continua_{r.iloc[0].cod}")))

    def c_alunos_de_formulario(self, alunos: pd.DataFrame):
        """
        Alunos inscritos no formulário podem ser alocados em uma (e somente uma) turma.

        SUM(TURMA, v_alunos(ALUNO, TURMA) | (ALUNO in FORMULARIO)) <= 1

        :param alunos: alunos na lista de espera da ONG
        """

        (alunos
         .query('formulario')
         .groupby('id')
         .apply(lambda r: self.solver.Add(r['v_aluno'].values.sum() <= 1, f"inscricao_{r.iloc[0].cod}")))

    def c_agrupa_colegas(self, alunos: pd.DataFrame):
        """
        Alunos já matriculados na ONG, que estudavam em uma mesma turma, devem continuar juntos

        v_alunos(ALUNO, TURMA) = v_alunos(COLEGA, TURMA)

        :param alunos: alunos já matriculados na ONG
        """

        colunas = ['cod', 'formulario', 'cluster', 'turma_id', 'v_turma', 'v_aluno']

        (alunos[colunas]
         .merge(alunos[colunas], on=['cluster', 'turma_id'], suffixes=['', '_colega'])
         .query('(~ formulario) & (cod != cod_colega)')
         .apply(lambda r: self.solver.Add(r['v_aluno'] == r['v_aluno_colega'],
                                          f"mantem_junto_{r.cod}_{r.cod_colega}_{r.turma_id}"), axis=1))

    def c_maximo_de_alunos_por_turma(self, alunos: pd.DataFrame):
        """
        Define a quantidade máxima de alunos por turma, se aberta.

        SUM(ALUNO, v_alunos(ALUNO, TURMA)) <= qtd_max_alunos(TURMA) * v_turma(TURMA)

        :param alunos: alunos cadastrados no sistema
        """

        (alunos
         .groupby('turma_id')
         .apply(lambda r: self.solver.Add(r['v_aluno'].values.sum() <= self.info['qtd_max_alunos'] *
                                          r.iloc[0]['v_turma'], f"max_alunos_{r.iloc[0].turma_id}")))

    def c_custos(self, alunos: pd.DataFrame, turmas: pd.DataFrame):
        """
        Modelo de custos da ONG.

        SUM((ALUNO, TURMA), v_alunos(ALUNO, TURMA)) * custo(ALUNO) + SUM(TURMA, v_turmas(TURMA)) * custo(TURMA)
        <=
        limite_custo

        :param alunos: alunos cadastrados no sistema
        :param turmas: oferta vigente de turmas
        """

        qtd_professores_por_turma = (self.info['qtd_professores_pedagogico'] + self.info['qtd_professores_acd'])
        custo_professor = qtd_professores_por_turma * self.info['custo_professor']

        self.solver.Add(alunos['v_aluno'].values.sum() * self.info['custo_aluno']
                        + turmas['v_turma'].values.sum() * custo_professor <= self.info['limite_custo'],
                        f"limite_custos")

    def funcao_objetivo(self, alunos: pd.DataFrame, turmas: pd.DataFrame):
        """
        Função Objetivo: maximizar a quantidade de alunos assistidos com a maior ocupação possível de turmas,
        priorizando a ordem de inscrição dos alunos de formulário e turmas de anos mais novos

        max SUM((ALUNO, TURMA), v_alunos(ALUNO, TURMA) * peso_inscricao(ALUNO))
            - SUM(TURMA, penaliza_vagas_remanescentes(TURMA))

        :param alunos: alunos cadastrados no sistema
        :param turmas: oferta vigente de turmas
        """

        # Prioriza turmas de anos mais novos
        max_serie_id = turmas['serie_id'].values.max() + 1
        alunos['peso_serie'] = (max_serie_id - alunos['serie_id']) / (max_serie_id * 5)

        # Penaliza as vagas remanescentes quando uma turma é aberta
        penaliza_vagas_remanescentes = (alunos
                                        .groupby('turma_id')
                                        .apply(lambda df: self.info['qtd_max_alunos'] * df.iloc[0]['v_turma']
                                               - df['v_aluno'].values.sum()))

        # Função Objetivo
        self.solver.Maximize((alunos['v_aluno'] * alunos['peso_inscricao'] * alunos['peso_serie']).values.sum()
                             - penaliza_vagas_remanescentes.values.sum() * 0.01)

    def otimiza(self, alunos: pd.DataFrame, turmas: pd.DataFrame) -> (bool, pd.DataFrame, pd.DataFrame):
        """
        Executa o modelo de otimização.

        :param alunos: alunos cadastrados no sistema
        :param turmas: oferta vigente de turmas
        :return: status do resultado e as estruturas de dados
        """

        # 1) Variáveis de decisão
        turmas = self.v_turmas(turmas)
        alunos = self.v_alunos(alunos, turmas)

        # 2) Restrições
        self.c_alunos_matriculados(alunos)
        self.c_alunos_de_formulario(alunos)
        self.c_agrupa_colegas(alunos)
        self.c_maximo_de_alunos_por_turma(alunos)
        self.c_custos(alunos, turmas)

        # 3) Função Objetivo
        self.funcao_objetivo(alunos, turmas)

        status = (self.solver.Solve() == pywraplp.Solver.OPTIMAL or pywraplp.Solver.FEASIBLE)

        if status:
            print("Alocação realizada com sucesso!\n")
            print("Objetivo: ", self.solver.Objective().Value())

            alunos['sol_alunos'] = alunos.apply(lambda r: r['v_aluno'].solution_value(), axis=1) == 1
            turmas['sol_turmas'] = turmas.apply(lambda r: r['v_turma'].solution_value(), axis=1) == 1
        else:
            print("Não há solução!")

        return status, alunos, turmas
