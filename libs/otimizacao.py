from libs.integracao import Integrador
from libs.tempo import cronometro

from ortools.linear_solver import pywraplp
import pandas as pd


class Otimizador(Integrador):

    @cronometro
    def __init__(self, nome_do_solver: str, nome_do_banco: str):
        self.solver = pywraplp.Solver.CreateSolver(nome_do_solver)
        super().__init__(nome_do_banco)

    @cronometro
    def v_alunos(self):
        """
        Combina alunos com possíveis turmas

        :return: variável de decisão binária de alunos x turmas
        """

        self.matriculados = self.matriculados.merge(self.turmas[['escola_id', 'serie_id', 'turma_id', 'v_turma']],
                                                    on=['escola_id', 'serie_id'])
        self.matriculados["v_aluno"] = self.matriculados.apply(lambda r: self.solver.BoolVar(f"{r.cod}_{r.turma_id}"),
                                                               axis=1)

        self.formulario = self.formulario.merge(self.turmas[['escola_id', 'serie_id', 'turma_id', 'v_turma']],
                                                on=['escola_id', 'serie_id'])
        self.formulario["v_aluno"] = self.formulario.apply(lambda r: self.solver.BoolVar(f"{r.cod}_{r.turma_id}"),
                                                           axis=1)

    @cronometro
    def v_turmas(self):
        """
        Define a abertura de turmas.

        :return: variável de decisão binária para abertura de turmas
        """

        self.turmas["v_turma"] = self.turmas.apply(lambda r: self.solver.BoolVar(f"turma_{r.turma_id}"), axis=1)

    @cronometro
    def c_alunos_matriculados(self):
        """
        Alunos já matriculados na ONG, que desejam continuar, devem ser alocados em uma turma

        SUM(TURMA, v_alunos(ALUNO, TURMA) | (ALUNO in MATRICULADO)) = 1
        """

        (self.matriculados
         .groupby('id')
         .apply(lambda r: self.solver.Add(r['v_aluno'].values.sum() == 1, f"continua_{r.iloc[0].cod}")))

    @cronometro
    def c_alunos_de_formulario(self):
        """
        Alunos inscritos no formulário podem ser alocados em uma (e somente uma) turma.

        SUM(TURMA, v_alunos(ALUNO, TURMA) | (ALUNO in FORMULARIO)) <= 1
        """

        (self.formulario
         .groupby('id')
         .apply(lambda r: self.solver.Add(r['v_aluno'].values.sum() <= 1, f"inscricao_{r.iloc[0].cod}")))

    @cronometro
    def c_agrupa_colegas(self):
        """
        Alunos já matriculados na ONG, que estudavam em uma mesma turma, devem continuar juntos

        v_alunos(ALUNO, TURMA) = v_alunos(COLEGA, TURMA)
        """

        colunas = ['cod', 'cluster', 'turma_id', 'v_turma', 'v_aluno']

        (self.matriculados[colunas]
         .merge(self.matriculados[colunas], on=['cluster', 'turma_id'], suffixes=['', '_colega'])
         .query('cod != cod_colega')
         .apply(lambda r: self.solver.Add(r['v_aluno'] == r['v_aluno_colega'],
                                          f"mantem_junto_{r.cod}_{r.cod_colega}_{r.turma_id}"), axis=1))

    @cronometro
    def c_maximo_de_alunos_por_turma(self):
        """
        Define a quantidade máxima de alunos por turma, se aberta.

        SUM(ALUNO, v_alunos(ALUNO, TURMA)) <= qtd_max_alunos(TURMA) * v_turma(TURMA)
        """

        colunas = ['v_aluno', 'v_turma', 'turma_id']
        alunos = pd.concat([self.matriculados[colunas], self.formulario[colunas]])

        (alunos
         .groupby('turma_id')
         .apply(lambda r: self.solver.Add(r['v_aluno'].values.sum() <= self.info['qtd_max_alunos'] *
                                          r.iloc[0]['v_turma'], f"max_alunos_{r.iloc[0].turma_id}")))

    @cronometro
    def c_custos(self):
        """
        Modelo de custos da ONG.

        SUM((ALUNO, TURMA), v_alunos(ALUNO, TURMA)) * custo(ALUNO) + SUM(TURMA, v_turmas(TURMA)) * custo(TURMA)
        <=
        limite_custo
        """

        qtd_professores_por_turma = (self.info['qtd_professores_pedagogico'] + self.info['qtd_professores_acd'])

        custo_aluno = self.info['custo_aluno']
        custo_professor = qtd_professores_por_turma * self.info['custo_professor']

        self.solver.Add(
            (self.matriculados['v_aluno'].values.sum() + self.formulario['v_aluno'].values.sum()) * custo_aluno +
            (self.turmas['v_turma'].values.sum() * custo_professor) <= self.info['limite_custo'], "limite_custos")

    @cronometro
    def funcao_objetivo(self):
        """
        Função Objetivo: maximizar a quantidade de alunos assistidos com a maior ocupação possível de turmas,
        priorizando a ordem de inscrição dos alunos de formulário e turmas de anos mais novos

        max SUM((ALUNO, TURMA), v_alunos(ALUNO, TURMA) * peso_inscricao(ALUNO))
            - SUM(TURMA, penaliza_vagas_remanescentes(TURMA))
        """

        self.matriculados['peso_inscricao'] = 1
        colunas = ['v_aluno', 'v_turma', 'serie_id', 'turma_id', 'peso_inscricao']

        alunos = pd.concat([self.matriculados[colunas], self.formulario[colunas]])

        # Prioriza turmas de anos mais novos
        max_serie_id = self.turmas['serie_id'].values.max() + 1
        alunos['peso_serie'] = (max_serie_id - alunos['serie_id'].values) / (max_serie_id * 5)

        # Penaliza as vagas remanescentes quando uma turma é aberta
        max_alunos = self.info['qtd_max_alunos']
        penalizacao = (alunos
                       .groupby('turma_id')
                       .apply(lambda df: max_alunos * df.iloc[0]['v_turma'] - df['v_aluno'].values.sum()))

        # Função Objetivo
        self.solver.Maximize((alunos['v_aluno'] * alunos['peso_inscricao'] * alunos['peso_serie']).values.sum()
                             - 0.01 * penalizacao.values.sum())

    @cronometro
    def otimiza(self):
        """
        Executa o modelo de otimização.
        """

        # 1) Variáveis de decisão
        self.v_turmas()
        self.v_alunos()

        # 2) Restrições
        self.c_alunos_matriculados()
        self.c_alunos_de_formulario()
        self.c_agrupa_colegas()
        self.c_maximo_de_alunos_por_turma()
        self.c_custos()

        # 3) Função Objetivo
        self.funcao_objetivo()

        status = (self.solver.Solve() == pywraplp.Solver.OPTIMAL or pywraplp.Solver.FEASIBLE)

        if status:
            print("\nAlocação realizada com sucesso!")
            print(f"Objetivo: {self.solver.Objective().Value():.4f}\n")

            for df in [self.matriculados, self.formulario]:
                df['sol_alunos'] = df.apply(lambda r: r['v_aluno'].solution_value(), axis=1) == 1

            self.turmas['sol_turmas'] = self.turmas.apply(lambda r: r['v_turma'].solution_value(), axis=1) == 1

            self.get_resultados()
        else:
            print("Não há solução!")
