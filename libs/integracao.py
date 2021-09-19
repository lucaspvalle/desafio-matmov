import pandas as pd
import sqlite3


class Integrador:

    def __init__(self, quem: str):
        self.cnx = sqlite3.connect(quem)
        self.info = self.get_parametros()

        # Limpa a escrita de execuções anteriores
        for view in ['base_alunos', 'indicadores']:
            self.cnx.execute(f"DROP VIEW IF EXISTS {view}")

        for tabela in ['sol_aluno', 'sol_priorizacao_formulario', 'sol_turma']:
            self.cnx.execute(f"DELETE FROM {tabela}")

        self.cnx.commit()

    def __del__(self):
        self.cnx.close()

    def get_alunos(self) -> pd.DataFrame:
        """
        Importa tabela de alunos do banco de dados.

        :return: alunos cadastrados no sistema
        """

        with open('sql/alunos.sql') as file:
            query = file.read().format(ano_planejamento=self.info['ano_planejamento'],
                                       otimiza_dentro_do_ano=self.info['otimiza_dentro_do_ano'])
            self.cnx.execute(query)
            self.cnx.commit()

        alunos = pd.read_sql("SELECT * FROM base_alunos", self.cnx)

        # Convertendo coluna em booleana
        alunos['formulario'] = alunos['formulario'] == 1

        # Priorizando a ordem de inscrição do formulário
        alunos['data_inscricao'] = (pd.to_datetime(alunos['data_inscricao'].values, dayfirst=True))

        tamanho_formulario = len(alunos.query('formulario'))

        alunos.loc[alunos['formulario'], 'peso_inscricao'] = \
            (alunos['data_inscricao'].rank(method='dense', ascending=False) / tamanho_formulario)

        return alunos

    def get_parametros(self) -> dict:
        """
        Parâmetros definidos pelo usuário.

        :return: parâmetros
        """

        parametros = pd.read_sql("SELECT chave, valor FROM parametro", self.cnx)
        return dict(zip(parametros['chave'], parametros['valor'].astype(int)))

    def get_turmas(self) -> pd.DataFrame:
        """
        Importa tabela de turmas do banco de dados.

        :return: oferta vigente de turmas
        """

        with open('sql/turmas.sql') as file:
            query = file.read().format(qtd_max_alunos=self.info['qtd_max_alunos'],
                                       qtd_professores_acd=self.info['qtd_professores_acd'],
                                       qtd_professores_pedagogico=self.info['qtd_professores_pedagogico'],
                                       possibilita_abertura_novas_turmas=self.info['possibilita_abertura_novas_turmas'])

        turmas = pd.read_sql(query, self.cnx)

        # Dicionário para auxiliar a nomenclatura de turmas
        aux = {1: "A", 2: "B", 3: "C", 4: "D"}

        turmas['salas'] = turmas.apply(lambda r: list(range(1, r['salas'] + 1)), axis=1)
        turmas = turmas.explode('salas', ignore_index=True)

        turmas['nome'] = turmas['nome'] + turmas['salas'].map(aux)
        turmas['turma_id'] = turmas.index + 1

        turmas = turmas.drop(['salas'], axis=1)

        return turmas

    def sol_aluno(self, alunos: pd.DataFrame):
        """
        Exporta os resultados de alunos alocados do modelo, que já participavam de turmas da ONG.

        :param alunos: alunos cadastrados no sistema
        """
        colunas = ['id', 'cpf', 'nome', 'email_aluno', 'telefone_aluno', 'nome_responsavel', 'telefone_responsavel',
                   'nome_escola_origem', 'turma_id']

        alunos.query('(~ formulario) & (sol_alunos)')[colunas].to_sql("sol_aluno", self.cnx, if_exists='replace',
                                                                      index=False)

    def sol_priorizacao_formulario(self, alunos: pd.DataFrame):
        """
        Exporta os resultados de alunos alocados do modelo, que estavam na lista de espera da ONG.

        :param alunos: alunos cadastrados no sistema
        """

        colunas = ['id', 'cpf', 'nome', 'email_aluno', 'telefone_aluno', 'nome_responsavel', 'telefone_responsavel',
                   'nome_escola_origem', 'turma_id', 'status_id']

        (alunos.query('(formulario) & (sol_alunos)').assign(status_id=None)[colunas]
         .to_sql("sol_priorizacao_formulario", self.cnx, if_exists='replace', index=False))

    def sol_turma(self, alunos: pd.DataFrame, turmas: pd.DataFrame):
        """
        Exporta os resultados de turmas abertas do modelo.

        :param alunos: alunos cadastrados no sistema
        :param turmas: oferta vigente de turmas
        """

        colunas = ['turma_id', 'nome', 'escola_id', 'serie_id', 'qtd_alunos', 'qtd_max_alunos', 'qtd_professores_acd',
                   'qtd_professores_pedagogico', 'aprova']

        alunos_por_turma = (alunos.query('sol_alunos').groupby('turma_id')['cpf'].count().reset_index()
                            .rename(columns={'cpf': 'qtd_alunos'}))

        (turmas.query('sol_turmas').assign(aprova=None).merge(alunos_por_turma)[colunas]
         .to_sql("sol_turma", self.cnx, if_exists='replace', index=False))

    def get_kpis(self):
        """
        Exporta os indicadores da solução obtida.
        """
        custo_professor_por_turma = ((self.info['qtd_professores_acd'] + self.info['qtd_professores_pedagogico']) *
                                     self.info['custo_professor'])

        with open('sql/indicadores.sql') as file:
            query = file.read().format(qtd_max_alunos=self.info['qtd_max_alunos'],
                                       custo_aluno=self.info['custo_aluno'],
                                       custo_professor_por_turma=custo_professor_por_turma)

        self.cnx.execute(query)
        self.cnx.commit()

    def get_relatorio_final(self, alunos, turmas):
        """
        Consolida os resultados da otimização no sistema

        :param alunos: alunos cadastrados no sistema
        :param turmas: oferta vigente de turmas
        """

        self.sol_aluno(alunos)
        self.sol_priorizacao_formulario(alunos)
        self.sol_turma(alunos, turmas)

        self.get_kpis()
