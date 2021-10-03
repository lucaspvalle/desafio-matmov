import pandas as pd
import sqlite3


class Integrador:

    def __init__(self, quem: str):
        self.cnx = sqlite3.connect(quem)
        self.info = self.get_parametros()

        # Limpa a escrita de execuções anteriores
        for view in ['base_matriculados', 'base_formulario', 'indicadores']:
            self.cnx.execute(f"DROP VIEW IF EXISTS {view}")

        for tabela in ['sol_aluno', 'sol_priorizacao_formulario', 'sol_turma']:
            self.cnx.execute(f"DELETE FROM {tabela}")

        # Principais tabelas do modelo
        self.matriculados, self.formulario = self.get_alunos()
        self.turmas = self.get_turmas()

        self.cnx.commit()

    def __del__(self):
        self.cnx.close()

    def get_alunos(self) -> (pd.DataFrame, pd.DataFrame):
        """
        Importa tabela de alunos do banco de dados.

        :return: alunos cadastrados no sistema
        """

        with open('sql/matriculados.sql') as file:
            query = file.read().format(ano_planejamento=self.info['ano_planejamento'],
                                       otimiza_dentro_do_ano=self.info['otimiza_dentro_do_ano'])
            self.cnx.execute(query)
            self.cnx.commit()

            matriculados = pd.read_sql("SELECT * FROM base_matriculados", self.cnx)

        with open('sql/formulario.sql') as file:
            query = file.read().format(ano_planejamento=self.info['ano_planejamento'],
                                       otimiza_dentro_do_ano=self.info['otimiza_dentro_do_ano'])
            self.cnx.execute(query)
            self.cnx.commit()

            formulario = pd.read_sql("SELECT * FROM base_formulario", self.cnx)

        # Priorizando a ordem de inscrição do formulário
        formulario['data_inscricao'] = (pd.to_datetime(formulario['data_inscricao'].values, dayfirst=True))

        formulario['peso_inscricao'] =\
            (formulario['data_inscricao'].rank(method='dense', ascending=False) / len(formulario))

        return matriculados, formulario

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

    def sol_aluno(self):
        """
        Exporta os resultados de alunos alocados do modelo, que já participavam de turmas da ONG.
        """
        colunas = ['id', 'cpf', 'nome', 'email_aluno', 'telefone_aluno', 'nome_responsavel', 'telefone_responsavel',
                   'nome_escola_origem', 'turma_id']

        self.matriculados.query('sol_alunos')[colunas].to_sql("sol_aluno", self.cnx, if_exists='replace', index=False)

    def sol_priorizacao_formulario(self):
        """
        Exporta os resultados de alunos alocados do modelo, que estavam na lista de espera da ONG.
        """

        colunas = ['id', 'cpf', 'nome', 'email_aluno', 'telefone_aluno', 'nome_responsavel', 'telefone_responsavel',
                   'nome_escola_origem', 'turma_id', 'status_id']

        (self.formulario.query('sol_alunos').assign(status_id=None)[colunas]
         .to_sql("sol_priorizacao_formulario", self.cnx, if_exists='replace', index=False))

    def sol_turma(self):
        """
        Exporta os resultados de turmas abertas do modelo.
        """

        colunas = ['turma_id', 'nome', 'escola_id', 'serie_id', 'qtd_alunos', 'qtd_max_alunos', 'qtd_professores_acd',
                   'qtd_professores_pedagogico', 'aprova']

        alunos = pd.concat([self.matriculados.query('sol_alunos')[['cpf', 'turma_id']],
                           self.formulario.query('sol_alunos')[['cpf', 'turma_id']]])

        alunos_x_turma = alunos.groupby('turma_id')['cpf'].count().reset_index().rename(columns={'cpf': 'qtd_alunos'})

        (self.turmas.query('sol_turmas').assign(aprova=None).merge(alunos_x_turma)[colunas]
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

    def get_resultados(self):
        """
        Consolida os resultados da otimização no sistema
        """

        self.sol_aluno()
        self.sol_priorizacao_formulario()
        self.sol_turma()

        self.get_kpis()
