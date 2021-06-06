from math import ceil
import pandas as pd


def get_parametros(cnx):
    # Parâmetros do modelo definidos pelo usuário
    parametros = pd.read_sql("SELECT chave, valor FROM parametro", cnx)
    parametros['valor'] = parametros['valor'].astype('int64')

    return dict(zip(parametros['chave'], parametros['valor']))


def get_alunos(cnx, info):
    # Importação de dados do SQL
    matriculados = pd.read_sql("SELECT id, turma_id, reprova, continua FROM aluno", cnx)
    formulario = pd.read_sql("SELECT id, escola_id, serie_id, data_inscricao, ano_referencia "
                             "FROM formulario_inscricao", cnx)
    serie = pd.read_sql("SELECT id, ativa FROM serie", cnx)
    turma = pd.read_sql("SELECT id, escola_id, serie_id FROM turma", cnx)

    # Filtrando alunos que continuam na ONG e mesclando com as turmas de seu perfil
    matriculados = (matriculados
                    .query('continua == 1')
                    .merge(turma, left_on='turma_id', right_on='id', suffixes=['', '_turma']))

    # Calculando nova série dos alunos (em caso de otimização para o próximo ano letivo)
    matriculados['nova_serie_id'] = matriculados.apply(lambda r: (r['serie_id'] + (1 - r['reprova']) *
                                                                  (1 - info['otimiza_dentro_do_ano'])), axis=1)

    formulario['nova_serie_id'] = formulario.apply(lambda r: (r['serie_id'] + (info['ano_planejamento'] -
                                                                               r['ano_referencia']) *
                                                              (1 - info['otimiza_dentro_do_ano'])), axis=1)

    formulario['data_inscricao'] = pd.to_datetime(formulario['data_inscricao'], dayfirst=True)

    # Informação para manter alunos de mesma turma agrupados
    matriculados.rename(columns={"turma_id": "cluster"}, inplace=True)
    formulario['cluster'] = 0

    # Ordenando prioridades do formulário por data de inscrição
    formulario = (formulario
                  .query('data_inscricao.notnull()')
                  .sort_values('data_inscricao')
                  .assign(inscricao_anterior=lambda df: df['id'].shift(1)))

    # Higienizando tabelas
    matriculados.drop(['id_turma', 'reprova', 'continua', 'serie_id'], axis=1, inplace=True)
    formulario.drop(['serie_id', 'ano_referencia'], axis=1, inplace=True)

    # Juntando base de alunos
    alunos = pd.concat([matriculados, formulario])

    # Tratando base de alunos concatenada apenas para séries ativas pela ONG
    alunos = (alunos
              .merge(serie, left_on='nova_serie_id', right_on='id', suffixes=['', '_serie'])
              .query('ativa == 1')
              .drop(['id_serie', 'ativa'], axis=1))

    return alunos


def get_turmas(cnx, alunos: pd.DataFrame, info):
    # Se a ONG optar por abrir novas turmas,
    if info['possibilita_abertura_novas_turmas']:  # TODO: adicionar % mínimo de alunos para abrir turma
        # Calcular a demanda por escola e série
        demanda = alunos.groupby(['escola_id', 'nova_serie_id']).agg({'escola_id': 'max', 'nova_serie_id': 'max',
                                                                      'id': 'count'})
        # Calcular a demanda de turmas por escola e série
        demanda['quebra'] = demanda.apply(lambda r: ceil(r['id'] / info['qtd_max_alunos']), axis=1)

        # Organizando as turmas necessárias
        turmas = pd.DataFrame([[row['escola_id'], row['nova_serie_id'], sala + 1]
                               for index, row in demanda.iterrows() for sala in range(row['quebra'])],
                              columns=['escola_id', 'serie_id', 'sala'])

        turmas['id'] = turmas.index + 1
    else:
        # Caso contrário, seguir com o atual
        turmas = pd.read_sql("SELECT id, escola_id, serie_id FROM turma", cnx)

    return turmas


def sol_aluno(cnx, alunos: pd.DataFrame):
    info_alunos = pd.read_sql("SELECT * FROM aluno", cnx)

    alunos = alunos.merge(info_alunos, how='left', on='id', suffixes=['', '_sql'])

    (alunos.query('(cluster > 0) & (sol_alunos == 1)')
     [['id', 'cpf', 'nome', 'email', 'telefone', 'nome_responsavel',
       'telefone_responsavel', 'nome_escola_origem', 'id_turma']]
     .to_sql("sol_aluno", cnx, if_exists='replace', index=False))


def sol_priorizacao_formulario(cnx, alunos: pd.DataFrame):
    info_alunos = pd.read_sql("SELECT * FROM formulario_inscricao", cnx)

    alunos = alunos.merge(info_alunos, how='left', on='id', suffixes=['', '_sql'])
    alunos['status_id'] = None

    (alunos.query('(cluster == 0) & (sol_alunos == 1)')
     [['id', 'nome', 'cpf', 'email_aluno', 'telefone_aluno', 'nome_responsavel', 'telefone_responsavel',
       'escola_id', 'serie_id', 'nome_escola_origem', 'id_turma', 'status_id']]
     .to_sql("sol_priorizacao_formulario", cnx, if_exists='replace', index=False))


def sol_turma(cnx, turmas: pd.DataFrame, info):
    turmas['qtd_max_alunos'] = info['qtd_max_alunos']
    turmas['qtd_professores_acd'] = info['qtd_professores_acd']
    turmas['qtd_professores_pedagogico'] = info['qtd_professores_pedagogico']
    turmas['aprova'] = None
    turmas['nome'] = None

    (turmas.query('sol_turmas == 1')
     [['id', 'nome', 'qtd_max_alunos', 'qtd_professores_acd',
       'qtd_professores_pedagogico', 'escola_id', 'serie_id', 'aprova']]
     .to_sql("sol_turma", cnx, if_exists='replace', index=False))


def truncate_tables(cnx):
    cnx.execute("DELETE FROM sol_aluno")
    cnx.execute("DELETE FROM sol_priorizacao_formulario")
    cnx.execute("DELETE FROM sol_turma")

    cnx.commit()
