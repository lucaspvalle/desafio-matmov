from numpy import ceil
import pandas as pd
import sqlite3


def get_parametros(cnx: sqlite3.Connection) -> dict:
    """
    Parâmetros definidos pelo usuário.

    :param cnx: conexão com o banco de dados
    :return: parâmetros
    """

    parametros = pd.read_sql("SELECT chave, valor FROM parametro", cnx)
    return dict(zip(parametros['chave'], parametros['valor'].astype(int)))


def get_alunos(cnx: sqlite3.Connection, info: dict) -> pd.DataFrame:
    """
    Importa tabela de alunos do banco de dados.

    :param cnx: conexão com o banco de dados
    :param info: decisões globais do modelo de otimização
    :return: alunos cadastrados no sistema
    """

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
    matriculados['nova_serie_id'] = (matriculados['serie_id'].values + (1 - matriculados['reprova'].values) *
                                     (1 - info['otimiza_dentro_do_ano']))

    formulario['nova_serie_id'] = (formulario['serie_id'].values +
                                   (info['ano_planejamento'] - formulario['ano_referencia'].values) *
                                   (1 - info['otimiza_dentro_do_ano']))

    formulario['data_inscricao'] = pd.to_datetime(formulario['data_inscricao'].values, dayfirst=True)

    # Informação para manter alunos de mesma turma agrupados
    matriculados.rename(columns={"turma_id": "cluster"}, inplace=True)
    formulario['cluster'] = 0  # cluster 0, portanto, identifica alunos de formulário

    # Ordenando prioridades do formulário por data de inscrição
    formulario = (formulario
                  .query('cluster == 0')
                  .sort_values('data_inscricao')
                  .assign(inscricao_anterior=lambda df: df['id'].shift(1))
                  .fillna(0).astype({'inscricao_anterior': int}))

    # Higienizando tabelas
    matriculados.drop(['id_turma', 'reprova', 'continua', 'serie_id'], axis=1, inplace=True)
    formulario.drop(['serie_id', 'ano_referencia', 'data_inscricao'], axis=1, inplace=True)

    # Juntando base de alunos
    alunos = pd.concat([matriculados, formulario])

    # Tratando base de alunos concatenada apenas para séries ativas pela ONG
    alunos = (alunos
              .merge(serie, left_on='nova_serie_id', right_on='id', suffixes=['', '_serie'])
              .query('ativa == 1')
              .drop(['id_serie', 'ativa'], axis=1))

    return alunos


def get_turmas(cnx: sqlite3.Connection, alunos: pd.DataFrame, info: dict) -> pd.DataFrame:
    """
    Importa tabela de turmas do banco de dados.

    :param cnx: conexão com o banco de dados
    :param alunos: alunos cadastrados no sistema
    :param info: decisões globais do modelo de otimização
    :return: oferta vigente de turmas
    """

    # Se a ONG optar por abrir novas turmas,
    if info['possibilita_abertura_novas_turmas']:

        # Dicionário para auxiliar a nomenclatura de turmas
        aux = {1: "A", 2: "B", 3: "C", 4: "D"}

        # Informações auxiliares para definir a nomenclatura de turmas
        regiao = pd.read_sql("SELECT * FROM regiao", cnx)
        serie = pd.read_sql("SELECT id, nome FROM serie", cnx)
        escola = pd.read_sql("SELECT * FROM escola", cnx)

        # Calcular a demanda por escola e série
        demanda = (alunos
                   .groupby(['escola_id', 'nova_serie_id'])
                   .agg({'id': 'count'})
                   .query(f'id >= {info["min_aluno_por_turma"]}'))

        # Calcular a demanda de turmas por escola e série
        demanda['sala'] = (demanda
                           .assign(quebra=ceil(demanda['id'].values / info['qtd_max_alunos']))
                           .astype({'quebra': int})
                           .apply(lambda r: list(range(1, r['quebra'] + 1)), axis=1))

        # Abrindo a quantidade necessária de salas para atender a demanda
        demanda = demanda.explode('sala').reset_index()

        # Nomeando novas turmas
        demanda['nome'] = (demanda
                           .merge(escola, left_on='escola_id', right_on='id', suffixes=['', '_escola'])
                           .merge(serie, left_on='nova_serie_id', right_on='id', suffixes=['', '_serie'])
                           .merge(regiao, left_on='regiao_id', right_on='id', suffixes=['', '_regiao'])
                           .astype({'sala': int})
                           .apply(lambda r: r['nome_regiao'] + "_" + r['nome_serie'][0] + aux[r['sala']], axis=1))

        # Padronizando a tabela final
        turmas = demanda.drop(columns='sala').rename(columns={'nova_serie_id': 'serie_id'}).assign(id=demanda.index + 1)

    # Caso contrário, seguir com o atual
    else:
        turmas = pd.read_sql("SELECT id, escola_id, serie_id FROM turma", cnx)

    return turmas


def sol_aluno(cnx: sqlite3.Connection, alunos: pd.DataFrame):
    """
    Exporta os resultados de alunos alocados do modelo, que já participavam de turmas da ONG.

    :param cnx: conexão com o banco de dados
    :param alunos: alunos cadastrados no sistema
    """

    info_alunos = pd.read_sql("SELECT * FROM aluno", cnx)

    (info_alunos
     .merge(alunos.query('(cluster > 0) & (sol_alunos == 1)')[['id', 'id_turma']], on='id', how='inner')
     .drop(['turma_id', 'reprova', 'continua'], axis=1)
     .to_sql("sol_aluno", cnx, if_exists='replace', index=False))


def sol_priorizacao_formulario(cnx: sqlite3.Connection, alunos: pd.DataFrame):
    """
    Exporta os resultados de alunos alocados do modelo, que estavam na lista de espera da ONG.

    :param cnx: conexão com o banco de dados
    :param alunos: alunos cadastrados no sistema
    """

    info_alunos = pd.read_sql("SELECT * FROM formulario_inscricao", cnx)
    info_alunos['data_inscricao'] = pd.to_datetime(info_alunos['data_inscricao'], dayfirst=True)

    (info_alunos
     .merge(alunos.query('(cluster == 0) & (sol_alunos == 1)')[['id', 'id_turma', 'serie_id']], on='id',
            suffixes=['_antigo', ''], how='inner')
     .assign(status_id=None)
     .sort_values('data_inscricao')
     .drop(['data_inscricao', 'ano_referencia', 'serie_id_antigo'], axis=1)
     .to_sql("sol_priorizacao_formulario", cnx, if_exists='replace', index=False))


def sol_turma(cnx: sqlite3.Connection, turmas: pd.DataFrame, info: dict):
    """
    Exporta os resultados de turmas abertas do modelo.

    :param cnx: conexão com o banco de dados
    :param turmas: oferta vigente de turmas
    :param info: decisões globais do modelo.
    """

    (turmas
     .query('sol_turmas == 1')
     .assign(qtd_max_alunos=info['qtd_max_alunos'],
             qtd_professores_acd=info['qtd_professores_acd'],
             qtd_professores_pedagogico=info['qtd_professores_pedagogico'],
             aprova=None)
     .drop(['v_turma', 'sol_turmas'], axis=1)
     .rename(columns={'id': 'id_turma'})
     .to_sql("sol_turma", cnx, if_exists='replace', index=False))


def truncate_tables(cnx: sqlite3.Connection):
    """
    Limpa as tabelas do banco de dados.

    :param cnx: conexão com o banco de dados
    """

    print("Não há solução!")

    cnx.execute("DELETE FROM sol_aluno")
    cnx.execute("DELETE FROM sol_priorizacao_formulario")
    cnx.execute("DELETE FROM sol_turma")

    cnx.commit()
