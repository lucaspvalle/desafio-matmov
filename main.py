import integracao
import sqlite3
from otimizacao import *


def main():
    cnx = sqlite3.connect("data.db")

    # Importação de dados
    info = integracao.get_parametros(cnx)
    alunos = integracao.get_alunos(cnx, info)
    turmas = integracao.get_turmas(cnx, alunos, info)

    # 1) Variáveis de decisão
    turmas = v_turmas(turmas)
    alunos = v_alunos(alunos, turmas)

    # 2) Restrições
    c_alunos_matriculados(alunos)
    c_alunos_de_formulario(alunos)
    c_agrupa_colegas(alunos)
    c_abertura_de_turmas(alunos)
    c_maximo_de_alunos_por_turma(info, alunos)
    c_prioriza_ordem_de_inscricao(alunos)
    c_custos(info, alunos, turmas)

    # 3) Função Objetivo
    funcao_objetivo(info, alunos)

    # 4) Execução
    factivel, alunos, turmas = otimiza(alunos, turmas)

    if factivel:
        integracao.sol_aluno(cnx, alunos)
        integracao.sol_priorizacao_formulario(cnx, alunos)
        integracao.sol_turma(cnx, turmas, info)
    else:
        integracao.truncate_tables(cnx)

    cnx.close()


if __name__ == "__main__":
    main()
