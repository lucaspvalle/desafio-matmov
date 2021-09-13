import sqlite3
from libs import integracao, otimizacao


def main():
    cnx = sqlite3.connect("data.db")

    # Importação de dados
    info = integracao.get_parametros(cnx)
    alunos = integracao.get_alunos(cnx, info)
    turmas = integracao.get_turmas(cnx, alunos, info)

    # 1) Variáveis de decisão
    turmas = otimizacao.v_turmas(turmas)
    alunos = otimizacao.v_alunos(alunos, turmas)

    # 2) Restrições
    otimizacao.c_alunos_matriculados(alunos)
    otimizacao.c_alunos_de_formulario(alunos)
    otimizacao.c_agrupa_colegas(alunos)
    otimizacao.c_maximo_de_alunos_por_turma(info, alunos)
    otimizacao.c_custos(info, alunos, turmas)

    # 3) Função Objetivo
    otimizacao.funcao_objetivo(info, alunos, turmas)

    # 4) Execução
    factivel, alunos, turmas = otimizacao.otimiza(alunos, turmas)

    if factivel:
        integracao.sol_aluno(cnx, alunos)
        integracao.sol_priorizacao_formulario(cnx, alunos)
        integracao.sol_turma(cnx, turmas, info)

        integracao.get_kpis(cnx, alunos, turmas, info)
    else:
        integracao.truncate_tables(cnx)

    cnx.close()


if __name__ == "__main__":
    main()
