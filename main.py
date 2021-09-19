from libs import integracao, otimizacao


def main():

    # Objetos de interação com o sistema
    integra = integracao.Integrador("data.db")
    resolvedor = otimizacao.Otimizador("CBC", integra.info)

    # Importação de dados
    alunos = integra.get_alunos()
    turmas = integra.get_turmas()

    # Execução
    factivel, alunos, turmas = resolvedor.otimiza(alunos, turmas)

    # Exportação de dados
    if factivel:
        integra.get_relatorio_final(alunos, turmas)


if __name__ == "__main__":
    main()
