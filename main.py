from libs.otimizacao import Otimizador
from libs.tempo import cronometro


@cronometro
def main():

    print("Iniciando a execução!")
    Otimizador(solver="CBC", banco="data.db").otimiza()


if __name__ == "__main__":
    main()
