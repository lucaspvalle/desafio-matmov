import time
import logging

logging.basicConfig(format="[%(asctime)s] %(message)s", datefmt="%d-%m-%y %H:%M:%S", level=logging.INFO)


def cronometro(funcao):
    def wrapper(*args, **kwargs):
        inicio = time.time()
        funcao(*args, **kwargs)
        fim = time.time()

        logging.info(f"'{funcao.__name__}' ({(fim - inicio):.4f} segundos)")

    return wrapper
