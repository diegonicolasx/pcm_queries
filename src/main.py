import polars as pl
from src.monthly.pcm_monthly import pcm_monthly
from src.quarterly.pcm_quarterly import pcm_quarterly

print("\nBienvenido al programa de extracción de datos de Fracttal para PCM. \n")

print("\nAhora, por favor identifiquese, de acuerdo al número que aparece junto a cada nombre:\n")
print("\t1. Diego Gallegos (DYR)")
print("\t2. Fernaando Vera.")
print("\t3. Vittorio Neira.")
user_number = input("\nIngrese el número correspondiente a su nombre: ")

while user_number not in ["1", "2", "3"]:
    print("Número no válido. Por favor, intente nuevamente.")
    user_number = input("\nIngrese el número correspondiente a su nombre: ")

print("\n")

while True:

    print("Para actualizar las OT's, ingrese la acción que desea realizar: ")
    print("\n")

    print("\t1 Actualizar OT's mensuales")
    print("\t2. Actualizar OT's trimetrales")
    print("\t3. Ambas")

    print("\n")


    x = input("Ingrese su opción: ")

    if x not in ["1", "2", "3"]:
        print("Opción no valida, por favor ingrese nuevamente \n")
        print("\n")
        continue


    else:
        if x == "1":
            print("Se actualizaran las OT's mensuales \n")
            pcm_monthly(user_number)
            break

        elif x =="2":
            print("Se actualizarán  las OT's trimestrales \n")
            pcm_quarterly(user_number)
            break

        elif x =="3":
            print("Se actualizarán OT's mensuales y trimestrales \n")

            pcm_monthly(user_number)
            pcm_quarterly(user_number)

            break