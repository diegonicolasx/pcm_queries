import polars as pl
from src.monthly.pcm_monthly import pcm_monthly

print("Bienvenido al programa para actualizar las planillas de PCM \n")

x = input("¿Desea actualizar las OT's mensuales?. S/N: ")

print("\n")

while True:

    if x.lower() in ["s", "si"]:
        pcm_monthly()
        break

    elif x.lower() in ["n", "no"]:
        break

    else:
        print("Opción no valida, por favor ingrese nuevamente \n")
        x = input("¿Desea actualizar las OT's mensuales?. S/N: ")
        print("\n")


##### Aca van los trimetrales

print("Aloha")
