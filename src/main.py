import polars as pl
from src.monthly.pcm_monthly import pcm_monthly
from src.quarterly.pcm_quarterly import pcm_quarterly
from src.corrective.pcm_correctives import pcm_correctives

print("\nBienvenido al programa de extracción de datos de Fracttal para PCM. Con este programa usted podrá actualizar las tablas de estatus de OT's, sean preventivos mensuales\
o correctivos mensuales \n")

print("\nAhora, por favor identifiquese, de acuerdo al número que aparece junto a cada nombre:\n")
print("\t1. Diego Gallegos (DYR)")
print("\t2. Lenin Heredia.")
user_number = input("\nIngrese el número correspondiente a su nombre: ")

while user_number not in ["1", "2"]:
    print("Número no válido. Por favor, intente nuevamente.")
    user_number = input("\nIngrese el número correspondiente a su nombre: ")

print("\n")

print("Seleccione el tipo de mantenimiento que desea actualizar: \n")

print("\t1. Preventivos")
print("\t2. Correctivos")

print("\n")

while True:

    x = input("Ingrese su opción: ")
    print("\n")

    if x not in ["1", "2"]:
        print("Opción no valida, por favor ingrese nuevamente \n")
        print("\t1. Preventivos")
        print("\t2. Correctivos")

    break

finish = 0

while finish < 2:

    if x == "1":

        print("Para actualizar las OT's, ingrese la acción que desea realizar: ")
        print("\n")

        print("\t1 Actualizar OT's mensuales")
        print("\t2. Actualizar OT's trimetrales")
        print("\t3. Ambas")

        print("\n")


        y = input("Ingrese su opción: ")

        if y not in ["1", "2", "3"]:
            print("Opción no valida, por favor ingrese nuevamente \n")
            print("\n")
            continue


        else:
            if y == "1":
                print("Se actualizaran las OT's mensuales \n")
                pcm_monthly(user_number)

            elif y =="2":
                print("Se actualizarán  las OT's trimestrales \n")
                pcm_quarterly(user_number)

            elif y =="3":
                print("Se actualizarán OT's mensuales y trimestrales \n")

                pcm_monthly(user_number)
                pcm_quarterly(user_number)

        finish +=1

        if finish > 1 :
            print("El programa ha finalizado") 
            break


        cont = input("¿Desea actualizar las OT's correctivas ? (S/N) : ")
        print("\n")

        while cont.upper() not in ["S", "N"]:
            print("Opción no valida, por favor ingrese nuevamente \n")
            print("\n")
            cont = input("¿Desea actualizar las OT's correctivas ? (S/N) : ")

        if cont.upper() == "N":
            print("El programa ha finalizado")
            break

        x = "2"
                

    elif x == "2" :

        print("Se actualizarán los correctivos \n")
        pcm_correctives(user_number)

        finish +=1

        if finish > 1 :
            print("El programa ha finalizado") 
            break

        cont = input("¿Desea actualizar las OT's preventivas ? (S/N) : ")
        print("\n")

        while cont.upper() not in ["S", "N"]:
            print("Opción no valida, por favor ingrese nuevamente \n")
            print("\n")
            cont = input("¿Desea actualizar las OT's preventivas ? (S/N) : ")
        
        if cont.upper() == "N":
            print("El programa ha finalizado")
            break

        x = "1"