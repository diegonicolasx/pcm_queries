from .utils import validate_year, validate_month, init_date, end_date
from .extract_data_fracttal import extract_wo_api
from .transform_monthly_calculated_data import transform_monthly_calculated_data
from .get_kpi import get_kpi
from .print_google_sheet import print_google_sheet

print("\nBienvenido al programa de extracción de datos de Fracttal para PMC.")

print("\nPara extraer los datos, se le pedirá ingresar el año y el mes deseado.")

print("El programa extraerá datos a partir del año y mes ingresado tomando como referencia la fecha programada para las OT's.\n")


while True:
    year = input("Ingrese al año (en formato AAAA): ")    
    if validate_year(year):
        break
    else:
        print("Por favor, intente nuevamente.\n")        

while True:
    month = input("Ingrese el mes (en número): ")    
    if validate_month(month):
        break
    else:
        print("Por favor, intente nuevamente.\n")

    
since_date = init_date(year, month, utc=True)

until_date = end_date(since_date)
        
print(f"\nLa fecha de extracción es: {since_date} hasta {until_date}\n")

print("\nAhora, por favor identifiquese, de acuerdo al número que aparece junto a cada nombre:\n")
print("\t1. Diego Gallegos (DYR)")
print("\t2. Fernaando Vera.")
print("\t3. Vittorio Neira.")
user_number = int(input("\nIngrese el número correspondiente a su nombre: "))

while user_number not in [1, 2, 3]:
    print("Número no válido. Por favor, intente nuevamente.")
    user_number = int(input("\nIngrese el número correspondiente a su nombre: "))

print("\nSe comenzará la extracción de datos, por favor espere...\n")

df_wo = extract_wo_api(since_date, until_date)

print("\nSe han extraído los datos, ahora se comenzará con la transformación de los mismos, por favor espere...\n")

df_final = transform_monthly_calculated_data(df_wo, user_number=user_number)

print(f"\nSe han transformado los datos, ahora se mostrarán los KPIs calculados:\n{df_final}")

print("\nAhora se comenzará con el cálculo de los KPIs, por favor espere...\n")

kpi_dict = get_kpi(df_wo)

print(f"\nSe han calculado los KPIs, ahora se mostrarán los mismos:\n{kpi_dict}")

print("\nFinalmente, se imprimirán los datos en la hoja de Google Sheets, por favor espere...\n")

print_google_sheet(df_final, kpi_dict, year, month, test=True)

print("\nSe han impreso los datos en la hoja de Google Sheets, el proceso ha finalizado exitosamente.")