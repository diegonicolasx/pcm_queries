import polars as pl
from src.monthly.extract_data_fracttal import extract_wo_api
from src.utils.utils import validate_year, validate_month, init_date, end_date
from src.corrective.transformed_corrective import transform_correctives
from src.corrective.print_corrective_sheet import print_corrective_sheet

def pcm_correctives(user_number : str) -> None:


    print("-------------------------------------------------------------------------------------------------------------------------- \n")
    print("Este modulo extrae datos de mantenimientos CORRECTIVOS desde fracttal, para luego ordenarlos y crear una hoja en google sheet con el resumen del estado de estos\
          mantenimientos \n")
    
    print("-------------------------------------------------------------------------------------------------------------------------- \n")
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
    
    df_wo = extract_wo_api(since_date= since_date, until_date= until_date)

    df_corrective = transform_correctives(df_wo, user_number)

    print_corrective_sheet(df_corrective, year, month, False)

    



if __name__ == "__main__":

    pcm_correctives(user_number="1")