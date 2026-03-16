import polars as pl
from datetime import datetime
from src.monthly.extract_data_fracttal import extract_wo_api
from src.quarterly.transform_quarterly_calculated_data import transform_quarterly_calculated_data
from src.quarterly.quarterly_sheet import print_quarterly_sheet
from src.utils.utils import validate_year, init_date, end_date


def pcm_quarterly():

    print("\nPara extraer los datos, se le pedirá ingresar el año y el número del trimestre.")

    

    print("El programa extraerá datos a partir del año y mes ingresado tomando como referencia la fecha programada para las OT's.\n")


    while True:
        year = input("Ingrese al año (en formato AAAA): ")    
        if validate_year(year):
            print("\n")
            break
        else:
            print("Por favor, intente nuevamente.\n")

    while True:

        print("Escoja el trimestre. Q1: Enero-Marzo, Q2: Abril-Junio, Q3: Julio-Septiembre, Q4: Octubre-Diciembre :")

        quarter = input("Ingrese el numero de trimestre: ")
        
        if 1 <= int(quarter) <= 4:
            print("n")
            break
        else:
            print("Trimestre no válido \n")

    # Meses del trimestre
    first_month = 3*int(quarter) - 2
    last_month = 3*int(quarter)

    since_date = init_date(year, first_month, utc= True)
    until_date = end_date(init_date(year,last_month,utc=True))

    ### Extraer datos de fracttal

    df_wo = extract_wo_api(since_date, until_date)

    df_transformated = transform_quarterly_calculated_data(df_wo,"1").sort(by="Portafolio")

    print_quarterly_sheet(df_transformated, year, quarter, False)




if __name__ == "__main__":
    pcm_quarterly()

    

    