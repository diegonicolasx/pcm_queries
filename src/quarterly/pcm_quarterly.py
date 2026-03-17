import polars as pl
import os
from datetime import datetime
from src.quarterly.kpi_get import get_kpi
from src.monthly.extract_data_fracttal import extract_wo_api
from src.quarterly.transform_quarterly_calculated_data import transform_quarterly_calculated_data
from src.quarterly.quarterly_sheet import print_quarterly_sheet
from src.utils.utils import validate_year, init_date, end_date
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()


def pcm_quarterly(user_number:str):

    if user_number == "1":
        results = Path(os.getenv("DYR_PATH"))
    elif user_number == "2":
        results = Path(os.getenv("FERNANDO_VERA_PATH"))
    elif user_number == "3":
        results = Path(os.getenv("VITTORIO_PATH"))    

    parks = pl.read_parquet(results / "plant_db.parquet").filter(pl.col("OM_status")=="Active").select(pl.col("portfolio"), pl.col("rcc_name"))
    parks = parks.rename({"portfolio":"Portafolio", "rcc_name": "Parque"}).sort("Parque")

    print("---------------------------------------------------------------------------------------------------------------------------")

    print("Se extraerán los datos trimestrales para PCM")

    print("\nPara extraer los datos, se le pedirá ingresar el año y el número del trimestre.\n")

    

    print("El programa extraerá datos a partir del año y mes ingresado tomando como referencia la fecha programada para las OT's.\n")


    while True:
        year = input("Ingrese al año (en formato AAAA): ")    
        if validate_year(year):
            print("\n")
            break
        else:
            print("Por favor, intente nuevamente.\n")

    while True:

        print("\n")

        print("Escoja el trimestre : \n")

        print("\t1. Enero - Marzo")
        print("\t2. Abril - Junio")
        print("\t3. Julio - Septiembre")
        print("\t4. Octubre - Diciembre")

        print("\n")

        quarter = input("Ingrese el numero de trimestre: ")
        
        if 1 <= int(quarter) <= 4:
            print("\n")
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

    df_transformated = transform_quarterly_calculated_data(df_wo,user_number)

    ot_resumen = get_kpi(df_transformated)

    df_transformated = parks.join(df_transformated, on =["Portafolio", "Parque"], how="left").fill_null("-")

    print_quarterly_sheet(df_transformated, ot_resumen, year, quarter, False)




if __name__ == "__main__":
    pcm_quarterly("1")

    

    