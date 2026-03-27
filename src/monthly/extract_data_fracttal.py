import requests
import polars as pl
import numpy as np
from datetime import datetime
from src.utils.utils import (
    schema_work_orders, 
    schema_work_orders_2, 
    equalize_dict_values_length,     
    normalizar_centro_costos,
    get_token
)

def extract_wo_api(since_date:str, until_date:str) -> pl.DataFrame:
    print("\nSe extraerán los datos de las ordenes de trabajo a travez de la API.")
            
    ot_df = pl.DataFrame(schema=schema_work_orders_2)

    token = get_token()
    
    headers = {
        "Authorization": f"Bearer {token}"}

    count = 0

    numero_final = 1000000

    array = np.arange(0, numero_final + 1, 100)

    for i in array:        

        count = count + 1

        print(f"\nIteración {count}")

        url_object = "https://app.fracttal.com/api/work_orders"
        
        params = {
            "since" : since_date,
            "until" : until_date,
            "type_date": "date_maintenance",
            "start" : i,
            "limit" : 100            
        }
        

        try:
            r = requests.get(url_object, headers=headers, params=params)
            print(r)
            data_raw = r.json()["data"]
        except:            
            #r = requests.get(url_object, headers=headers, params=params)
            #data_raw = r.json()["data"]
            print("No se encontraron datos")
            continue


        if data_raw:
            print("\nSe extraerán datos.")
            dict_data = {}            
            try:
                for j in data_raw:
                    #print("\nLista de columnas:")                                                  
                    data = dict(j)
                    keys = data.keys()
                    keys_2 = dict_data.keys()
                    for key in keys:
                        #print(f"\t-{key}: {data[key]}")
                        if key not in keys_2:
                            dict_data.update({key:[]})
                        dict_data[key].append(data[key])            
            except Exception as e:
                print(f"\nError al extraer datos: {e}")

            try:                
                print(dict_data["children"])
                if len(dict_data["children"]) != len(dict_data["id_work_order"]):
                    dict_data = equalize_dict_values_length(dict_data)
                    print(len(dict_data["children"]))
            except Exception as e:
                print(f"\nNo existe children. Error: {e}")

    
            try:  
                df = pl.DataFrame(dict_data, schema=schema_work_orders, strict=False)
            except Exception as e:
                print(e)
                df = pl.DataFrame(dict_data, schema=schema_work_orders_2, strict=False)

            if "children" not in df.columns:
                df = (
                    df
                    .with_columns(pl.lit(None).alias("children"))
                )
            
            ot_df = pl.concat([ot_df, df])
            print(f"\nDatos extraídos en la iteración {count}")
            
        else:
            print("No hay más datos disponibles.")
            break   
    
    input_date_format = "%Y-%m-%dT%H:%M:%S%.f%:z"

    ot_df = (
        ot_df
        .with_columns(
            pl.col("creation_date").str.to_datetime(format=input_date_format).dt.convert_time_zone("America/Santiago"),
            pl.col("initial_date").str.to_datetime(format=input_date_format).dt.convert_time_zone("America/Santiago"),
            pl.col("final_date").str.to_datetime(format=input_date_format).dt.convert_time_zone("America/Santiago"),
            pl.col("cal_date_maintenance").str.to_datetime(format=input_date_format).dt.convert_time_zone("America/Santiago"),
            pl.col("date_maintenance").str.to_datetime(format=input_date_format).dt.convert_time_zone("America/Santiago"),
            pl.col("first_date_task").str.to_datetime(format=input_date_format).dt.convert_time_zone("America/Santiago"),
            pl.col("wo_final_date").str.to_datetime(format=input_date_format).dt.convert_time_zone("America/Santiago"),
            pl.col("event_date").str.to_datetime(format=input_date_format).dt.convert_time_zone("America/Santiago"),
            pl.col("review_date").str.to_datetime(format=input_date_format).dt.convert_time_zone("America/Santiago")
        )
    )

    ot_df = (
        ot_df
        .with_columns(
            pl.col("creation_date").dt.strftime("%Y-%m-%d %H:%M").str.to_datetime(),
            pl.col("initial_date").dt.strftime("%Y-%m-%d %H:%M").str.to_datetime(),
            pl.col("final_date").dt.strftime("%Y-%m-%d %H:%M").str.to_datetime(),
            pl.col("cal_date_maintenance").dt.strftime("%Y-%m-%d %H:%M").str.to_datetime(),
            pl.col("date_maintenance").dt.strftime("%Y-%m-%d %H:%M").str.to_datetime(),
            pl.col("first_date_task").dt.strftime("%Y-%m-%d %H:%M").str.to_datetime(),
            pl.col("wo_final_date").dt.strftime("%Y-%m-%d %H:%M").str.to_datetime(),
            pl.col("event_date").dt.strftime("%Y-%m-%d %H:%M").str.to_datetime(),
            pl.col("review_date").dt.strftime("%Y-%m-%d %H:%M").str.to_datetime()
        )
    )    

    ot_df = (
        normalizar_centro_costos(ot_df, "costs_center_description")
    )

    ## Esta parte corrige las OT de baño, que aparece MTM como responable, pero son externos contratados por el cliente

    ot_df = ot_df.with_columns(
            pl.when(
                (pl.col("tasks_log_types_description") == "MTM-Mantenimiento Menor") & 
                (pl.col("groups_description") == "Baño")
            )
            .then(pl.lit("SSMA-Salud, Seguridad y Medio Ambiente"))
            .otherwise(pl.col("tasks_log_types_description"))
            .alias("tasks_log_types_description")  
    )

    return ot_df

if __name__ == "__main__":
    import calendar
    since_date = "2026-01-01T00:00:00-03"
    # Convertir a datetime
    fecha = datetime.fromisoformat(since_date)

    # Obtener el último día del mes
    ultimo_dia = calendar.monthrange(fecha.year, fecha.month)[1]

    # Crear fecha final con 23:59:59
    fecha_final = fecha.replace(day=ultimo_dia, hour=23, minute=59, second=59)

    # Convertir a string con formato correcto
    until_date = fecha_final.strftime('%Y-%m-%dT%H:%M:%S') + "-03"

    df = extract_wo_api(since_date, until_date)
    print(df)
    df.write_excel("test")
    #df.write_parquet("results/work_orders.parquet")