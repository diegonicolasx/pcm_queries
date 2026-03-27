import polars as pl
import requests
import os
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path

load_dotenv()

def normalizar_centro_costos(df, columna="centro_costos"):
    return df.with_columns(
        pl.col(columna)
        .str.strip_chars()
        .str.replace_all(r"\s*\(\s*\d*\s*\)", "")
        .str.replace_all(r"\s+", " ")
        .str.strip_chars()
        .str.to_lowercase()
        .str.replace_all("á", "a")
        .str.replace_all("é", "e")
        .str.replace_all("í", "i")
        .str.replace_all("ó", "o")
        .str.replace_all("ú", "u")
        .str.replace_all("ñ", "n")
        .str.replace_all("_", " ")
        .str.strip_chars()
        .alias(f"{columna}")
    )

def get_token() -> str:
    url = "https://one.fracttal.com/oauth/token"

    credentials = (os.getenv("CLIENT_ID"), os.getenv("CLIENT_SECRET"))

    data = {"grant_type": "client_credentials"}

    response = requests.post(url, auth=credentials, data=data)

    if response.status_code != 200:
        print("Error al obtener el token. Código de estado:", response.status_code)        

    token = response.json().get("access_token")

    return token

def validate_year(year_input:str)->bool:
    """Valida que el año tenga 4 dígitos y sea <= año actual"""
    current_year = datetime.now().year
    try:
        year_int = int(year_input)        
        
        if len(year_input) != 4:
            print("Error: Ha ingresado un valor no válido.")
            return False
        elif year_int > current_year:
            print(f"Error: El año no puede ser mayor al año actual ({current_year}).")
            return False
        else:
            return True
            
    except ValueError:
        print("Error: Debe ingresar un valor numérico válido.")
        return False
    
def validate_month(month_input:str)->bool:
    """Valida que el mes esté entre 1 y 12"""
    try:
        month_int = int(month_input)
        if month_int < 1 or month_int > 12:
            print("Error: El mes debe estar entre 1 y 12.")
            return False
        else:
            return True
                
    except ValueError:
        print("Error: Debe ingresar un valor numérico válido.")
        return False
    
def init_date(year:str, month:str, utc:bool)->str:    
    # Convertir a enteros
    year_int = int(year)
    month_int = int(month)
    
    # Crear la fecha (primer día del mes a las 00:00:00)
    date = datetime(year_int, month_int, 1, 0, 0, 0)
    
    if utc:
        # Formatear con UTC offset
        date = date.strftime('%Y-%m-%dT%H:%M:%S-03')
    
    return date

def end_date(since_date:str)->str:
    import calendar
    # Convertir a datetime
    fecha = datetime.fromisoformat(since_date)

    # Obtener el último día del mes
    ultimo_dia = calendar.monthrange(fecha.year, fecha.month)[1]

    # Crear fecha final con 23:59:59
    fecha_final = fecha.replace(day=ultimo_dia, hour=23, minute=59, second=59)

    # Convertir a string con formato correcto
    until_date = fecha_final.strftime('%Y-%m-%dT%H:%M:%S') + "-03"

    return until_date

def add_portfolio_and_name(df:pl.DataFrame, user_number:str)->pl.DataFrame:    
    if user_number == "1":
        results = Path(os.getenv("DYR_PATH"))

    elif user_number == "2":
        results = Path(os.getenv("VITTORIO_PATH"))    

    plant_db = pl.read_parquet(results / "plant_db.parquet").select(pl.col("fracttal_name"), pl.col("portfolio"), pl.col("rcc_name"))

    df_joined = (
        df
        .join(plant_db, left_on="costs_center_description", right_on="fracttal_name", how="left")
    )

    return df_joined

def filter_team(df:pl.DataFrame, team:str)->pl.DataFrame:
    dict_teams = {
        "MTM-Mantenimiento Menor":["OT MTM", "Status MTM"],
        "MTB-Mantenimiento Mayor":["OT MTB IVC", "Status MTB"],
        "OPE-Centro De Control":["OT OPE remoto", "Status OPE"],        
        "SSMA-Salud, Seguridad y Medio Ambiente":["OT SSMA", "Status SSMA"],
        "LAO-Lavado, Aseo y Ornato":["OT LAO", "Status LAO"],
    }

    if team == "LAO-Lavado, Aseo y Ornato":
        df_team = (
            df
            .filter(pl.col("tasks_log_types_description") == team)
            .select(
                pl.col("portfolio"),
                pl.col("rcc_name"),
                pl.col("wo_folio"),
                pl.col("id_status_work_order"),
                pl.col("description")
            )
            .with_columns(
                    # Crear columna de orden: 1 para Desmalezamiento, 2 para Limpieza
                    pl.when(pl.col("description").str.starts_with("Desmalezamiento"))
                    .then(pl.lit(1))
                    .when((pl.col("description").str.starts_with("Limpieza de módulos fotovoltaicos")) |
                          (pl.col("description").str.starts_with("Limpieza de modulos fotovoltaicos")))
                    .then(pl.lit(2))
                    .otherwise(pl.lit(3))
                    .alias("orden_tipo")
                )
            .drop(pl.col("description"))
            .unique()   
            .sort(["portfolio", "rcc_name", "orden_tipo", "wo_folio"])  # Ordenar por tipo
            .drop(pl.col("orden_tipo"))
            .with_columns(pl.col("wo_folio").cast(pl.Utf8).alias("wo_folio"))
        )
    else:
        df_team = (
            df
            .filter(pl.col("tasks_log_types_description") == team)
            .select(
                pl.col("portfolio"),
                pl.col("rcc_name"),
                pl.col("wo_folio"),
                pl.col("id_status_work_order"),
            )
            .unique()   
            .with_columns(pl.col("wo_folio").cast(pl.Utf8).alias("wo_folio"))
        )
    
    df_result =(
        df_team
        .group_by(["portfolio", "rcc_name"])
        .agg(
            # Concatenamos las OTs con " - " como separador
            pl.col("wo_folio").str.join(delimiter=" - ").alias("wo_folio"),
            # Tomamos el estado mínimo (menor número)
            pl.col("id_status_work_order").min().alias("id_status_work_order")                                        
        )
        .with_columns(
            pl.when(pl.col("id_status_work_order") == 1).then(pl.lit("OT en Proceso"))
            .when(pl.col("id_status_work_order") == 2).then(pl.lit("OT en Revisión"))
            .when(pl.col("id_status_work_order") == 3).then(pl.lit("OT Finalizada"))
            .otherwise(pl.lit("Sin estado"))
            .alias("id_status_work_order")
        )
        .select(
            pl.col("portfolio").alias("Portafolio"),
            pl.col("rcc_name").alias("Parque"),
            pl.col("wo_folio").alias(dict_teams.get(team)[0]),
            pl.col("id_status_work_order").alias(dict_teams.get(team)[1])
        )
    )

    return df_result

def equalize_dict_values_length(data, fill_value=None):
    max_length = max(len(v) for v in data.values())  # Encontrar el tamaño máximo

    for key in data:
        data[key] += [fill_value] * (max_length - len(data[key]))  # Rellenar con el valor deseado

    return data

trigger_dict = {
    "NO_SCHEDULE_TASK" : "Tarea no Programada",
    "DATE$EVERY$1$MONTHS" : "Fecha Cada 1 Meses",
    "DATE$EVERY$6$MONTHS" : "Fecha Cada 6 Meses",	 
    "DATE$EVERY$4$MONTHS" : "Fecha Cada 4 Meses",	
    "EVENT$Corte de vegetacion" : "Evento Corte de vegetacion",
    "DATE$EVERY$12$MONTHS" : "Fecha Cada 12 Meses",
    "EVENT$Lavado de paneles" : "Evento Lavado de paneles",
    "EVENT$LAVADO PANELES" : "Evento LAVADO PANALES",
    "EVENT$Solución de problemas NVR" : "Eventos Solución de problemas NVR",
    "EVENT$CORTE DE PASTO" : "Eventos CORTE DE PASTO",
    "DATE$EVERY$24$MONTHS" : "Fecha Cada 24 Meses",
    "DATE$EVERY$3$MONTHS" : "Fecha Cada 3 Meses",
    "EVENT$GRAZE CUT" : "Eventos GRAZE CUT",
    "EVENT$Limpieza de canales" : "Eventos Limpieza de canales",
    "EVENT$Limpieza de ventiladores IVS" : "Eventos Limpieza de ventiladores IVS",
    "EVENT$COMMUNICATION FAILURE" : "Eventos COMMUNICATION FAILURE",
    "EVENT$REMOTE RECLOSER CLOSURE" : "Eventos REMOTE RECLOSER CLOSURE",
    "EVENT$AJUSTE INVENTARIO" : "Eventos AJUSTE INVENTARIO",
    "EVENT$MANTENIMIENTO DE CANALES" : "Eventos MANTENIMIENTO DE CANALES",
    "EVENT$IVS INSPECTION / REMOTE" : "Eventos IVS INSPECTION / REMOTE",
    "EVENT$ON-SITE IVS INSPECTION" : "Eventos ON-SITE IVS INSPECTION",
    "EVENT$LAO" : "Eventos LAO",
    "DATE$EVERY$36$MONTHS" : "",
    "DATE$EVERY$1$WEDNESDAY" : "Fecha Cada 1 Miercoles",
    "READING$EACH$10000$KILOMETROS [Km] ( Vehiculo001 {} )" : "READING$EACH$10000$KILOMETROS [Km] ( Vehiculo001 {} )",
    "DATE$EVERY$1$WEEKS" : "Fecha Cada 1 Semanas",
    "DATE$EVERY$2$MONTHS" : "Fecha Cada 2 Meses"
}

schema_work_orders = {
    "id_work_order":pl.Int64,
    "id_work_orders_tasks":pl.Int64,    
    "id_task":pl.Int64,
    "id_item":pl.Int64,
    "id_status_work_order":pl.Int32,
    "wo_folio":pl.Int32,
    "creation_date":pl.String,
    "duration":pl.Int32,    
    "initial_date":pl.String,
    "id_priorities":pl.Int32,
    "priorities_description":pl.String,
    "final_date":pl.String,
    "completed_percentage":pl.Int32,
    "created_by":pl.String,
    "signature":pl.String,
    "personnel_description":pl.String,
    "personnel_path_image":pl.String,
    "code":pl.String,
    "items_log_description":pl.String,
    "done":pl.Boolean,
    "description":pl.String,
    "id_request":pl.Int64,
    "stop_assets":pl.Boolean,
    "stop_assets_sec":pl.Int32,
    "tasks_log_task_type_main":pl.String,
    "parent_description":pl.String,
    "trigger_description":pl.String,
    "resources_hours":pl.String,        
    "resources_inventory":pl.String,
    "resources_human_resources":pl.String,
    "resources_services":pl.String,
    "cal_date_maintenance":pl.String,
    "real_duration":pl.Int32,
    "date_maintenance":pl.String,
    "user_assigned":pl.String,
    "note":pl.String,
    "details_signature":pl.String,
    "responsible_path_signature":pl.String,
    "validator_path_signature":pl.String,
    "first_date_task":pl.String,
    "costs_center_description":pl.String,
    "tasks_duration":pl.Int32,
    "total_cost_task":pl.Int32,
    "requested_by":pl.String,
    "groups_description":pl.String,
    "groups_1_description":pl.String,
    "groups_2_description":pl.String,
    "task_note":pl.String,
    "id_parent_wo":pl.Int64,
    "has_children":pl.Boolean,
    "real_stop_assets_sec":pl.Int32,
    "wo_final_date":pl.String,
    "is_offline":pl.Boolean,
    "tasks_log_types_description":pl.String,
    "tasks_log_types_2_description":pl.String,
    "code_create_by":pl.String,
    "event_date":pl.String,
    "rating":pl.Int32,
    "enable_budget":pl.Boolean,
    "work_orders_status_custom_description":pl.String,
    "review_date":pl.String,
    "code_responsible":pl.String,
    "id_parent":pl.Int64,
    "id_failure_type":pl.Int64,
    "types_description":pl.String,
    "id_failure_cause":pl.Int64,
    "causes_description":pl.String,
    "id_failure_detection_method":pl.Int64,
    "detection_method_description":pl.String,
    "id_failure_severity":pl.Int64,
    "severiry_description":pl.String,
    "id_damage_type":pl.Int64,
    "damages_types_description":pl.String,
    "caused_damage":pl.String,
    "id_group_task":pl.Int64,
    "id_items_availability":pl.Int64,
    "validated_by_description":pl.String,
    "id_personnel_log":pl.Int64,
    "id_contacts_log":pl.Int64,
    "annotations":pl.String,
    "id_personnel":pl.Int64,
    "id_contact":pl.Int64,
    "task_status": pl.String,
    "id_status_work_order_task": pl.String,
    "labels": pl.String,
    "time_disruption": pl.String,
    "id_company":pl.Int64,
}

schema_work_orders_2 = {
    "id_work_order":pl.Int64,
    "id_work_orders_tasks":pl.Int64,    
    "id_task":pl.Int64,
    "id_item":pl.Int64,
    "id_status_work_order":pl.Int32,
    "wo_folio":pl.Int32,
    "creation_date":pl.String,
    "duration":pl.Int32,    
    "initial_date":pl.String,
    "id_priorities":pl.Int32,
    "priorities_description":pl.String,
    "final_date":pl.String,
    "completed_percentage":pl.Int32,
    "created_by":pl.String,
    "signature":pl.String,
    "personnel_description":pl.String,
    "personnel_path_image":pl.String,
    "code":pl.String,
    "items_log_description":pl.String,
    "done":pl.Boolean,
    "description":pl.String,
    "id_request":pl.Int64,
    "stop_assets":pl.Boolean,
    "stop_assets_sec":pl.Int32,
    "tasks_log_task_type_main":pl.String,
    "parent_description":pl.String,
    "trigger_description":pl.String,
    "resources_hours":pl.String,        
    "resources_inventory":pl.String,
    "resources_human_resources":pl.String,
    "resources_services":pl.String,
    "cal_date_maintenance":pl.String,
    "real_duration":pl.Int32,
    "date_maintenance":pl.String,
    "user_assigned":pl.String,
    "note":pl.String,
    "details_signature":pl.String,
    "responsible_path_signature":pl.String,
    "validator_path_signature":pl.String,
    "first_date_task":pl.String,
    "costs_center_description":pl.String,
    "tasks_duration":pl.Int32,
    "total_cost_task":pl.Int32,
    "requested_by":pl.String,
    "groups_description":pl.String,
    "groups_1_description":pl.String,
    "groups_2_description":pl.String,
    "task_note":pl.String,
    "id_parent_wo":pl.Int64,
    "has_children":pl.Boolean,
    "real_stop_assets_sec":pl.Int32,
    "wo_final_date":pl.String,
    "is_offline":pl.Boolean,
    "tasks_log_types_description":pl.String,
    "tasks_log_types_2_description":pl.String,
    "code_create_by":pl.String,
    "event_date":pl.String,
    "rating":pl.Int32,
    "enable_budget":pl.Boolean,
    "work_orders_status_custom_description":pl.String,
    "review_date":pl.String,
    "code_responsible":pl.String,
    "id_parent":pl.Int64,
    "id_failure_type":pl.Int64,
    "types_description":pl.String,
    "id_failure_cause":pl.Int64,
    "causes_description":pl.String,
    "id_failure_detection_method":pl.Int64,
    "detection_method_description":pl.String,
    "id_failure_severity":pl.Int64,
    "severiry_description":pl.String,
    "id_damage_type":pl.Int64,
    "damages_types_description":pl.String,
    "caused_damage":pl.String,
    "id_group_task":pl.Int64,
    "id_items_availability":pl.Int64,
    "validated_by_description":pl.String,
    "id_personnel_log":pl.Int64,
    "id_contacts_log":pl.Int64,
    "annotations":pl.String,
    "id_personnel":pl.Int64,
    "id_contact":pl.Int64,
    "task_status": pl.String,
    "id_status_work_order_task": pl.String,
    "labels": pl.String,
    "time_disruption": pl.String,
    "id_company":pl.Int64,
    "children":pl.String
}