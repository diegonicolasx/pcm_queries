import polars as pl

def get_kpi(df:pl.DataFrame) -> dict:

    final_dict = {
        "MTM-Mantenimiento Menor": {
            "OT en Proceso": 0,
            "OT en Revisión": 0,
            "OT Finalizada": 0,
            "Sin estado": 0
        },
        "MTB-Mantenimiento Mayor": {
            "OT en Proceso": 0,
            "OT en Revisión": 0,
            "OT Finalizada": 0,
            "Sin estado": 0
        },
        "OPE-Centro De Control": {
            "OT en Proceso": 0,
            "OT en Revisión": 0,
            "OT Finalizada": 0,
            "Sin estado": 0
        },
        "SSMA-Salud, Seguridad y Medio Ambiente": {
            "OT en Proceso": 0,
            "OT en Revisión": 0,
            "OT Finalizada": 0,
            "Sin estado": 0
        },
        "LAO-Lavado, Aseo y Ornato": {
            "OT en Proceso": 0,
            "OT en Revisión": 0,
            "OT Finalizada": 0,
            "Sin estado": 0
        }
    }

    df = (
        df
        .filter(
            (pl.col("id_status_work_order") != 4) &
            ((pl.col("trigger_description") == "DATE$EVERY$1$MONTHS") |
             (pl.col("tasks_log_types_description") == "LAO-Lavado, Aseo y Ornato"))
        )    
        .select(            
            pl.col("tasks_log_types_description"),
            pl.col("wo_folio"),
            pl.col("id_status_work_order"),
        )
        .unique()
        .with_columns(
            pl.when(pl.col("id_status_work_order") == 1).then(pl.lit("OT en Proceso"))
            .when(pl.col("id_status_work_order") == 2).then(pl.lit("OT en Revisión"))
            .when(pl.col("id_status_work_order") == 3).then(pl.lit("OT Finalizada"))
            .otherwise(pl.lit("Sin estado"))
            .alias("id_status_work_order")
        )      
    )

    for team in final_dict.keys():
        for status in final_dict[team].keys():
            count = (
                df
                .filter(
                    (pl.col("tasks_log_types_description") == team) &
                    (pl.col("id_status_work_order") == status)
                )
                .shape[0]                
            )
            final_dict[team][status] = count    
    
    return final_dict
