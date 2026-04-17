import polars as pl
from src.utils.utils  import add_portfolio_and_name, filter_team

def transform_correctives(wo_dataframe : pl.DataFrame, user_number: str):

    correctives_df = wo_dataframe.filter((pl.col("id_status_work_order") != 4) &
                                  (pl.col("trigger_description") == "NO_SCHEDULE_TASK")
                                  )
    
    correctives_df = add_portfolio_and_name(correctives_df, user_number)

    correctives_df = correctives_df.with_columns(
        pl.when(pl.col("tasks_log_types_description") == "LAO-Lavado, Aseo y Ornato")
        .then(pl.lit("LAC-Lavado y Corte de Vegetacion"))
        .when(pl.col("tasks_log_types_description").is_null())
        .then(pl.lit("Sin Equipo"))
        .otherwise(pl.col("tasks_log_types_description"))
        .alias("tasks_log_types_description")
    )

    teams = [
        "MTM-Mantenimiento Menor",
        "MTB-Mantenimiento Mayor",
        "OPE-Centro De Control",        
        "SSMA-Salud, Seguridad y Medio Ambiente",
        "LAC-Lavado y Corte de Vegetacion",
        "Sin Equipo"
    ]

    count = 0
    df_joined = pl.DataFrame()

    for team in teams:
        count += 1
        df = (
            correctives_df.filter(pl.col("tasks_log_types_description") == team)
                .select(
                    pl.col("portfolio"),
                    pl.col("rcc_name"),
                    pl.col("wo_folio"),
                    pl.col("id_status_work_order"),
                    pl.col("description"),
                    pl.col("id_request")
                )
                .unique()   
                .with_columns(pl.col("wo_folio").cast(pl.Utf8).alias("wo_folio"))

        )

        df = df.with_columns(pl.lit(team).alias("Equipo"))

        df = (
            df.with_columns(
                pl.when(pl.col("id_status_work_order") == 1).then(pl.lit("OT en Proceso"))
                .when(pl.col("id_status_work_order") == 2).then(pl.lit("OT en Revisión"))
                .when(pl.col("id_status_work_order") == 3).then(pl.lit("OT Finalizada"))
                .otherwise(pl.lit("Sin estado"))
                .alias("id_status_work_order")
            )
            .select(
                pl.col("portfolio").alias("Portafolio"),
                pl.col("rcc_name").alias("Parque"),
                pl.col("wo_folio").alias("OT"),
                pl.col("Equipo"),
                pl.col("id_status_work_order").alias("Estado"),
                pl.col("description").alias("Descripción"),
                pl.col("id_request").alias("ST")
            )
        )


        if count == 1:
            df_joined = df

        else:
            df_joined = pl.concat([df_joined, df])
        
    
    #### separacion

    mtm_df = df_joined.filter(pl.col("Equipo") =="MTM-Mantenimiento Menor")
    mtb_df = df_joined.filter(pl.col("Equipo") =="MTB-Mantenimiento Mayor")
    se_df = df_joined.filter(pl.col("Equipo") =="Sin Equipo")
    other_df = df_joined.filter(~pl.col("Equipo").is_in(["MTM-Mantenimiento Menor","MTB-Mantenimiento Mayor","Sin Equipo"]))
    



    return mtm_df, mtb_df, se_df, other_df