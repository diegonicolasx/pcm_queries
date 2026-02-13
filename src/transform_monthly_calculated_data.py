import polars as pl
from .utils import add_portfolio_and_name, filter_team

def transform_monthly_calculated_data(df:pl.DataFrame, user_number:int)->pl.DataFrame:
    monthly_df = (
        df
        .filter(
            (pl.col("trigger_description") == "DATE$EVERY$1$MONTHS") &
            (pl.col("id_status_work_order") != 4)
        )
        #.with_columns(
        #    pl.when(pl.col("id_status_work_order") == 1).then(pl.lit("OT en Proceso"))
        #    .when(pl.col("id_status_work_order") == 2).then(pl.lit("OT en Revisión"))
        #    .when(pl.col("id_status_work_order") == 3).then(pl.lit("OT Finalizada"))
        #    .otherwise(pl.lit("Sin OT"))
        #    .alias("id_status_work_order")
        #)
    )

    monthly_df = add_portfolio_and_name(monthly_df, user_number)

    teams = [
        "MTM-Mantenimiento Menor",
        "MTB-Mantenimiento Mayor",
        "OPE-Centro De Control",        
        "SSMA-Salud, Seguridad y Medio Ambiente",
    ]

    count = 0

    for team in teams:
        count += 1

        df = filter_team(monthly_df, team)

        if count == 1:
            df_joined = df
        else:
            df_joined = (
                df_joined
                .join(df, on=["Portafolio", "Parque"], how="full", coalesce=True)
            )
        
    df_joined = (
        df_joined
        .select(
            pl.col("Portafolio"),
            pl.col("Parque"),
            pl.col("OT MTM"),
            pl.col("OT MTB IVC"),
            pl.col("OT OPE remoto"),
            pl.col("OT SSMA"),
            pl.col("Status MTM"),
            pl.col("Status MTB"),
            pl.col("Status OPE"),
            pl.col("Status SSMA")
        )
        .sort(by = "Parque")
    )

    return df_joined
