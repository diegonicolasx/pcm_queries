import polars as pl
from src.utils.utils  import add_portfolio_and_name, filter_team

def transform_quarterly_calculated_data(df:pl.DataFrame, user_number:str)->pl.DataFrame:

    quarterly_df = (
        df
        .filter(
            (pl.col("id_status_work_order") != 4) &
            ((pl.col("trigger_description") == "DATE$EVERY$3$MONTHS"))
        )        
    )

    quarterly_df = add_portfolio_and_name(quarterly_df, user_number)

    teams = [
        "MTM-Mantenimiento Menor",
        "MTB-Mantenimiento Mayor",
    ]

    count = 0

    for team in teams:
        count += 1

        df = filter_team(quarterly_df, team)

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
            pl.col("Status MTM"),
            pl.col("Status MTB"),
        )
        .sort(by = "Parque")
    )

    return df_joined
