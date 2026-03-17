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
    }

    name_dict = {"MTM-Mantenimiento Menor": "Status MTM", "MTB-Mantenimiento Mayor": "Status MTB"}


    for team in final_dict.keys():
        for status in final_dict[team].keys():
            count = (
                df
                .filter(
                    (pl.col(name_dict[team]) == status)                 
                    )
                .shape[0]                
            )

            final_dict[team][status] = count    
    
    return final_dict