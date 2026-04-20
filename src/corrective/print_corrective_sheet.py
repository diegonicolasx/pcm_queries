import polars as pl
import gspread
import os
import copy
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from typing import Dict
from src.utils.utils import ot_status_formating
from typing import Tuple

load_dotenv()

def print_corrective_sheet(df_corrective: Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame], 
                           year:str, 
                           month:str, 
                           test:bool)->None:

    SHEET_KEY = "1qltarr2mibCoWQSZqXU2qcib0knP93dRI-4kUfTk7DA"

    MONTHS = {
        "1": "Enero", "2": "Febrero", "3": "Marzo", 
        "4": "Abril", "5": "Mayo", "6": "Junio",
        "7": "Julio", "8": "Agosto", "9": "Septiembre",
        "10": "Octubre", "11": "Noviembre", "12": "Diciembre"
    }

    sheet_name = f"{'Test ' if test else ''}{MONTHS.get(month)} 1M ({year})"

    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file('key/credentials.json', scopes=scope)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_KEY)
    existing_sheets = {sheet.title: sheet for sheet in sh.worksheets()}

    if sheet_name in existing_sheets:
        print("\nLa hoja de cálculo ya existe, se eliminará y se creará una nueva.")        
        sh.del_worksheet(sh.worksheet(sheet_name))
    else:
        print("\nNo existe la hoja de cálculo, se creará una nueva.")

    sh.add_worksheet(title=sheet_name, rows="500", cols="40")
    worksheet = sh.worksheet(sheet_name)

    #### Obtener los dataframes de los correctivos por equipo, corregir alfunas cosas

    mtm_df , mtb_df , snequipo_df , other_df = df_corrective

    mtb_df = mtb_df.drop("Equipo").rename({"OT": "OT MTB"}).sort(["Portafolio", "Parque"])
    mtm_df = mtm_df.drop("Equipo").rename({"OT": "OT MTM"}).sort(["Portafolio", "Parque"])
    snequipo_df = snequipo_df.drop("Equipo").rename({"OT": "OT Sin Equipo"}).sort(["Portafolio", "Parque"])
    other_df = other_df.drop("Equipo").rename({"OT": "OT Otros Equipos"}).sort(["Portafolio", "Parque"])

    # Insertar los datos

    data_mtb = [mtb_df.columns] + mtb_df.to_numpy().tolist()
    data_mtm = [mtm_df.columns] + mtm_df.to_numpy().tolist()
    data_snequipo = [snequipo_df.columns] + snequipo_df.to_numpy().tolist()
    data_other = [other_df.columns] + other_df.to_numpy().tolist()

    worksheet.update(data_mtb, "A1")
    worksheet.update(data_mtm, "H1")
    worksheet.update(data_snequipo, "O1")
    worksheet.update(data_other, "V1")

    ## Dimensiones
    num_rows_1, num_columns_1 = len(mtb_df) + 1, len(mtb_df.columns)
    num_rows_2, num_columns_2 = len(mtm_df) + 1, len(mtm_df.columns)
    num_rows_3, num_columns_3 = len(snequipo_df) + 1, len(snequipo_df.columns)
    num_rows_4, num_columns_4 = len(other_df) + 1, len(other_df.columns)

    num_rows = [num_rows_1,num_rows_2, num_rows_3, num_rows_4]
    num_columns = [num_columns_1, num_columns_2, num_columns_3, num_columns_4]

    #Formato del header

    header_format = {
        "backgroundColor": {"red": 1.0, "green": 0.6, "blue": 0.0}, #Verde
        "textFormat": {
            "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},  # Blanco
            "fontSize": 10,
            "bold": True
        },
        "horizontalAlignment": "CENTER",
        "verticalAlignment": "MIDDLE",
        "wrapStrategy": "WRAP"
    }

    worksheet.format('A1:F1', header_format)

    header_format["backgroundColor"] = {"red": 0.23, "green": 0.56, "blue": 0.75}

    worksheet.format('H1:M1', header_format)

    header_format["backgroundColor"] = {"red": 0.56, "green": 0.46, "blue": 0.64}

    worksheet.format('O1:T1', header_format)

    header_format["backgroundColor"] = {"red": 0.34, "green": 0.56, "blue": 0.22}

    worksheet.format('V1:AA1', header_format)

    requests = []

    for i in range(4):

        rows = num_rows[i]
        col = num_columns[i]
        
        if rows > 1:
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": 1,  # Desde fila 2 (índice 1)
                        "endRowIndex": rows,
                        "startColumnIndex": 7*i,  # Columna A
                        "endColumnIndex": 7*i + col  # Hasta columna J
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": {"red": 0.93, "green": 0.98, "blue": 0.97},
                            "borders": {
                                "top": {"style": "SOLID", "width": 1, "color": {"red": 0.6, "green": 0.6, "blue": 0.6}},
                                "bottom": {"style": "SOLID", "width": 1, "color": {"red": 0.6, "green": 0.6, "blue": 0.6}},
                                "left": {"style": "SOLID", "width": 1, "color": {"red": 0.6, "green": 0.6, "blue": 0.6}},
                                "right": {"style": "SOLID", "width": 1, "color": {"red": 0.6, "green": 0.6, "blue": 0.6}}
                            },
                            "verticalAlignment": "MIDDLE"
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,borders,verticalAlignment)"
                }
                })
        else:
            continue
    
    col_widths = [
            {"start": 0, "end": 1, "size": 150},
            {"start": 1, "end": 2, "size": 110},
            {"start": 2, "end": 3, "size": 85},
            {"start": 3, "end": 4, "size": 140},
            {"start": 4, "end": 5, "size": 320},
            {"start": 5, "end": 6, "size": 75},
            {"start": 7, "end": 8, "size": 150},
            {"start": 8, "end": 9, "size": 110},
            {"start": 9, "end": 10, "size": 85},
            {"start": 10, "end": 11, "size": 140},
            {"start": 11, "end": 12, "size": 300},
            {"start": 12, "end": 13, "size": 75},
            {"start": 14, "end": 15, "size": 150},
            {"start": 15, "end": 16, "size": 110},
            {"start": 16, "end": 17, "size": 110},
            {"start": 17, "end": 18, "size": 140},
            {"start": 18, "end": 19, "size": 300},
            {"start": 19, "end": 20, "size": 75},
            {"start": 21, "end": 22, "size": 150},
            {"start": 22, "end": 23, "size": 110},
            {"start": 23, "end": 24, "size": 150},
            {"start": 24, "end": 25, "size": 140},
            {"start": 25, "end": 26, "size": 300},
            {"start": 26, "end": 27, "size": 75}
        ]
    
    for cw in col_widths:
            requests.append({
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": worksheet.id,
                        "dimension": "COLUMNS",
                        "startIndex": cw["start"],
                        "endIndex": cw["end"]
                    },
                    "properties": {"pixelSize": cw["size"]},
                    "fields": "pixelSize"
                }
            })
    
    sh.batch_update({"requests": requests})

    ot_status_formating(worksheet, sh, 3, num_rows_1)
    ot_status_formating(worksheet, sh, 10, num_rows_2)
    ot_status_formating(worksheet, sh, 17, num_rows_3)
    ot_status_formating(worksheet, sh, 24, num_rows_4)

    worksheet.format(f"E2:E{num_rows_1}", {"wrapStrategy": "WRAP"})
    worksheet.format(f"C2:C{num_rows_1}", {"horizontalAlignment": "CENTER"})
    worksheet.format(f"F2:F{num_rows_1}", {"horizontalAlignment": "CENTER"})
    worksheet.format(f"L2:L{num_rows_2}", {"wrapStrategy": "WRAP"})
    worksheet.format(f"J2:J{num_rows_2}", {"horizontalAlignment": "CENTER"})
    worksheet.format(f"M2:M{num_rows_2}", {"horizontalAlignment": "CENTER"})
    worksheet.format(f"S2:S{num_rows_3}", {"wrapStrategy": "WRAP"})
    worksheet.format(f"Q2:Q{num_rows_3}", {"horizontalAlignment": "CENTER"})
    worksheet.format(f"T2:T{num_rows_3}", {"horizontalAlignment": "CENTER"})
    worksheet.format(f"Z2:Z{num_rows_4}", {"wrapStrategy": "WRAP"})
    worksheet.format(f"X2:X{num_rows_4}", {"horizontalAlignment": "CENTER"})
    worksheet.format(f"AA2:AA{num_rows_4}", {"horizontalAlignment": "CENTER"})