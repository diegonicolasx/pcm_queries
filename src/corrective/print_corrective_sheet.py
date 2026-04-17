import polars as pl
import gspread
import os
import copy
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from typing import Dict
from src.utils.utils import ot_status_formating

load_dotenv()

def print_corrective_sheet(df_corrective:pl.DataFrame, year:str, month:str, test:bool)->None:

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
    
    mtm_df , mtb_df , snequipo_df , other_df = df_corrective

    ######## Insertar la Tabla de Mantenimientos mayores

    mtb_df = mtb_df.drop("Equipo")
    mtb_df = mtb_df.rename({"OT": "OT MTB"})
    mtb_df = mtb_df.sort(["Portafolio", "Parque"])

    data = [mtb_df.columns] + mtb_df.to_numpy().tolist()
    worksheet.update('A1', data)

    num_rows = len(mtb_df) + 1
    num_columns = len(mtb_df.columns)

    header_format = {
        "backgroundColor": {"red": 0.96, "green": 0.56, "blue": 0.05}, #Verde
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

    if num_rows > 1:  # Si hay datos además del encabezado        
        # Aplicar fondo azul claro y bordes

        requests = []

        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": worksheet.id,
                    "startRowIndex": 1,  # Desde fila 2 (índice 1)
                    "endRowIndex": num_rows,
                    "startColumnIndex": 0,  # Columna A
                    "endColumnIndex": num_columns  # Hasta columna J
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
            }),

        col_widths = [
            {"start": 0, "end": 1, "size": 150},
            {"start": 1, "end": 2, "size": 110},
            {"start": 2, "end": 3, "size": 85},
            {"start": 3, "end": 4, "size": 140},
            {"start": 4, "end": 5, "size": 320},
            {"start": 5, "end": 6, "size": 80},
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
    
    ot_status_formating(worksheet, sh, 3, num_rows)
    worksheet.format(f"E2:E{num_rows}", {"wrapStrategy": "WRAP"})
    worksheet.format(f"C2:C{num_rows}", {"horizontalAlignment": "CENTER"})
    worksheet.format(f"F2:F{num_rows}", {"horizontalAlignment": "CENTER"})
    
    #__________________________________________________________________________________#
    ### Insertar tabla de mantenimientos menores

    mtm_df = mtm_df.drop("Equipo")
    mtm_df = mtm_df.rename({"OT": "OT MTM"})
    mtm_df = mtm_df.sort(["Portafolio", "Parque"])

    data = [mtm_df.columns] + mtm_df.to_numpy().tolist()
    worksheet.update('H1', data)

    num_rows = len(mtm_df) + 1
    num_columns = len(mtm_df.columns)

    header_format = {
        "backgroundColor": {"red": 0.23, "green": 0.56, "blue": 0.75}, #Verde
        "textFormat": {
            "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},  # Blanco
            "fontSize": 10,
            "bold": True
        },
        "horizontalAlignment": "CENTER",
        "verticalAlignment": "MIDDLE",
        "wrapStrategy": "WRAP"
    }

    worksheet.format('H1:M1', header_format)


    if num_rows > 1:  # Si hay datos además del encabezado        
        # Aplicar fondo azul claro y bordes

        requests = []

        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": worksheet.id,
                    "startRowIndex": 1,  # Desde fila 2 (índice 1)
                    "endRowIndex": num_rows,
                    "startColumnIndex": 7,  # Columna A
                    "endColumnIndex": 7 + num_columns  # Hasta columna J
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
            }),

        col_widths = [
            {"start": 7, "end": 8, "size": 150},
            {"start": 8, "end": 9, "size": 110},
            {"start": 9, "end": 10, "size": 85},
            {"start": 10, "end": 11, "size": 140},
            {"start": 11, "end": 12, "size": 300},
            {"start": 12, "end": 13, "size": 80}
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
    
    ot_status_formating(worksheet, sh, 10, num_rows)
    worksheet.format(f"L2:L{num_rows}", {"wrapStrategy": "WRAP"})
    worksheet.format(f"J2:J{num_rows}", {"horizontalAlignment": "CENTER"})
    worksheet.format(f"M2:M{num_rows}", {"horizontalAlignment": "CENTER"})


    


    
