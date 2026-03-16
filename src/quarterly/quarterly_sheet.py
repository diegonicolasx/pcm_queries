import gspread
from gspread_formatting import set_row_height
import polars as pl
import os
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from typing import Dict

load_dotenv()



def print_quarterly_sheet(df:pl.DataFrame, year:str, quarter:str, test:bool)->None:

    SHEET_KEY = os.getenv("SHEET_KEY")

    Q = {
        "1": "(Ene/Mar)", "2": "(Abr/Jun)", "3": "(Jul/Sep)", "4": "(Oct/Dic)" 
    }

    sheet_name = f"{'Test ' if test else ''}3M {year} {Q.get(quarter)}"

    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file('key/credentials.json', scopes=scope)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_KEY)    
    existing_sheets = {sheet.title: sheet for sheet in sh.worksheets()}

    try:
        sh.del_worksheet(sh.worksheet("3M 2026 (Ene/Mar))"))

    except:
        pass

    if sheet_name in existing_sheets:
        print("\nLa hoja de cálculo ya existe, se eliminará y se creará una nueva.")        
        sh.del_worksheet(sh.worksheet(sheet_name))
    else:
        print("\nNo existe la hoja de cálculo, se creará una nueva.")

    sh.add_worksheet(title=sheet_name, rows="500", cols="40")
    worksheet = sh.worksheet(sheet_name)
    # Traducir nombres de columnas al texto de display (con \n si aplica)
    display_headers = df.columns
    data = [display_headers] + df.to_numpy().tolist()
    worksheet.update('A1', data)

    num_rows = len(df) + 1  # +1 por el encabezado

    header_format = {
        "backgroundColor": {"red": 0.2745, "green": 0.7412, "blue": 0.7765}, #Celeste
        "textFormat": {
            "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},  # Blanco
            "fontSize": 10,
            "bold": True
        },
        "borders": {
            "top": {"style": "SOLID", "width": 1, "color": {"red": 0.1, "green": 0.1, "blue": 0.1}},
            "bottom": {"style": "SOLID", "width": 1, "color": {"red": 0.1, "green": 0.1, "blue": 0.1}},
            "left": {"style": "SOLID", "width": 1, "color": {"red": 0.1, "green": 0.1, "blue": 0.1}},
            "right": {"style": "SOLID", "width": 1, "color": {"red": 0.1, "green": 0.1, "blue": 0.1}}
            },
        "horizontalAlignment": "CENTER",
        "verticalAlignment": "MIDDLE"
    }

    worksheet.format('A1:B1', header_format)

    header_format.update({"backgroundColor": {"red": 1.0, "green": 0.6, "blue": 0.0}})  # Naranjo

    worksheet.format('C1:D1', header_format)

    header_format.update({"backgroundColor": {"red": 0.2039, "green": 0.6588, "blue": 0.3255}})  # Verde

    worksheet.format('E1:F1', header_format)

    set_row_height(worksheet, "1", 40)

     # 2. Formato de fondo y bordes para todas las filas de datos (dinámico)
    if num_rows > 1:  # Si hay datos además del encabezado        
        # Aplicar fondo azul claro y bordes
        requests = [{
            "repeatCell": {
                "range": {
                    "sheetId": worksheet.id,
                    "startRowIndex": 1,  # Desde fila 2 (índice 1)
                    "endRowIndex": num_rows,
                    "startColumnIndex": 0,  # Columna A
                    "endColumnIndex": 6  # Hasta columna J
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.95, "green": 1.0, "blue": 1.0},
                        "borders": {
                            "top": {"style": "SOLID", "width": 1, "color": {"red": 0.1, "green": 0.1, "blue": 0.1}},
                            "bottom": {"style": "SOLID", "width": 1, "color": {"red": 0.1, "green": 0.1, "blue": 0.1}},
                            "left": {"style": "SOLID", "width": 1, "color": {"red": 0.1, "green": 0.1, "blue": 0.1}},
                            "right": {"style": "SOLID", "width": 1, "color": {"red": 0.1, "green": 0.1, "blue": 0.1}}
                        },
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE"
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,borders,horizontalAlignment,verticalAlignment)"
            }
        }]
        
        sh.batch_update({"requests": requests})

    
    # 4. Ajustar ancho de columnas
    # Obtener valores de la hoja
    all_values = worksheet.get_all_values()

    requests = []

    pixel_size = [150, 120, 85, 120, 150, 150]
    
    for col_index in range(len(all_values[0])):  # Para cada columna
        # Encontrar el texto más largo en la columna        

        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": worksheet.id,
                    "dimension": "COLUMNS",
                    "startIndex": col_index,
                    "endIndex": col_index + 1
                },
                "properties": {
                    "pixelSize": pixel_size[col_index]
                },
                "fields": "pixelSize"
            }
        })


    rule = {
        "condition": {
            "type": "ONE_OF_LIST",
            "values": [
                {"userEnteredValue": "OT en Proceso"},
                {"userEnteredValue": "OT en Revisión"},
                {"userEnteredValue": "OT Finalizada"}
            ]
        },
        "showCustomUi": True, # Esto habilita el estilo de "chip"
        "strict": True    }

    requests.append({
        "setDataValidation": {
            "range": {
                "sheetId": worksheet.id,
                "startRowIndex": 1, 
                "endRowIndex": num_rows, # Usamos tu variable dinámica
                "startColumnIndex": 4, 
                "endColumnIndex": 6
            },
            "rule": rule
        }
    })


    sh.batch_update({"requests": requests})


    return 0
