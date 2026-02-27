import gspread
import polars as pl
import os
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from typing import Dict

load_dotenv()

COLUMN_HEADER_DISPLAY = {
    "OT LAO": "OT LAO\n(corte - lavado)",
}


def print_google_sheet(df:pl.DataFrame, resumen_ot:Dict[str, Dict[str, int]], year:str, month:str, test:bool)->None:
    SHEET_KEY = os.getenv("SHEET_KEY")

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
    # Traducir nombres de columnas al texto de display (con \n si aplica)
    display_headers = [COLUMN_HEADER_DISPLAY.get(col, col) for col in df.columns]
    data = [display_headers] + df.to_numpy().tolist()
    worksheet.update('A1', data)

     # === FORMATO ===
    
    # 1. Formato de encabezados (fila 1)
    #num_cols = len(df.columns)
    num_rows = len(df) + 1  # +1 por el encabezado

    header_format = {
        "backgroundColor": {"red": 0.204, "green": 0.659, "blue": 0.325}, #Verde
        "textFormat": {
            "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},  # Blanco
            "fontSize": 10,
            "bold": True
        },
        "horizontalAlignment": "CENTER",
        "verticalAlignment": "MIDDLE"
    }
    
    worksheet.format('A1:B1', header_format)
    worksheet.format('H1:L1', header_format)

    header_format.update({"backgroundColor": {"red": 1.0, "green": 0.6, "blue": 0.0}})  # Naranjo

    worksheet.format('C1:G1', header_format)

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
                    "endColumnIndex": 12  # Hasta columna J
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.851, "green": 0.886, "blue": 0.953},
                        "borders": {
                            "top": {"style": "SOLID", "width": 1, "color": {"red": 0.6, "green": 0.6, "blue": 0.6}},
                            "bottom": {"style": "SOLID", "width": 1, "color": {"red": 0.6, "green": 0.6, "blue": 0.6}},
                            "left": {"style": "SOLID", "width": 1, "color": {"red": 0.6, "green": 0.6, "blue": 0.6}},
                            "right": {"style": "SOLID", "width": 1, "color": {"red": 0.6, "green": 0.6, "blue": 0.6}}
                        }
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,borders)"
            }
        }]
        
        sh.batch_update({"requests": requests})

    
     # 3. Crear validación de datos (dropdown) con colores para columnas de Status
    verde_ot_finalizada = {"red": 0.831, "green": 0.929, "blue": 0.737}
    amarillo_ot_proceso = {"red": 1.0, "green": 0.725, "blue": 0.0}
    azul_ot_revision = {"red": 0.039, "green": 0.325, "blue": 0.659}
    
    status_columns_indices = [7, 8, 9, 10, 11]  # H, I, J, K, L (índices 0-based)
    
    # Obtener valores actuales para identificar celdas con contenido
    cells = worksheet.get('H2:L' + str(num_rows))
    
    validation_requests = []
    conditional_format_requests = []
    
    for col_idx in status_columns_indices:
        for row_idx in range(1, num_rows):  # Desde fila 2 (índice 1)
            # Verificar si la celda tiene contenido
            data_row_idx = row_idx - 1
            data_col_idx = col_idx - 7
            
            cell_value = ""
            if data_row_idx < len(cells) and data_col_idx < len(cells[data_row_idx]):
                cell_value = str(cells[data_row_idx][data_col_idx]).strip() if cells[data_row_idx][data_col_idx] else ""
            
            # Solo agregar validación si la celda tiene contenido
            if cell_value:
                # Agregar validación con dropdown
                validation_requests.append({
                    "setDataValidation": {
                        "range": {
                            "sheetId": worksheet.id,
                            "startRowIndex": row_idx,
                            "endRowIndex": row_idx + 1,
                            "startColumnIndex": col_idx,
                            "endColumnIndex": col_idx + 1
                        },
                        "rule": {
                            "condition": {
                                "type": "ONE_OF_LIST",
                                "values": [
                                    {"userEnteredValue": "OT Finalizada"},
                                    {"userEnteredValue": "OT en Proceso"},
                                    {"userEnteredValue": "OT en Revisión"}
                                ]
                            },
                            "showCustomUi": True,
                            "strict": True
                        }
                    }
                })
                
                # Agregar reglas de formato condicional para esta celda
                # Regla 1: OT Finalizada = Verde
                conditional_format_requests.append({
                    "addConditionalFormatRule": {
                        "rule": {
                            "ranges": [{
                                "sheetId": worksheet.id,
                                "startRowIndex": row_idx,
                                "endRowIndex": row_idx + 1,
                                "startColumnIndex": col_idx,
                                "endColumnIndex": col_idx + 1
                            }],
                            "booleanRule": {
                                "condition": {
                                    "type": "TEXT_EQ",
                                    "values": [{"userEnteredValue": "OT Finalizada"}]
                                },
                                "format": {
                                    "backgroundColor": verde_ot_finalizada,
                                    "textFormat": {
                                        "foregroundColor": {"red": 0.0, "green": 0.5, "blue": 0.0},
                                        "bold": True
                                    }
                                }
                            }
                        },
                        "index": 0
                    }
                })
                
                # Regla 2: OT en Proceso = Amarillo
                conditional_format_requests.append({
                    "addConditionalFormatRule": {
                        "rule": {
                            "ranges": [{
                                "sheetId": worksheet.id,
                                "startRowIndex": row_idx,
                                "endRowIndex": row_idx + 1,
                                "startColumnIndex": col_idx,
                                "endColumnIndex": col_idx + 1
                            }],
                            "booleanRule": {
                                "condition": {
                                    "type": "TEXT_CONTAINS",
                                    "values": [{"userEnteredValue": "OT en Proceso"}]
                                },
                                "format": {
                                    "backgroundColor": amarillo_ot_proceso,
                                    "textFormat": {
                                        "foregroundColor": {"red": 0.6, "green": 0.4, "blue": 0.0},
                                        "bold": True
                                    }
                                }
                            }
                        },
                        "index": 0
                    }
                })
                
                # Regla 3: OT en Revisión = Azul
                conditional_format_requests.append({
                    "addConditionalFormatRule": {
                        "rule": {
                            "ranges": [{
                                "sheetId": worksheet.id,
                                "startRowIndex": row_idx,
                                "endRowIndex": row_idx + 1,
                                "startColumnIndex": col_idx,
                                "endColumnIndex": col_idx + 1
                            }],
                            "booleanRule": {
                                "condition": {
                                    "type": "TEXT_CONTAINS",
                                    "values": [{"userEnteredValue": "OT en Revisión"}]
                                },
                                "format": {
                                    "backgroundColor": azul_ot_revision,
                                    "textFormat": {
                                        "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},  # Blanco,
                                        "bold": True
                                    }
                                }
                            }
                        },
                        "index": 0
                    }
                })
    
    # Ejecutar todas las solicitudes
    all_requests = validation_requests + conditional_format_requests
    if all_requests:
        sh.batch_update({"requests": all_requests})
    
    # 4. Ajustar ancho de columnas
    # Obtener valores de la hoja
    all_values = worksheet.get_all_values()

    requests = []
    
    for col_index in range(len(all_values[0])):  # Para cada columna
        # Encontrar el texto más largo en la columna        
        max_length = 0        
        for row in all_values:
            cell_length = max(len(line) for line in str(row[col_index]).split("\n"))
            max_length = max(max_length, cell_length)
    
        # Calcular ancho (aproximado: 10 píxeles por carácter + padding)        
        pixel_size = min(max_length * 10, 400)  # Máximo 400 píxeles

        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": worksheet.id,
                    "dimension": "COLUMNS",
                    "startIndex": col_index,
                    "endIndex": col_index + 1
                },
                "properties": {
                    "pixelSize": pixel_size
                },
                "fields": "pixelSize"
            }
        })

    sh.batch_update({"requests": requests})
    
    print(f"\n✅ Hoja '{sheet_name}' creada y formateada exitosamente.")

    print("\nCreación de los gráficos.")

    """
    Crea 4 gráficos de dona (uno por equipo) usando datos ocultos
    """
    
    # === 1. PREPARAR DATOS EN COLUMNAS OCULTAS ===
    # Usaremos columnas muy a la derecha (por ejemplo, columnas AA en adelante)
    start_col_index = 26  # Columna AA (0-based: A=0, B=1, ..., Z=25, AA=26)    

    equipos = list(resumen_ot.keys())
    estados = ['OT en Proceso', 'OT en Revisión', 'OT Finalizada']  # Sin "Sin estado" si no quieres mostrarlo
    
    # Preparar datos
    headers = ['Equipo'] + estados
    data = [headers]    
    
    for equipo in equipos:
        equipo_nombre = equipo.split('-')[0]  # MTM, MTB, OPE, SSMA, LAO
        fila = [equipo_nombre]
        for estado in estados:
            fila.append(resumen_ot[equipo].get(estado, 0))
        data.append(fila)
    
    # Escribir datos en columna AA (oculta)
    worksheet.update('AA1', data)
    
    #Calcular posición vertical (debajo de la tabla)
    requests = []

    offset_y_base = (num_rows + 3) * 21
    
    for idx, equipo in enumerate(equipos):
        equipo_nombre = equipo.split('-')[0]
        
        # Calcular posición del gráfico (2x2 grid)
        row = idx // 2
        col = idx % 2
        
        # Posición en píxeles
        offset_x = 20 + (col * 620)
        offset_y = offset_y_base + (row * 480)
        
        # Fila de datos para este equipo (0-based)
        data_row_index = idx + 1  # Header en fila 0, datos empiezan en fila 1
        
        # Crear especificación del gráfico
        chart_spec = {
            "addChart": {
                "chart": {
                    "spec": {
                        "title": f"Status Mensuales {equipo_nombre}",
                        "titleTextFormat": {
                            "fontSize": 18,
                            "bold": True
                        },
                        "pieChart": {
                            "legendPosition": "BOTTOM_LEGEND",
                            "pieHole": 0.5,
                            "domain": {
                                "sourceRange": {
                                    "sources": [{
                                        "sheetId": worksheet.id,
                                        "startRowIndex": 0,  # Fila de headers
                                        "endRowIndex": 1,
                                        "startColumnIndex": start_col_index + 1,  # AB (primera columna de valores)
                                        "endColumnIndex": start_col_index + 4  # Hasta AD
                                    }]
                                }
                            },
                            "series": {
                                "sourceRange": {
                                    "sources": [{
                                        "sheetId": worksheet.id,
                                        "startRowIndex": data_row_index,  # Fila de datos de este equipo
                                        "endRowIndex": data_row_index + 1,
                                        "startColumnIndex": start_col_index + 1,  # AB
                                        "endColumnIndex": start_col_index + 4  # Hasta AD
                                    }]
                                }
                            },
                            "threeDimensional": True
                        },
                        "fontName": "Arial"
                    },
                    "position": {
                        "overlayPosition": {
                            "anchorCell": {
                                "sheetId": worksheet.id,
                                "rowIndex": 0,
                                "columnIndex": 0
                            },
                            "offsetXPixels": offset_x,
                            "offsetYPixels": offset_y,
                            "widthPixels": 600,
                            "heightPixels": 450
                        }
                    }
                }
            }
        }

        requests.append(chart_spec)            
    
    # === 4. EJECUTAR TODAS LAS SOLICITUDES ===
    sh.batch_update({"requests": requests})
    
    print(f"✅ {len(equipos)} gráficos de dona creados exitosamente debajo de la tabla.")    

def test()->None:
    sheet_name = "DYR test"
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file('key/credentials.json', scopes=scope)
    gc = gspread.authorize(creds)
    SHEET_KEY = os.getenv("SHEET_KEY")
    sh = gc.open_by_key(SHEET_KEY)
    sheets = sh.worksheets()    
    if sheet_name not in [sheet.title for sheet in sheets]:
        print("\nNo existe la hoja de cálculo, se creará una nueva.")
        sh.add_worksheet(title=sheet_name, rows="100", cols="20")
    else:
        print("\nLa hoja de cálculo ya existe, se eliminará y se creará una nueva.")
        sh.del_worksheet(sh.worksheet(sheet_name))
        sh.add_worksheet(title=sheet_name, rows="100", cols="20")

if __name__ == "__main__":
    test()    