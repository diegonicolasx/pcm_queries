

def get_formato_condicional_request(sheet_id: int, num_rows: int) -> dict:


    payload = {
        "requests": [
            # Regla 1: OT en proceso -> AMARILLO
            {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{
                            "sheetId": sheet_id,
                            "startRowIndex": 1,
                            "endRowIndex": num_rows,
                            "startColumnIndex": 4,
                            "endColumnIndex": 6
                        }],
                        "booleanRule": {
                            "condition": {
                                "type": "TEXT_EQ",
                                "values": [{"userEnteredValue": "OT en Proceso"}]
                            },
                            "format": {
                                "textFormat": {
                                    "bold": True,
                                    "foregroundColor": {"red": 0, "green": 0, "blue": 0}
                                },
                                "backgroundColor": {
                                    "red": 1.0, 
                                    "green": 1.0,
                                    "blue": 0.6
                                }
                            }
                        }
                    },
                    "index": 0
                }
            },
            # Regla 2: OT en revisión -> AZUL
            {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{
                            "sheetId": sheet_id,
                            "startRowIndex": 1,
                            "endRowIndex": num_rows,
                            "startColumnIndex": 4,
                            "endColumnIndex": 6
                        }],
                        "booleanRule": {
                            "condition": {
                                "type": "TEXT_EQ",
                                "values": [{"userEnteredValue": "OT en Revisión"}]
                            },
                            "format": {
                                "textFormat": {
                                    "bold": True,
                                    "foregroundColor": {"red": 1, "green": 1, "blue": 1}
                                },
                                "backgroundColor": {
                                    "red": 0.3,
                                    "green": 0.6,
                                    "blue": 1.0
                                }
                            }
                        }
                    },
                    "index": 1
                }
            },
            # Regla 3: OT finalizada -> VERDE
            {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{
                            "sheetId": sheet_id,
                            "startRowIndex": 1,
                            "endRowIndex": num_rows,
                            "startColumnIndex": 4,
                            "endColumnIndex": 6
                        }],
                        "booleanRule": {
                            "condition": {
                                "type": "TEXT_EQ",
                                "values": [{"userEnteredValue": "OT Finalizada"}]
                            },
                            "format": {
                                "textFormat": {
                                    "bold": True,
                                    "foregroundColor": {"red": 0, "green": 0, "blue": 0}
                                },
                                "backgroundColor": {
                                    "red": 0.5,
                                    "green": 0.9,
                                    "blue": 0.5
                                }
                            }
                        }
                    },
                    "index": 2
                }
            }
        ]
    }

    return payload


def get_cell_format(sheet_id: int, num_rows: int) -> dict:


    payload = {
        "requests": [
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
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
            }
        ]
    }

    return payload


def chart_graph(sheet_id: int, equipos: list, start_index: int) -> dict :


    request = []

    for idx, equipo in enumerate(equipos):
        print(equipo)
        equipo_nombre = equipo.split('-')[0]
        
        # Calcular posición del gráfico (2x2 grid)

        
        # Posición en píxeles
        offset_x = 900
        offset_y = 40 + (idx * 420)
        
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
                            "pieHole": 0.4,
                            "domain": {
                                "sourceRange": {
                                    "sources": [{
                                        "sheetId": sheet_id,
                                        "startRowIndex": 0,  # Fila de headers
                                        "endRowIndex": 1,
                                        "startColumnIndex": start_index + 1,  # AB (primera columna de valores)
                                        "endColumnIndex": start_index + 4  # Hasta AD
                                    }]
                                }
                            },
                            "series": {
                                "sourceRange": {
                                    "sources": [{
                                        "sheetId": sheet_id,
                                        "startRowIndex": data_row_index,  # Fila de datos de este equipo
                                        "endRowIndex": data_row_index + 1,
                                        "startColumnIndex": start_index + 1,  # AB
                                        "endColumnIndex": start_index + 4  # Hasta AD
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
                                "sheetId": sheet_id,
                                "rowIndex": 0,
                                "columnIndex": 0
                            },
                            "offsetXPixels": offset_x,
                            "offsetYPixels": offset_y,
                            "widthPixels": 500,
                            "heightPixels": 380

                        }
                    }
                }
            }
        }

        request.append(chart_spec)

    return {"requests":request}