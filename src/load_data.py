"""
Módulo para cargar datos desde archivos Excel.

Este módulo proporciona funciones para leer archivos Excel
y convertirlos en DataFrames de Pandas para su análisis.
"""
import pandas as pd
import pandera.pandas as pa

# Esquema para validacion inicial opcional
schema = pa.DataFrameSchema(coerce=False)

def load_data(path: str) -> pd.DataFrame:
    """
    Carga datos desde un archivo Excel.
    
    Parameters
    ----------
    path : str
        Ruta del archivo Excel a cargar.
    
    Returns
    -------
    pd.DataFrame
        DataFrame con los datos del archivo Excel.
    """
    df = pd.read_excel(path)
    return schema.validate(df)