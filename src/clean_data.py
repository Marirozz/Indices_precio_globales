"""
Módulo para limpieza y normalización de datos.

Este módulo contiene funciones para:
- Renombrar columnas
- Normalizar nombres de columnas
- Limpiar texto
- Manejar valores nulos o cero
"""
import pandas as pd
import re
from typing import List, Optional

def rename_column_by_position(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renombra columnas específicas por su posición.
    
    Renombra las columnas en posiciones 1 y 2 como 'nombre' y 'unidad_de_medida'.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame a modificar.
    
    Returns
    -------
    pd.DataFrame
        DataFrame con las columnas renombradas.
    """
    cols = df.columns.tolist()

    cols[1] = 'nombre'
    cols[2] = 'unidad_de_medida'

    df.columns = cols
    return df


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza los nombres de las columnas.
    
    Convierte nombres a minúsculas, elimina espacios y 
    los reemplaza con guiones bajos.
    Las columnas de fecha o numéricas se mantienen intactas.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame a modificar.
    
    Returns
    -------
    pd.DataFrame
        DataFrame con nombres de columnas normalizados.
    """
    df.columns = [
        col.strip().lower().replace(' ', '_') if isinstance(col, str) else col
        for col in df.columns
    ]
    return df


def clean_text(text: Optional[str]) -> Optional[str]:
    """
    Limpia y normaliza un texto.
    
    Elimina espacios extra, normaliza comas y maneja valores nulos.
    
    Parameters
    ----------
    text : str or None
        Texto a limpiar.
    
    Returns
    -------
    str or NaN
        Texto limpio o NaN si la entrada era nula.
    """
    if pd.isna(text):
        return text
    text = re.sub(r'([a-zA-ZáéíóúÁÉÍÓÚñÑ]),\s*', r'\1 ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def clean_text_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """
    Aplica limpieza de texto a columnas específicas.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame a modificar.
    columns : list of str
        Nombres de las columnas a limpiar.
    
    Returns
    -------
    pd.DataFrame
        DataFrame con las columnas limpias.
    """
    for col in columns:
        if col in df.columns:
            df[col] = df[col].apply(clean_text)
    return df


def replace_zero_with_nan(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reemplaza valores cero con NaN en columnas numéricas.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame a modificar.
    
    Returns
    -------
    pd.DataFrame
        DataFrame con ceros reemplazados por NaN.
    """
    numeric_cols = list(df.select_dtypes(include='number').columns)
    for col in numeric_cols:
        df[col] = df[col].replace(0, float('nan'))

    return df