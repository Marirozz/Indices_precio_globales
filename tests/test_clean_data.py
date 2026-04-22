import pandas as pd
import numpy as np
from src.clean_data import (
    rename_column_by_position,
    clean_columns,
    clean_text,
    clean_text_columns,
    replace_zero_with_nan
)

def test_rename_column_by_position(mock_raw_data: pd.DataFrame):
    df = rename_column_by_position(mock_raw_data.copy())
    assert df.columns[1] == 'nombre'
    assert df.columns[2] == 'unidad_de_medida'

def test_clean_columns():
    df = pd.DataFrame(columns=[' Mi Columna ', 'Otra-Columna'])
    df = clean_columns(df)
    assert df.columns[0] == 'mi_columna'
    assert df.columns[1] == 'otra-columna'

def test_clean_text():
    assert clean_text("Hola   Mundo") == "Hola Mundo"
    assert clean_text("Manzana, pera") == "Manzana pera"
    assert clean_text("   Espacios   ") == "Espacios"
    assert pd.isna(clean_text(np.nan))

def test_clean_text_columns(mock_raw_data: pd.DataFrame):
    # Asegurar que las columnas existan usando clean_columns provisional
    df = mock_raw_data.copy()
    df.columns = ['orden', 'nombre', 'unidad_de_medida', 'fecha1', 'fecha2']
    df = clean_text_columns(df, ['nombre', 'unidad_de_medida'])
    
    assert df.loc[0, 'nombre'] == 'Arroz blanco'
    assert df.loc[2, 'nombre'] == 'yuca'

def test_replace_zero_with_nan():
    df = pd.DataFrame({'a': [1, 0, 3], 'b': [0.0, 2.5, 0.0], 'c': ['A', 'B', 'C']})
    df = replace_zero_with_nan(df)
    
    assert pd.isna(df.loc[1, 'a'])
    assert pd.isna(df.loc[0, 'b'])
    assert pd.isna(df.loc[2, 'b'])
    # Las columnas strings no se deben afectar
    assert df.loc[0, 'c'] == 'A'
