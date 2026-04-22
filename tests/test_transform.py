import pandas as pd
from src.transform import (
    asignar_categoria,
    add_category,
    calculate_average_price,
    standardize_measurement_units
)

def test_asignar_categoria():
    assert asignar_categoria("Arroz blanco selecto") == "Arroz"
    assert asignar_categoria("Muslo pollo pimiento") == "Pollo"
    assert asignar_categoria("Producto Extraño") == "Otro"

def test_add_category(mock_clean_data: pd.DataFrame):
    df = add_category(mock_clean_data.copy())
    assert 'categoria' in df.columns
    assert df.loc[0, 'categoria'] == 'Arroz'

def test_calculate_average_price(mock_clean_data: pd.DataFrame):
    df = calculate_average_price(mock_clean_data.copy())
    assert 'precio_promedio' in df.columns
    # Para el índice 0 (Arroz): 30, 32, 30 -> promedio 30.666
    assert round(df.loc[0, 'precio_promedio'], 2) == 30.67

def test_standardize_measurement_units():
    df = pd.DataFrame({'unidad_de_medida': ['Libras', 'Litros', 'Unidades', 'Otro']})
    df = standardize_measurement_units(df)
    
    assert df.loc[0, 'unidad_de_medida'] == 'Libra'
    assert df.loc[1, 'unidad_de_medida'] == 'Litro'
    assert df.loc[2, 'unidad_de_medida'] == 'Unidad'
    assert df.loc[3, 'unidad_de_medida'] == 'Otro'
