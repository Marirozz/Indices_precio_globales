import pytest
import pandas as pd
import numpy as np

@pytest.fixture
def mock_raw_data() -> pd.DataFrame:
    """Fixture que provee un dataframe de datos crudos simulados."""
    return pd.DataFrame({
        'Orden': [1, 2, 3],
        ' Nombre ': ['Arroz blanco ', 'Pollo entero', '  yuca  '],
        'Unidad De Medida': ['Libras', 'Libras', 'Libras'],
        '2021-01-01': [30.5, 60.0, np.nan],
        '2021-02-01': [31.0, 65.0, 15.0]
    })

@pytest.fixture
def mock_clean_data() -> pd.DataFrame:
    """Fixture que provee un dataframe con datos limpios para tests de transform."""
    return pd.DataFrame({
        'nombre': ['Arroz', 'Pollo', 'Yuca'],
        'unidad_de_medida': ['Libra', 'Libra', 'Libra'],
        '2021-01-01': [30.0, 60.0, 15.0],
        '2021-02-01': [32.0, 65.0, 16.0],
        '2021-03-01': [30.0, np.nan, 15.5]
    })
