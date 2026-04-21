# Análisis de Índices de Precios Globales

Análisis de datos de índices de precios promedio en República Dominicana. Este proyecto procesa, limpia y analiza datos de precios de productos básicos recopilados por organismos de vigilancia.

## 📋 Descripción

Este proyecto contiene un pipeline de procesamiento de datos que:
- Carga datos de precios desde archivos Excel
- Limpia y normaliza los datos
- Categoriza productos
- Calcula promedios de precios
- Estandariza unidades de medida
- Genera análisis y visualizaciones

## 📁 Estructura del Proyecto

```
Indices_precio_globales/
├── src/                          # Código fuente del proyecto
│   ├── __init__.py              # Inicializador del paquete
│   ├── load_data.py             # Módulo para cargar datos
│   ├── clean_data.py            # Módulo para limpiar datos
│   └── transform.py             # Módulo para transformar datos
├── Notebook/                     # Notebooks de Jupyter
│   ├── 01_exploracion.ipynb     # Exploración inicial de datos
│   ├── 02_limpieza.ipynb        # Limpieza de datos
│   └── Analisys-precios.ipynb   # Análisis y visualizaciones
├── data/
│   ├── raw/                      # Datos sin procesar
│   └── processed/                # Datos procesados
├── pyproject.toml               # Configuración del proyecto
├── Requirements.txt             # Dependencias del proyecto
├── LICENSE                      # Licencia del proyecto
└── README.md                    # Este archivo
```

## 🔧 Dependencias

- **pandas** (>=1.3.0) - Manipulación y análisis de datos
- **numpy** (>=1.20.0) - Computación numérica
- **openpyxl** (>=3.0.0) - Lectura de archivos Excel
- **jupyter** (>=1.0.0) - Notebooks interactivos (desarrollo)
- **matplotlib** o **plotly** - Visualización de datos

## 🚀 Instalación

### 1. Clonar el repositorio
```bash
git clone https://github.com/tu-usuario/indices-precio-globales.git
cd Indices_precio_globales
```

### 2. Crear un virtual environment
```bash
python -m venv venv

# En Windows
venv\Scripts\activate

# En macOS/Linux
source venv/bin/activate
```

### 3. Instalar dependencias
```bash
pip install -r Requirements.txt
```

## 📊 Uso

### Ejecutar el pipeline de limpieza
```python
from src.load_data import load_data
from src.clean_data import rename_column_by_position, clean_text_columns, replace_zero_with_nan
from src.transform import add_category, calculate_average_price, standardize_measurement_units

# Cargar datos
df = load_data('data/raw/Monitoreo_Precios_Promedios_Globales_RD.xlsx')

# Limpiar
df = rename_column_by_position(df)
df = clean_text_columns(df, ['nombre', 'unidad_de_medida'])
df = replace_zero_with_nan(df)

# Transformar
df = standardize_measurement_units(df)
df = add_category(df)
df = calculate_average_price(df)

# Guardar
df.to_csv('data/processed/data_limpia.csv', index=False)
```

### Usar los notebooks
1. Abrir Jupyter Lab/Notebook:
   ```bash
   jupyter lab
   ```
2. Navegar a `Notebook/` y ejecutar los notebooks en orden:
   - `01_exploracion.ipynb` - Análisis exploratorio
   - `02_limpieza.ipynb` - Limpieza de datos
   - `Analisys-precios.ipynb` - Análisis y gráficos

## 📚 Módulos

### `load_data.py`
- `load_data(path)` - Lee archivos Excel y retorna un DataFrame

### `clean_data.py`
- `rename_column_by_position(df)` - Renombra columnas por posición
- `clean_columns(df)` - Normaliza nombres de columnas
- `clean_text(text)` - Limpia texto (espacios, caracteres especiales)
- `clean_text_columns(df, columns)` - Aplica limpieza de texto a columnas específicas
- `replace_zero_with_nan(df)` - Reemplaza ceros con NaN en columnas numéricas

### `transform.py`
- `add_category(df)` - Asigna categorías de productos
- `calculate_average_price(df)` - Calcula precio promedio por producto
- `standardize_measurement_units(df)` - Estandariza unidades de medida

## 📝 Contribuciones

Las contribuciones son bienvenidas. Por favor:
1. Fork el repositorio
2. Crea una rama para tu feature (`git checkout -b feature/AmazingFeature`)
3. Commit tus cambios (`git commit -m 'Add some AmazingFeature'`)
4. Push a la rama (`git push origin feature/AmazingFeature`)
5. Abre un Pull Request

## 📄 Licencia

Este proyecto está licenciado bajo la Licencia MIT - ver el archivo [LICENSE](LICENSE) para más detalles.

## ✉️ Contacto

Mariana Rodriguez - [@Marirozz](https://github.com/Marirozz)

## 🙏 Agradecimientos

- Datos proporcionados por organismos de vigilancia de precios en República Dominicana
