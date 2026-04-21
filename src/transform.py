"""
Módulo para transformación y enriquecimiento de datos.

Este módulo contiene funciones para:
- Categorizar productos
- Calcular promedios de precios
- Estandarizar unidades de medida
- Agregar datos por período (mes)
- Generar resúmenes estadísticos por categoría
- Gestionar valores nulos (NaN) con estrategias CONSERVADORAS
  * 0-15% NaN: Interpolación lineal
  * 15-30% NaN: Forward fill + backward fill
  * >30% NaN: Eliminar (poco confiable)
- Detectar anomalías de precios (cambios sospechosos entre períodos)
"""
import re
import pandas as pd
from typing import List, Dict, Any


categorias = {
    'Aceite': ['Aceite'],
    'Arroz': ['Arroz'],
    'Carne res': ['Carne de res', 'Paleta de res', 'Carne molida'],
    'Carne cerdo': ['Carne de cerdo', 'Paleta de cerdo', 'Pierna de cerdo', 'Patica', 'Chuleta'],
    'Pollo': ['Pollo', 'Muslo pollo'],
    'Legumbres': ['Habichuelas', 'Gandules'],
    'Huevos': ['Huevos'],
    'Pasta': ['Espaguetis'],
    'Embutidos': ['Salami'],
    'Pescado': ['Arenque', 'Bacalao', 'Filete', 'Tilapia', 'Camarones', 'Sardina'],
    'Lacteos': ['Yogurt', 'Queso', 'Margarina'],
    'Leche liquida': ['Leche líquida', 'Leche liquida'],
    'Leche en polvo': ['Leche en polvo'],
    'Pan': ['Pan'],
    'Harina': ['Harina'],
    'Vegetales': ['Auyama', 'Ají', 'Apio', 'Ajo', 'Berenjena', 'Cebolla', 'Tomate', 'Tayota', 'Lechuga', 'Repollo', 'Pepino', 'Zanahoria', 'Remolacha', 'Papa', 'Yuca', 'Yautía', 'Batata', 'Ñame'],
    'Frutas': ['Plátano', 'Guineo', 'Aguacate', 'Coco', 'Chinola', 'Fresa', 'Limón', 'Lechosa', 'Mango', 'Melón', 'Manzana', 'Pera', 'Naranja', 'Uva', 'Sandía', 'Piña', 'Toronja', 'Tamarindo', 'Granadillo', 'Zapote'],
    'Avena': ['Avena'],
    'Azúcar': ['Azúcar'],
    'Café': ['Café'],
    'Chocolate': ['Chocolate'],
    'Pasta de tomate': ['Pasta de tomate'],
    'Vinagre': ['Vinagre'],
    'Sal': ['Sal'],
    'Sopita': ['Sopita']
}


def asignar_categoria(nombre: Any) -> str:
    """
    Asigna una categoría a un producto basado en su nombre.
    
    Parameters
    ----------
    nombre : str
        Nombre del producto.
    
    Returns
    -------
    str
        Categoría asignada o 'Otro' si no coincide.
    """
    nombre_str = str(nombre).strip()
    
    # Obtener la primera palabra
    primera_palabra = nombre_str.split()[0]
    
    # Buscar categoria usando la primera palabra primero
    for categoria, palabras in categorias.items():
        for palabra in palabras:
            if re.search(rf'\b{palabra}\b', primera_palabra, re.IGNORECASE):
                return categoria
    
    # Si la primera palabra no coincide, buscar en el nombre completo
    for categoria, palabras in categorias.items():
        for palabra in palabras:
            if re.search(rf'\b{palabra}\b', nombre_str, re.IGNORECASE):
                return categoria
    return 'Otro'


def add_category(df: pd.DataFrame) -> pd.DataFrame:
    """
    Añade una columna de categoría basada en el nombre del producto.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame con columna 'nombre'.
    
    Returns
    -------
    pd.DataFrame
        DataFrame con nueva columna 'categoria'.
    """
    df['categoria'] = df['nombre'].apply(asignar_categoria)
    return df


def calculate_average_price(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula el precio promedio para cada producto.
    
    Utiliza todas las columnas numéricas (excepto 'orden') 
    como fechas/períodos de precios.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame con columnas de precios.
    
    Returns
    -------
    pd.DataFrame
        DataFrame con nueva columna 'precio_promedio'.
    """
    cols_fecha = df.select_dtypes(include='number').columns

    # opcional: excluir columnas que no son precios
    cols_fecha = [col for col in cols_fecha if col != 'orden']

    df['precio_promedio'] = df[cols_fecha].mean(axis=1)

    return df


measure_singular = {
    'Litros': 'Litro',
    'Libras': 'Libra',
    'Unidades': 'Unidad',
    'Fundas': 'Funda',
    'Latas': 'Lata',
    'Sobres': 'Sobre',
    'Galones': 'Galón',
    'Botellas': 'Botella',
    'Gramos': 'Gramo',
    'Onzas': 'Onza',
    'Envases': 'Envase',
}

def standardize_measurement_units(df: pd.DataFrame) -> pd.DataFrame:

    """
    Estandariza las unidades de medida al singular.
    
    Convierte unidades plurales a su forma singular.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame con columna 'unidad_de_medida'.
    
    Returns
    -------
    pd.DataFrame
        DataFrame con unidades de medida estandarizadas.
    """
    if 'unidad_de_medida' not in df.columns:
        return df
    for plural, singular in measure_singular.items():
        df['unidad_de_medida'] = df['unidad_de_medida'].str.replace(
            plural, 
            singular, 
            case=False, 
            regex=False
        )
    
    # Limpiamos espacios en blanco accidentales
    df['unidad_de_medida'] = df['unidad_de_medida'].str.strip()
    return df


def aggregate_by_month(df: pd.DataFrame, cols_fecha: List[str]) -> pd.DataFrame:
    """
    Agrupa datos por mes-fin, calculando el promedio mensual.
    
    Transforma los datos diarios/periódicos en promedios mensuales,
    reindexando las fechas a formato 'YYYY-MM'.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame con información de productos.
    cols_fecha : list of str
        Nombres de las columnas con fechas/períodos.
    
    Returns
    -------
    pd.DataFrame
        DataFrame con datos agregados por mes.
    """
    cols_info = [col for col in df.columns if col not in cols_fecha]
    df_t = df[cols_fecha].T
    df_t.index = pd.to_datetime(df_t.index, errors='raise')
    df_mensual = df_t.groupby(pd.Grouper(freq='ME')).mean()
    df_mensual = df_mensual.transpose()
    df_mensual.columns = df_mensual.columns.strftime('%Y-%m')
    
    df_final = pd.concat(
        [df[cols_info].reset_index(drop=True), df_mensual.reset_index(drop=True)],
        axis=1
    )
    
    return df_final


def create_category_measurement_summary(df: pd.DataFrame, cols_fecha: List[str]) -> pd.DataFrame:
    """
    Genera un resumen de estadísticas por categoría.
    
    Calcula promedio, mínimo, máximo y cantidad de productos
    por categoría y unidad de medida.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame con columnas 'categoria', 'unidad_de_medida' y 'precio_promedio'.
    cols_fecha : list of str
        Nombres de las columnas con fechas (para cálculo interno).
    
    Returns
    -------
    pd.DataFrame
        DataFrame con estadísticas por categoría, ordenado por promedio descendente.
    """
    resumen = df.groupby(['categoria', 'unidad_de_medida'])['precio_promedio'].agg([
        'mean',
        'min',
        'max',
        'count'
    ]).round(2)
    
    resumen.columns = ['Promedio', 'Mínimo', 'Máximo', 'Cantidad de Productos']
    resumen = resumen.sort_values('Promedio', ascending=False)
    
    return resumen

def create_category_summary(df: pd.DataFrame, cols_fecha: List[str]) -> pd.DataFrame:
    """
    Genera un resumen de estadísticas por categoría.
    
    Calcula promedio, mínimo, máximo y cantidad de productos
    por categoría.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame con columnas 'categoria' y 'precio_promedio'.
    cols_fecha : list of str
        Nombres de las columnas con fechas (para cálculo interno).
    
    Returns
    -------
    pd.DataFrame
        DataFrame con estadísticas por categoría, ordenado por promedio descendente.
    """
    resumen = df.groupby(['categoria'])['precio_promedio'].agg([
        'mean',
        'min',
        'max',
        'count'
    ]).round(2)
    
    resumen.columns = ['Promedio', 'Mínimo', 'Máximo', 'Cantidad de Productos']
    resumen = resumen.sort_values('Promedio', ascending=False)
    
    return resumen


def calculate_null_percentage(df: pd.DataFrame, cols_fecha: List[str]) -> pd.Series:
    """
    Calcula el porcentaje de valores NaN en cada fila.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame con columnas de fecha/precios.
    cols_fecha : list of str
        Nombres de las columnas a analizar.
    
    Returns
    -------
    pd.Series
        Porcentaje de NaN por fila (0-100).
    """
    null_count = df[cols_fecha].isna().sum(axis=1)
    null_percentage = (null_count / len(cols_fecha)) * 100
    return null_percentage


def impute_by_null_threshold(df: pd.DataFrame, cols_fecha: List[str], threshold_low: int = 15, threshold_high: int = 30) -> pd.DataFrame:
    """
    Aplica estrategia de imputación según el porcentaje de NaN.
    
    Estrategia condicional CONSERVADORA:
    - 0-15% NaN: Interpolación lineal (datos confiables)
    - 15-30% NaN: Forward fill + backward fill (datos moderados)
    - >30% NaN: Se recomienda eliminar (datos poco confiables)
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame con columnas de fecha/precios y 'categoria'.
    cols_fecha : list of str
        Nombres de las columnas con fechas/precios.
    threshold_low : int, default 15
        Umbral bajo de porcentaje NaN (por debajo: interpolación).
    threshold_high : int, default 30
        Umbral alto de porcentaje NaN (por encima: considerar eliminar).
    
    Returns
    -------
    pd.DataFrame
        DataFrame con valores imputados según estrategia.
    """
    df = df.copy()
    null_pct = calculate_null_percentage(df, cols_fecha)
    
    for idx in df.index:
        pct = null_pct[idx]
        
        if pct <= threshold_low:
            df.loc[idx, cols_fecha] = df.loc[idx, cols_fecha].interpolate(
                method='linear',
                limit_direction='both'
            )
        
        elif pct <= threshold_high:
            df.loc[idx, cols_fecha] = df.loc[idx, cols_fecha].ffill()
            df.loc[idx, cols_fecha] = df.loc[idx, cols_fecha].bfill()
        
        else:
            pass
    
    return df


def get_null_statistics(df: pd.DataFrame, cols_fecha: List[str]) -> pd.DataFrame:
    """
    Genera estadísticas sobre valores NaN por fila.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame con columnas de fecha/precios.
    cols_fecha : list of str
        Nombres de las columnas a analizar.
    
    Returns
    -------
    pd.DataFrame
        Estadísticas de NaN por fila (índice, porcentaje, conteo).
    """
    null_pct = calculate_null_percentage(df, cols_fecha)
    null_count = df[cols_fecha].isna().sum(axis=1)
    
    stats = pd.DataFrame({
        'nombre': df['nombre'],
        'categoria': df['categoria'],
        'null_count': null_count,
        'null_percentage': null_pct.round(2),
        'valores_validos': len(cols_fecha) - null_count
    })
    
    return stats.sort_values('null_percentage', ascending=False)


def remove_high_null_products(df: pd.DataFrame, cols_fecha: List[str], max_null_pct: int = 30) -> pd.DataFrame:
    """
    Elimina productos con porcentaje de NaN superior al máximo permitido.
    
    Productos con pocos datos históricos pueden distorsionar el análisis,
    por lo que se recomienda eliminarlos para garantizar confiabilidad.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame con columnas de fecha/precios.
    cols_fecha : list of str
        Nombres de las columnas con fechas/precios.
    max_null_pct : int, default 30
        Porcentaje máximo de NaN permitido (por encima se eliminan).
    
    Returns
    -------
    pd.DataFrame
        DataFrame sin productos que superen max_null_pct de NaN.
    """
    df = df.copy()
    null_pct = calculate_null_percentage(df, cols_fecha)
    
    products_to_remove = df[null_pct > max_null_pct][['nombre', 'categoria']]
    
    df_clean = df[null_pct <= max_null_pct].copy()
    
    print(f"\nELIMINACIÓN DE PRODUCTOS CON >{ max_null_pct}% NaN")
    print(f"Productos eliminados: {len(products_to_remove)}")
    print(f"Productos restantes: {len(df_clean)}")
    
    if len(products_to_remove) > 0:
        print("\nProductos eliminados:")
        print(products_to_remove.to_string(index=False))
    
    return df_clean


def detect_price_anomalies(df: pd.DataFrame, cols_fecha: List[str], threshold_pct: int = 40) -> pd.DataFrame:
    """
    Detecta cambios anómalos entre precios consecutivos.
    
    Identifica productos con cambios porcentuales grandes entre períodos,
    lo que indica posibles errores de digitación o datos inconsistentes.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame con columnas de precios y columna 'nombre'.
    cols_fecha : list of str
        Nombres de las columnas con fechas/precios en orden cronológico.
    threshold_pct : int, default 40
        Porcentaje de cambio máximo permitido entre períodos consecutivos.
        Cambios superiores se marcan como anomalías.
    
    Returns
    -------
    pd.DataFrame
        DataFrame con información de anomalías detectadas.
        Columnas: nombre, categoria, periodo1, precio1, periodo2, precio2, cambio_pct
    """
    anomalias = []
    
    for idx, row in df.iterrows():
        nombre = row['nombre']
        unidad_de_medida = row['unidad_de_medida']
        categoria = row['categoria']
        precios = row[cols_fecha].values
        
        for i in range(len(precios) - 1):
            precio_anterior = precios[i]
            precio_siguiente = precios[i + 1]
            
            if pd.isna(precio_anterior) or pd.isna(precio_siguiente):
                continue
            
            if precio_anterior != 0:
                cambio_pct = abs((precio_siguiente - precio_anterior) / precio_anterior) * 100
                
                if cambio_pct > threshold_pct:
                    anomalias.append({
                        'nombre': nombre,
                        'unidad_de_medida': unidad_de_medida,
                        'categoria': categoria,
                        'periodo1': cols_fecha[i],
                        'precio1': round(precio_anterior, 2),
                        'periodo2': cols_fecha[i + 1],
                        'precio2': round(precio_siguiente, 2),
                        'cambio_pct': round(cambio_pct, 2)
                    })
    
    df_anomalias = pd.DataFrame(anomalias)
    
    if len(df_anomalias) > 0:
        df_anomalias = df_anomalias.sort_values('cambio_pct', ascending=False)
    
    return df_anomalias


def get_anomaly_report(df: pd.DataFrame, cols_fecha: List[str], threshold_pct: int = 40) -> Dict[str, Any]:
    """
    Genera un reporte detallado de anomalías de precios.
    
    Identifica productos con cambios sospechosos y posibles errores
    de digitación.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame con columnas de precios.
    cols_fecha : list of str
        Nombres de las columnas con fechas/precios.
    threshold_pct : int, default 40
        Porcentaje de cambio para considerar una anomalía.
    
    Returns
    -------
    dict
        Diccionario con información resumida de anomalías:
        - 'total_anomalias': cantidad total de cambios anómalos
        - 'productos_afectados': cantidad de productos con al menos una anomalía
        - 'anomalias_por_categoria': resumen por categoría
        - 'anomalias_datos': DataFrame con detalles
    """
    anomalias_df = detect_price_anomalies(df, cols_fecha, threshold_pct)
    
    reporte = {
        'total_anomalias': len(anomalias_df),
        'productos_afectados': anomalias_df['nombre'].nunique() if len(anomalias_df) > 0 else 0,
        'anomalias_datos': anomalias_df
    }
    
    if len(anomalias_df) > 0:
        reporte['anomalias_por_categoria'] = anomalias_df.groupby('categoria').size().sort_values(ascending=False)
    else:
        reporte['anomalias_por_categoria'] = pd.Series()
    
    return reporte


def detect_outliers_iqr(df: pd.DataFrame, cols_fecha: List[str]) -> pd.DataFrame:
    """
    Detecta outliers usando el método IQR (Rango Intercuartil).
    
    Identifica valores anormalmente altos o bajos:
    - Límite inferior = Q1 - 1.5 * IQR
    - Límite superior = Q3 + 1.5 * IQR
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame con columna de precios y opcionalmente 'categoria'.
    col_price : str, default 'precio_promedio'
        Nombre de la columna con precios a analizar.
    method : str, default 'by_category'
        'by_category': detecta outliers dentro de cada categoría
        'global': detecta outliers en todo el dataset
    
    Returns
    -------
    pd.DataFrame
        DataFrame con información de outliers detectados.
        Columnas: nombre, categoria, precio, tipo_outlier, limite_inferior, limite_superior
    """
    outliers = []
    
    for idx, row in df.iterrows():
        precios = row[cols_fecha].dropna()
        
        if len(precios) < 4:
            continue
            
        Q1 = precios.quantile(0.25)
        Q3 = precios.quantile(0.75)
        IQR = Q3 - Q1
        
        limite_inferior = Q1 - 1.5 * IQR
        limite_superior = Q3 + 1.5 * IQR
        
        for col in cols_fecha:
            precio = row[col]
            if pd.isna(precio):
                continue
            
            if precio < limite_inferior or precio > limite_superior:
                tipo = 'Bajo' if precio < limite_inferior else 'Alto'
                outliers.append({
                    'nombre': row['nombre'],
                    'unidad_de_medida': row['unidad_de_medida'],
                    'categoria': row['categoria'],
                    'periodo': col,
                    'precio': round(precio, 2),
                    'tipo_outlier': tipo,
                    'limite_inferior': round(limite_inferior, 2),
                    'limite_superior': round(limite_superior, 2)
                })
    
    return pd.DataFrame(outliers)


def detect_outliers_zscore(df: pd.DataFrame, cols_fecha: List[str], threshold: int = 3) -> pd.DataFrame:
    """
    Detecta outliers usando Z-Score.
    
    Z-Score mide cuántas desviaciones estándar está un valor de la media.
    |Z| > threshold se considera outlier.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame con columna de precios y opcionalmente 'categoria'.
    cols_fecha : list of str
        Nombres de las columnas con fechas/precios.
    threshold : int, default 3
        Umbral de Z-Score (|Z| > threshold = outlier)
        Típicamente: 2 = moderado, 3 = severo
    
    Returns
    -------
    pd.DataFrame
        DataFrame con información de outliers detectados.
        Columnas: nombre, categoria, precio, z_score, media_historica, desviacion_std
    """
    outliers = []
    
    for idx, row in df.iterrows():
        precios = row[cols_fecha].dropna()
        
        if len(precios) < 3:
            continue
        
        media = precios.mean()
        std = precios.std()
        
        if std == 0:
            continue
        
        for col in cols_fecha:
            precio = row[col]
            if pd.isna(precio):
                continue
            
            z_score = (precio - media) / std
            
            if abs(z_score) > threshold:
                outliers.append({
                    'nombre': row['nombre'],
                    'unidad_de_medida': row['unidad_de_medida'],
                    'categoria': row['categoria'],
                    'periodo': col,
                    'precio': round(precio, 2),
                    'z_score': round(z_score, 2),
                    'media_historica': round(media, 2),
                    'desviacion_std': round(std, 2)
                })
    
    return pd.DataFrame(outliers)


def get_outlier_summary(df: pd.DataFrame, cols_fecha: List[str]) -> Dict[str, Any]:
    """
    Genera un resumen comparativo de outliers con ambos métodos.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame con columna de precios.
    cols_fecha : list of str
        Nombres de las columnas con fechas/precios.
    
    Returns
    -------
    dict
        Diccionario con resumen de ambos métodos y visualización.
    """
    outliers_iqr = detect_outliers_iqr(df, cols_fecha)
    outliers_zscore = detect_outliers_zscore(df, cols_fecha)
    
    resumen = {
        'outliers_iqr_count': len(outliers_iqr),
        'outliers_zscore_count': len(outliers_zscore),
        'outliers_iqr': outliers_iqr,
        'outliers_zscore': outliers_zscore,
        'productos_totales': len(df),
        'porcentaje_iqr': round((len(outliers_iqr) / len(df)) * 100, 2),
        'porcentaje_zscore': round((len(outliers_zscore) / len(df)) * 100, 2)
    }
    
    return resumen
