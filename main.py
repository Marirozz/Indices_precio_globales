"""
Pipeline principal para análisis de índices de precios.

Este script ejecuta el flujo completo de procesamiento de datos utilizando
logs y dependencias extraídas del archivo de configuración.
"""

import sys
import yaml
from pathlib import Path

# Importar logger
from src.logger import get_logger

# Importar funciones del módulo src
from src.load_data import load_data
from src.clean_data import (
    rename_column_by_position,
    clean_columns,
    clean_text_columns,
    replace_zero_with_nan
)
from src.transform import (
    add_category,
    standardize_measurement_units,
    calculate_average_price,
    aggregate_by_month,
    create_category_measurement_summary,
    create_category_summary,
    get_null_statistics,
    impute_by_null_threshold,
    remove_high_null_products,
    get_anomaly_report,
    get_outlier_summary
)


def main():
    """Ejecuta el pipeline completo de análisis de precios."""
    logger = get_logger("pipeline")
    
    logger.info("=" * 60)
    logger.info("PIPELINE DE ANÁLISIS DE ÍNDICES DE PRECIOS")
    logger.info("=" * 60)
    
    # 0. CONFIGURACIÓN
    config_path = "config.yaml"
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
            logger.info("✓ Configuración cargada.")
    except Exception as e:
        logger.error(f"✗ Error al cargar {config_path}: {e}")
        return
        
    raw_data_path = config.get("data", {}).get("raw", "data/raw/Monitoreo_Precios_Promedios_Globales_RD.xlsx")
    output_dir = Path(config.get("data", {}).get("processed_dir", "data/processed"))
    
    # 1. CARGAR DATOS
    logger.info("[1/8] Cargando datos...")
    try:
        df = load_data(raw_data_path)
        logger.info(f"✓ Datos cargados: {df.shape[0]} productos, {df.shape[1]} columnas")
    except Exception as e:
        logger.error(f"✗ Error al cargar datos desde {raw_data_path}: {e}")
        return
    
    # 2. RENOMBRAR COLUMNAS
    logger.info("[2/8] Renombrando columnas...")
    df = rename_column_by_position(df)
    df = clean_columns(df)
    logger.info("✓ Columnas normalizadas")
    
    # 3. LIMPIAR TEXTO
    logger.info("[3/8] Limpiando texto en columnas...")
    df = clean_text_columns(df, ['nombre', 'unidad_de_medida'])
    logger.info("✓ Texto limpio")
    
    # 4. REEMPLAZAR CEROS CON NaN
    logger.info("[4/8] Reemplazando ceros con NaN...")
    initial_zeros = (df.select_dtypes(include='number') == 0).sum().sum()
    df = replace_zero_with_nan(df)
    logger.info(f"✓ {initial_zeros} ceros reemplazados con NaN")
    
    # 5. ESTANDARIZAR UNIDADES DE MEDIDA
    logger.info("[5/8] Estandarizando unidades de medida...")
    df = standardize_measurement_units(df)
    logger.info("✓ Unidades estandarizadas a forma singular")
    
    # 6. CATEGORIZAR PRODUCTOS
    logger.info("[6/8] Categorizando productos...")
    df = add_category(df)
    categorias_unicas = df['categoria'].nunique()
    logger.info(f"✓ Productos categorizados en {categorias_unicas} categorías")
    
    # 7. ANALIZAR Y IMPUTAR NaN
    logger.info("[7/8] Analizando y tratando valores nulos...")
    cols_fecha = [col for col in df.select_dtypes(include='number').columns if col != 'orden']
    
    null_stats = get_null_statistics(df, cols_fecha)
    logger.info(f"Estadísticas de valores nulos calculadas. Top nulos: {null_stats.head(2).to_dict('records')}")
    
    thresh_low = config.get("imputation", {}).get("threshold_low", 15)
    thresh_high = config.get("imputation", {}).get("threshold_high", 30)
    
    df = impute_by_null_threshold(df, cols_fecha, threshold_low=thresh_low, threshold_high=thresh_high)
    logger.info(f"✓ Valores imputados exitosamente (umbrales {thresh_low}-{thresh_high}%)")
    
    #Eliminar productos con >X% NaN (no confiables)
    df = remove_high_null_products(df, cols_fecha, max_null_pct=thresh_high)
    logger.info("✓ Depuración completada")
    

    #8. DETECTAR ANOMALÍAS DE PRECIOS
    logger.info("-" * 60)
    logger.info("DETECCIÓN DE ANOMALÍAS DE PRECIOS")
    logger.info("-" * 60)
    anom_thresh = config.get("anomalies", {}).get("threshold_pct", 40)
   
    reporte_anomalias = get_anomaly_report(df, cols_fecha, threshold_pct=anom_thresh)
    df_anom = reporte_anomalias['anomalias_datos']

    logger.info(f"Total de anomalías detectadas: {reporte_anomalias['total_anomalias']}")
    logger.info(f"Productos afectados: {reporte_anomalias['productos_afectados']}")

    if not df_anom.empty:
        df_anom.to_csv(output_dir / 'anomalias_detectadas.csv', index=False, encoding='utf-8-sig')
    
        df_anom['id_unico'] = df_anom['nombre'].astype(str) + " | " + df_anom['unidad_de_medida'].astype(str)
        ids_con_error = df_anom['id_unico'].unique()
    
        df['id_temporal'] = df['nombre'].astype(str) + " | " + df['unidad_de_medida'].astype(str)
    
        df_limpio = df[~df['id_temporal'].isin(ids_con_error)].copy()
    
        df_limpio = df_limpio.drop(columns=['id_temporal'])
        df = df_limpio 
    
        logger.warning(f"Se detectaron {reporte_anomalias['total_anomalias']} variaciones sospechosas.")
        logger.warning(f"Se excluyeron {len(ids_con_error)} combinaciones de Producto+Unidad.")
        logger.info(f"El pipeline continuará con {len(df)} registros validados.")
    else:
        logger.info("No se detectaron anomalías significativas.")


    # 9. CALCULAR ESTADÍSTICAS Y EXPORTAR
    logger.info("[9/9] Calculando estadísticas finales...")
    
    df = calculate_average_price(df)
    df_mensual = aggregate_by_month(df, cols_fecha)
    resumen_categoria_medida = create_category_measurement_summary(df, cols_fecha)
    resumen_categoria = create_category_summary(df, cols_fecha)
    
    logger.info("✓ Estadísticas calculadas")
    
    # 10.EXPORTAR RESULTADOS
    logger.info("=" * 60)
    logger.info("RESULTADOS FINALES")
    logger.info("=" * 60)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    df.to_csv(output_dir / 'data_limpia.csv', index=False, encoding='utf-8-sig')
    logger.info("✓ Datos limpios guardados: data/processed/data_limpia.csv")
    
    df_mensual.to_csv(output_dir / 'data_mensual.csv', index=False, encoding='utf-8-sig')
    logger.info("✓ Datos mensuales guardados: data/processed/data_mensual.csv")
    
    resumen_categoria_medida.to_csv(output_dir / 'resumen_categoria_medida.csv', encoding='utf-8-sig')
    logger.info("✓ Resumen por categoría y medida guardado: data/processed/resumen_categoria_medida.csv")

    resumen_categoria.to_csv(output_dir / 'resumen_categoria.csv', encoding='utf-8-sig')
    logger.info("✓ Resumen por categoría guardado: data/processed/resumen_categoria.csv")


        
 # DETECCIÓN DE DATOS ATÍPICOS (OUTLIERS)
    logger.info("-" * 60)
    logger.info("DETECCIÓN DE DATOS ATÍPICOS (OUTLIERS)")
    logger.info("-" * 60)
    
    outliers_resumen = get_outlier_summary(df, cols_fecha) 
    
    logger.info(f"Outliers IQR: {outliers_resumen['outliers_iqr_count']} ({outliers_resumen['porcentaje_iqr']}%)")
    logger.info(f"Outliers Z-Score: {outliers_resumen['outliers_zscore_count']} ({outliers_resumen['porcentaje_zscore']}%)")
    
    if len(outliers_resumen['outliers_iqr']) > 0:
        outliers_resumen['outliers_iqr'].to_csv(
            output_dir / 'outliers_iqr.csv', 
            index=False, 
            encoding='utf-8-sig'
        )
        logger.info("✓ Detalle de Outliers (IQR) guardado")
    
    if len(outliers_resumen['outliers_zscore']) > 0:
        outliers_resumen['outliers_zscore'].to_csv(
            output_dir / 'outliers_zscore.csv', 
            index=False, 
            encoding='utf-8-sig'
        )
        logger.info("✓ Detalle de Outliers (Z-Score) guardado")
    
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETADO EXITOSAMENTE")
    logger.info("=" * 60)
    
    return df, df_mensual, resumen_categoria


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        sys.stderr.write(f"Error fatal en la inicialización: {e}\n")
        sys.exit(1)
