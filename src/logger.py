import logging
import sys

def get_logger(name: str = "pipeline") -> logging.Logger:
    """Configura y retorna el logger estándar para el proyecto."""
    logger = logging.getLogger(name)
    
    # Prevenir que el logger se duplique si se llama múltiples veces
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        # Formato de los logs
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Handler para la consola
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
    return logger
