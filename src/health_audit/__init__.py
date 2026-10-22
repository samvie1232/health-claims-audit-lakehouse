"""Reglas de auditoría de facturación en salud (Silver y Gold) como funciones puras sobre DataFrames.

Los notebooks de Databricks leen y escriben tablas; este paquete solo transforma DataFrames.
Así la misma lógica corre en Databricks y en los tests con Spark local.
"""

__version__ = "1.1.0"
