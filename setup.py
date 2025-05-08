from setuptools import setup, find_packages

setup(
    name='extra',               # Nombre del paquete
    version='0.1.0',            # Versión
    packages=find_packages(),   # Detecta automáticamente la carpeta 'extra'
    install_requires=[],        # Aquí van las dependencias si las hay
)