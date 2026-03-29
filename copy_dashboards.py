import shutil
import os

src = 'Dashboards'
dst = os.path.join('Portal', 'Dashboards')

if not os.path.exists(dst):
    shutil.copytree(src, dst)
    print("Directorio copiado exitosamente.")
else:
    print("El directorio ya existe.")
