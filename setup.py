import sys
from cx_Freeze import setup, Executable

build_exe_options = {"packages": ["numpy"]}

setup(
    name="PostGIStoEPANET",
    version="1.0",
    description="PostGIStoEPANET.",
    options={"build_exe": build_exe_options},
    executables=[Executable(script=r"PostGIStoEPANET.py", base = "Win32GUI")])
