@echo off
REM One-shot Windows build: creates a venv, installs deps, runs PyInstaller.
setlocal enableextensions

cd /d "%~dp0.."
if not exist .venv (
    python -m venv .venv || goto :err
)
call .venv\Scripts\activate.bat || goto :err
python -m pip install --upgrade pip || goto :err
python -m pip install -r requirements.txt || goto :err
python -m pip install pyinstaller || goto :err
python build\build_windows.py %* || goto :err

echo.
echo [OK] dist\DuplicatorSearchDestroy\DuplicatorSearchDestroy.exe
endlocal
exit /b 0

:err
echo.
echo [FAIL] Build failed.
endlocal
exit /b 1
