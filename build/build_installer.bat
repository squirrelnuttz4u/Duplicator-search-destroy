@echo off
REM ========================================================================
REM  Build the PyInstaller --onedir app, then wrap it in an Inno Setup .exe.
REM
REM  Requires:
REM    * Python 3.11+ on PATH
REM    * Inno Setup 6 installed (ISCC.exe). Default install paths we look up:
REM        C:\Program Files (x86)\Inno Setup 6\ISCC.exe
REM        C:\Program Files\Inno Setup 6\ISCC.exe
REM      Override with:  set ISCC=C:\path\to\ISCC.exe
REM
REM  Produces:
REM    dist\DuplicatorSearchDestroy\            (onedir app)
REM    dist\installer\DuplicatorSearchDestroy-Setup-<version>.exe
REM ========================================================================
setlocal enableextensions

cd /d "%~dp0.."

REM ---- Step 1: build the app ---------------------------------------------
call build\build_windows.bat || goto :err

REM ---- Step 2: locate ISCC.exe -------------------------------------------
if defined ISCC goto :have_iscc
if exist "C:\Program Files (x86)\Inno Setup 6\ISCC.exe" (
    set "ISCC=C:\Program Files (x86)\Inno Setup 6\ISCC.exe"
    goto :have_iscc
)
if exist "C:\Program Files\Inno Setup 6\ISCC.exe" (
    set "ISCC=C:\Program Files\Inno Setup 6\ISCC.exe"
    goto :have_iscc
)
echo.
echo [FAIL] Inno Setup not found.
echo        Install it from https://jrsoftware.org/isdl.php, or set ISCC
echo        to the full path of ISCC.exe.
goto :err

:have_iscc
echo.
echo [INFO] Using Inno Setup compiler: %ISCC%

REM ---- Step 3: compile the installer -------------------------------------
if not exist dist\installer mkdir dist\installer
"%ISCC%" build\installer.iss || goto :err

echo.
echo [OK] Installer written to: dist\installer\
dir /b dist\installer\*.exe
endlocal
exit /b 0

:err
echo.
echo [FAIL] Installer build failed.
endlocal
exit /b 1
