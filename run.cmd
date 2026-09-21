@echo off
REM ===========================================================================
REM  Neurora remote view (Python) - one-command launcher.
REM
REM  Creates the virtualenv on first run, starts the signaling server, opens
REM  the public ngrok tunnel the phone connects through, and loads the viewer
REM  UI in your browser.
REM
REM  Double-click this file, or run it as .\run.cmd from a terminal.
REM  Press any key in this window to shut everything down.
REM
REM  Overridable before launching:
REM    PORT          listen port            (else .env, else 8787)
REM    PYTHON        python interpreter     (else py -3, else python)
REM    NGROK_PATH    path to ngrok.exe      (else PATH, else known locations)
REM    NGROK_DOMAIN  reserved ngrok domain  (else the one baked into the APK)
REM    SKIP_TUNNEL=1 local only, no tunnel
REM ===========================================================================
setlocal enabledelayedexpansion
cd /d "%~dp0"

REM UTF-8 console, so the server's log output renders the same way it does
REM everywhere else rather than as mojibake under the default OEM codepage.
chcp 65001 >nul

set "SERVER_TITLE=Neurora signaling server"
set "TUNNEL_TITLE=Neurora tunnel"
if not defined NGROK_DOMAIN set "NGROK_DOMAIN=district-body-stumbling.ngrok-free.dev"
set "VENV_PY=%~dp0.venv\Scripts\python.exe"

REM -------------------------------------------------------------- python ----
REM The py launcher first (it is what a normal Windows install registers),
REM then a plain python on PATH.
if not defined PYTHON (
  where py >nul 2>&1
  if not errorlevel 1 (
    set "PYTHON=py -3"
  ) else (
    where python >nul 2>&1
    if not errorlevel 1 set "PYTHON=python"
  )
)

if not defined PYTHON (
  echo.
  echo   Python was not found on PATH.
  echo   Install it from https://www.python.org/downloads/ ^(version 3.11 or newer^),
  echo   tick "Add python.exe to PATH", then run this again.
  echo.
  pause
  exit /b 1
)

REM ---------------------------------------------------------------- venv ----
if not exist "%VENV_PY%" (
  echo Creating the virtual environment ^(first run only^)...
  %PYTHON% -m venv ".venv"
  if errorlevel 1 (
    echo.
    echo   Could not create the virtual environment. Check the output above.
    echo.
    pause
    exit /b 1
  )
  echo Installing dependencies...
  "%VENV_PY%" -m pip install --upgrade pip
  "%VENV_PY%" -m pip install -r requirements.txt
  if errorlevel 1 (
    echo.
    echo   pip install failed. Check the output above.
    echo.
    pause
    exit /b 1
  )
)

REM ---------------------------------------------------------------- port ----
REM Environment first, then .env, then the default.
if not defined PORT (
  set "PORT=8787"
  if exist ".env" (
    for /f "usebackq eol=# tokens=1,* delims==" %%A in (".env") do (
      if /i "%%A"=="PORT" set "PORT=%%B"
    )
  )
)

REM --------------------------------------------------------------- ngrok ----
REM PATH first, then the places it actually tends to live on this machine.
REM Not fatal if missing: the server and the browser viewer still work on
REM localhost, only the phone cannot reach them.
set "NGROK="
if defined NGROK_PATH if exist "%NGROK_PATH%" set "NGROK=%NGROK_PATH%"
if not defined NGROK for %%P in (ngrok.exe) do if not defined NGROK set "NGROK=%%~$PATH:P"
if not defined NGROK (
  for %%D in (
    "%LOCALAPPDATA%\Microsoft\WinGet\Packages\Ngrok.Ngrok_Microsoft.Winget.Source_8wekyb3d8bbwe\ngrok.exe"
    "%LOCALAPPDATA%\Microsoft\WindowsApps\ngrok.exe"
    "%USERPROFILE%\ngrok\ngrok.exe"
    "%USERPROFILE%\Downloads\ngrok-v3-stable-windows-amd64\ngrok.exe"
    "%USERPROFILE%\scoop\shims\ngrok.exe"
    "C:\ngrok\ngrok.exe"
    "C:\ProgramData\chocolatey\bin\ngrok.exe"
  ) do if not defined NGROK if exist %%D set "NGROK=%%~D"
)
if /i "%SKIP_TUNNEL%"=="1" set "NGROK="

REM --------------------------------------------------------------- start ----
echo.
echo   Starting Neurora remote view...
echo.

REM Each child gets its own window so its log stays readable, and a title we
REM can shut down by at the end. server.py reads .env itself (load_dotenv in
REM main), so the TURN credentials there are picked up without any extra flag.
start "%SERVER_TITLE%" cmd /k chcp 65001 ^>nul ^& "%VENV_PY%" server.py

if defined NGROK (
  start "%TUNNEL_TITLE%" cmd /k "%NGROK%" http --url=%NGROK_DOMAIN% %PORT% --log=stdout
)

REM Wait for the server to actually answer before reporting or opening the
REM browser, so the first page load is never a connection-refused error.
REM
REM curl.exe (shipped with Windows since 1803) and ping, rather than
REM timeout/powershell: `timeout` refuses to run when stdin is redirected,
REM which is exactly what happens when this script is launched from another
REM process rather than double-clicked. ping is the redirect-safe sleep.
set "LOCAL_URL=http://localhost:!PORT!/"
set /a WAITED=0
:waitforserver
curl -s -o nul -m 2 "!LOCAL_URL!health" && goto serverup
set /a WAITED+=1
if !WAITED! geq 40 (
  echo   The server did not come up. Check the "%SERVER_TITLE%" window.
  echo.
  pause
  exit /b 1
)
ping -n 2 127.0.0.1 >nul
goto waitforserver
:serverup

REM Ask ngrok what it actually published rather than assuming the reserved
REM domain was accepted - a domain belonging to another account is rejected
REM and ngrok falls back to a random hostname.
set "PUBLIC_URL="
if defined NGROK (
  REM Deliberately pipe-free. Inside a for /f the command is parsed by cmd
  REM first, so a `|` must be written `^|` - and PowerShell then receives the
  REM caret literally and fails to parse. foreach/if does the same job with
  REM nothing for cmd to mangle.
  for /f "usebackq delims=" %%U in (`powershell -NoProfile -Command "foreach($i in 1..30){ try{ $t=(Invoke-RestMethod 'http://127.0.0.1:4040/api/tunnels' -TimeoutSec 1).tunnels; foreach($x in $t){ if($x.proto -eq 'https'){ $x.public_url; exit } } } catch { }; Start-Sleep -Milliseconds 500 }"`) do set "PUBLIC_URL=%%U"
)

echo.
echo   ===================================================================
echo     Viewer UI ^(this machine^) : !LOCAL_URL!
if defined PUBLIC_URL (
  echo     Viewer UI ^(share / phone^): !PUBLIC_URL!/
) else (
  if defined NGROK (
    echo     Tunnel   : did not report a URL - check the "%TUNNEL_TITLE%" window.
  ) else (
    echo     Tunnel   : ngrok not found, running on localhost only.
    echo                The phone cannot reach this. Install ngrok, or set
    echo                NGROK_PATH to its location, and run this again.
  )
)
echo   ===================================================================
echo.
echo   Press any key here to stop the server and the tunnel.
echo.

start "" "!LOCAL_URL!"
pause >nul

REM ---------------------------------------------------------------- stop ----
REM Two passes, because neither alone is reliable. The title match closes the
REM console windows themselves; it misses the processes whenever the window
REM was never created under that title (which happens when this script is
REM launched with its output redirected). The command-line match catches the
REM python and ngrok processes precisely, but leaves their parent windows
REM sitting at a prompt. Together they shut everything down and leave nothing
REM on screen.
echo.
echo   Shutting down...
taskkill /FI "WINDOWTITLE eq %SERVER_TITLE%*" /T /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq %TUNNEL_TITLE%*" /T /F >nul 2>&1
powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name='python.exe' OR Name='ngrok.exe'\" | Where-Object { $_.CommandLine -like '*server.py*' -or $_.CommandLine -like '*http --url*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }" >nul 2>&1
echo   Stopped.
ping -n 3 127.0.0.1 >nul
endlocal
