@echo off
:: 改下面这行的数字为最新的 session ID（share link 里 id= 后面的值）
set SESSION_ID=756801

echo Starting polly-bridge local relay...
echo Session ID: %SESSION_ID%
"C:\Users\kk\AppData\Roaming\TRAE SOLO CN\ModularData\ai-agent\vm\tools\python\python.exe" "C:\Users\kk\polly-bridge\local_relay.py" %SESSION_ID%
pause
