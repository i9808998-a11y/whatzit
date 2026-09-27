@echo off
cd /d %~dp0
if not exist .env copy .env.example .env
for /f "usebackq tokens=1,* delims==" %%A in (".env") do if not "%%A"=="" if not "%%A:~0,1"=="#" set "%%A=%%B"
node server.js
pause
