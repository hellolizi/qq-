@echo off
chcp 65001 >nul
cd /d "%~dp0"
title 小鲸鱼 - 重新登录(查额度用)

echo ============================================
echo   重新登录阿里云百炼
echo ============================================
echo.
echo 会拉起浏览器,登录你的阿里云账号点一下授权就行。
echo 授权完这个窗口会自己结束,然后 /余额 又能用了。
echo.

set "BL=%LOCALAPPDATA%\bailian-cli\bin\bl.exe"
if not exist "%BL%" set "BL=bl"

"%BL%" auth login --console

echo.
echo 搞定。按任意键关闭本窗口。
pause >nul