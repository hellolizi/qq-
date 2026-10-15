@echo off
chcp 65001 >nul
cd /d "%~dp0"
title QQ DeepSeek Bot

echo ============================================
echo   QQ + DeepSeek 一键启动
echo ============================================
echo.

set "RUNNING="
tasklist /FI "IMAGENAME eq NapCatWinBootMain.exe" 2>nul | find /I "NapCatWinBootMain.exe" >nul && set "RUNNING=1"

if defined RUNNING (
    echo [1/2] NapCat 已在运行,跳过启动。
) else (
    echo [1/2] 启动 NapCat ^(会弹 UAC,请点"是"^)...
    start "" "%~dp0NapCat\启动NapCat.bat"
    echo       等待 NapCat 就绪,首次使用需要在浏览器里扫码登录...
    timeout /t 25 /nobreak >nul
)

echo.
echo [1.5/2] 启动二维码监听(有新登录码就存成 二维码.png)...
start "二维码监听" /min "%~dp0二维码.bat"
timeout /t 2 /nobreak >nul

echo.
echo [2/2] 启动 DeepSeek 机器人...
echo       保持本窗口开着;按 Ctrl+C 可停止机器人。
echo.
python bot.py
echo.
echo 机器人已退出。按任意键关闭本窗口。
pause >nul