@echo off
chcp 65001 >nul
cd /d "%~dp0"
title 二维码监听 - 请勿关闭本窗口

echo ============================================
echo   监听 NapCat 登录二维码
echo   有新码就存成 二维码.png(覆盖上一张)
echo ============================================
echo.

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0watch_qr.ps1"

echo.
echo 监听已停止。按任意键关闭。
pause >nul