@echo off
chcp 65001 >nul
cd /d "%~dp0"
title 二维码 - 用手机扫这个窗口

echo ============================================
echo   盯着 NapCat 的登录码,一出码就画在这个窗口
echo   手机 QQ 对着屏幕扫;按 Ctrl+C 退出
echo ============================================
echo.

python "%~dp0二维码.py" --watch

echo.
echo 已停止。按任意键关闭。
pause >nul