@echo off
chcp 65001 >nul
cd /d "%~dp0"
title QQ 小鲸鱼

echo ============================================
echo   QQ + 小鲸鱼 一键启动
echo ============================================
echo.

set "RUNNING="
tasklist /FI "IMAGENAME eq NapCatWinBootMain.exe" 2>nul | find /I "NapCatWinBootMain.exe" >nul && set "RUNNING=1"

if defined RUNNING (
    echo [1/3] NapCat 已在运行,跳过启动。
) else (
    echo [1/3] 启动 NapCat ^(会弹 UAC,请点"是"^)...
    start "" "%~dp0NapCat\启动NapCat.bat"
)

echo.
echo [2/3] 二维码会直接画在这个窗口里,手机 QQ 对着屏幕扫就行。
echo       不用去开文件夹;扫完也不用按键,登录成功会自动往下走。
echo.
python "%~dp0二维码.py" --wait 120

echo.
echo [3/3] 启动小鲸鱼...
echo       保持本窗口开着;按 Ctrl+C 可停止机器人。
echo.
python bot.py
echo.
echo 机器人已退出。按任意键关闭本窗口。
pause >nul