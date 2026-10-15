# 监听 NapCat 的登录二维码,有新码就覆盖保存成 二维码.png
# 由 二维码.bat 调用(自动绕过执行策略限制)

$ErrorActionPreference = 'SilentlyContinue'
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$cache = Join-Path $root 'NapCat\cache'
$dst = Join-Path $root '二维码.png'

# 超过这个时间的二维码算过期,不写出去(免得扫到失效码)
$freshSeconds = 600
# 同一批生成的图里按这个时间窗挑最大的那张(最清晰)
$sameBatch = 120

Write-Host "监听目录: $cache"
Write-Host "输出文件: $dst"
Write-Host "有新二维码就覆盖保存,按 Ctrl+C 停止。"
Write-Host ""

$lastWrite = $null
while ($true) {
    $files = Get-ChildItem (Join-Path $cache 'qrcode*.png')
    if ($files) {
        $newest = $files | Sort-Object LastWriteTime -Descending | Select-Object -First 1
        $ageSec = (Get-Date) - $newest.LastWriteTime
        if ($ageSec.TotalSeconds -lt $freshSeconds) {
            $pick = $files |
                Where-Object { ($newest.LastWriteTime - $_.LastWriteTime).TotalSeconds -lt $sameBatch } |
                Sort-Object Length -Descending | Select-Object -First 1
            if ($pick.FullName -ne $lastWrite) {
                Copy-Item $pick.FullName $dst -Force
                $lastWrite = $pick.FullName
                $dim = ''
                try {
                    $img = [System.Drawing.Image]::FromFile($dst)
                    $dim = "$($img.Width)x$($img.Height)"
                    $img.Dispose()
                } catch { }
                Write-Host ("[{0}] 已保存二维码 -> 二维码.png  ({1} {2})" -f `
                    (Get-Date -Format 'HH:mm:ss'), $pick.Name, $dim)
            }
        }
    }
    Start-Sleep -Seconds 2
}