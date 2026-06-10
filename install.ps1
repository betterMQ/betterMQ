# betterMQ installer for Windows.
#
#   powershell -ExecutionPolicy Bypass -c "irm https://bettermq.com/install.ps1 | iex"
#
# Downloads the latest bettermq-windows-amd64 binary from GitHub Releases,
# verifies sha256 (when checksums.txt is published for the release), installs
# to %USERPROFILE%\.bettermq\bin and adds it to your user PATH.
#
# Optional env vars:
#   BETTERMQ_INSTALL_DIR — base dir (default: %USERPROFILE%\.bettermq)
#   BETTERMQ_VERSION     — explicit version like "0.3.1" (default: latest)
#   BETTERMQ_FORCE=1     — reinstall even if the same version is present

$ErrorActionPreference = "Stop"

$Repo = "betterMQ/betterMQ"
$Asset = "bettermq-windows-amd64.exe"
$InstallDir = if ($env:BETTERMQ_INSTALL_DIR) { $env:BETTERMQ_INSTALL_DIR } else { Join-Path $env:USERPROFILE ".bettermq" }
$BinDir = Join-Path $InstallDir "bin"
$ReleasesUrl = "https://github.com/$Repo/releases"

function Info($msg) { Write-Host "-> $msg" -ForegroundColor DarkGray }
function Ok($msg) { Write-Host "OK $msg" -ForegroundColor Green }

function Print-Logo {
  $esc = [char]27
  $brand = "${esc}[38;2;31;71;240m"
  $reset = "${esc}[0m"
  Write-Host "     better${brand}MQ${reset}"
}

function Print-Welcome {
    Write-Host ""
    Print-Logo
    Write-Host ""
    Write-Host "     Http Messaging & Scheduling" -ForegroundColor DarkGray
    Write-Host ""
    Write-Host "  installing self-hosted HTTP message broker" -ForegroundColor DarkGray
    Write-Host "  -----------------------------------------" -ForegroundColor DarkGray
    Write-Host ""
}

function Print-Success($ver) {
    Write-Host ""
    Write-Host "  betterMQ $ver installed" -ForegroundColor Green
    Write-Host ""
}

Print-Welcome

# --- resolve version ---------------------------------------------------------

$Version = $env:BETTERMQ_VERSION
if (-not $Version) {
    try {
        $release = Invoke-RestMethod -Uri "https://api.github.com/repos/$Repo/releases/latest" -UseBasicParsing
        $Version = $release.tag_name -replace '^v', ''
    } catch {
        throw "Could not resolve latest version from $ReleasesUrl — set BETTERMQ_VERSION and retry."
    }
}
$Tag = "v$Version"
$AssetBase = "$ReleasesUrl/download/$Tag"
Info "Version: $Version"

# --- skip when already installed ----------------------------------------------

$BinPath = Join-Path $BinDir "bettermq.exe"
$VersionFile = Join-Path $BinDir "bettermq.version"
if ($env:BETTERMQ_FORCE -ne "1" -and (Test-Path $BinPath) -and (Test-Path $VersionFile)) {
    if ((Get-Content $VersionFile -ErrorAction SilentlyContinue) -eq $Version) {
        Ok "bettermq v$Version is already installed - nothing to do"
        Info "Force a reinstall by setting BETTERMQ_FORCE=1"
        return
    }
}

# --- download + verify ----------------------------------------------------------

New-Item -ItemType Directory -Force -Path $BinDir | Out-Null
$Tmp = Join-Path ([System.IO.Path]::GetTempPath()) ("bettermq-install-" + [Guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Force -Path $Tmp | Out-Null

try {
    $ZipPath = Join-Path $Tmp "$Asset.zip"
    Info "Downloading $Asset.zip..."
    Invoke-WebRequest -Uri "$AssetBase/$Asset.zip" -OutFile $ZipPath -UseBasicParsing

    # checksums.txt is published from v0.4+ — verify when available.
    $ChecksumsPath = Join-Path $Tmp "checksums.txt"
    $Expected = $null
    try {
        Invoke-WebRequest -Uri "$AssetBase/checksums.txt" -OutFile $ChecksumsPath -UseBasicParsing
        $line = Select-String -Path $ChecksumsPath -Pattern ([regex]::Escape("$Asset.zip")) | Select-Object -First 1
        if ($line) { $Expected = ($line.Line -split '\s+')[0] }
    } catch {
        Info "No checksums.txt for $Tag - skipping verification."
    }
    if ($Expected) {
        $Actual = (Get-FileHash -Path $ZipPath -Algorithm SHA256).Hash.ToLower()
        if ($Actual -ne $Expected.ToLower()) {
            throw "Checksum mismatch for $Asset.zip (expected $Expected, got $Actual)"
        }
        Ok "Verified sha256"
    }

    Expand-Archive -Path $ZipPath -DestinationPath $Tmp -Force
    $Extracted = Join-Path $Tmp $Asset
    if (-not (Test-Path $Extracted)) { throw "Binary $Asset missing from archive" }

    Move-Item -Path $Extracted -Destination $BinPath -Force
    Set-Content -Path $VersionFile -Value $Version
    Ok "Installed binary -> $BinPath"
} finally {
    Remove-Item -Recurse -Force $Tmp -ErrorAction SilentlyContinue
}

# --- PATH ----------------------------------------------------------------------

$UserPath = [Environment]::GetEnvironmentVariable("Path", "User")
if ($UserPath -notlike "*$BinDir*") {
    [Environment]::SetEnvironmentVariable("Path", "$UserPath;$BinDir", "User")
    $env:Path = "$env:Path;$BinDir"
    Ok "Added $BinDir to your user PATH (restart terminals to pick it up)"
}

Print-Success $Version
Write-Host "  Start:   bettermq serve"
Write-Host "  Panel:   http://localhost:8080/panel/" -ForegroundColor DarkGray
Write-Host "  Docs:    http://localhost:8080/docs" -ForegroundColor DarkGray
Write-Host "  GitHub:  https://github.com/$Repo" -ForegroundColor DarkGray
Write-Host ""
Write-Host "Open a new terminal (or use the full path below) and run:" -ForegroundColor DarkGray
Write-Host "  $BinPath serve"
Write-Host ""
