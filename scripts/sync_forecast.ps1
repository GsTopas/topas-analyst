# sync_forecast.ps1 - Sync af Turomkostninger 2026.xls -> Supabase
#
# Brug:
#   pwsh scripts\sync_forecast.ps1                  # tving sync
#   pwsh scripts\sync_forecast.ps1 -DryRun          # vis kun (ingen upload)
#   pwsh scripts\sync_forecast.ps1 -LogonCheck      # logon-mode: skip hvis kørt i dag

[CmdletBinding()]
param(
  [string]$ExcelPath = "K:\OFFICE\Operations\Turregnskab\Opfølgning\Turomkostninger 2026.xls",
  [string]$EnvFile   = "$PSScriptRoot\..\.env",
  [switch]$DryRun,
  [switch]$LogonCheck
)

$ErrorActionPreference = "Stop"

# Sti til lokal sync-log (én linje pr. sync med dato)
$SyncLogFile = "$PSScriptRoot\..\.sync_state\last_forecast_sync.txt"

# === Helper: Vis Windows toast-notifikation (uden eksterne moduler) ===
function Show-Notification {
  param([string]$Title, [string]$Message, [string]$Icon = "Warning")
  try {
    Add-Type -AssemblyName System.Windows.Forms -ErrorAction SilentlyContinue
    $notify = New-Object System.Windows.Forms.NotifyIcon
    $notify.Icon = [System.Drawing.SystemIcons]::$Icon
    $notify.Visible = $true
    $notify.ShowBalloonTip(15000, $Title, $Message, [System.Windows.Forms.ToolTipIcon]::$Icon)
    Start-Sleep -Seconds 16
    $notify.Dispose()
  } catch {
    # Fallback: skriv til event-log
    Write-Warning "$Title : $Message"
  }
}

# === LogonCheck-mode: skip hvis allerede synced i dag ===
if ($LogonCheck) {
  $today = (Get-Date).ToString("yyyy-MM-dd")
  if (Test-Path $SyncLogFile) {
    $lastDate = (Get-Content $SyncLogFile -Raw -ErrorAction SilentlyContinue).Trim()
    if ($lastDate -eq $today) {
      Write-Host "Forecast-sync allerede koert i dag ($today). Skipper."
      exit 0
    }
  }

  # Tjek K:-adgang foer sync
  if (-not (Test-Path $ExcelPath)) {
    Show-Notification `
      -Title "Topas Forecast-sync mangler K:-drev" `
      -Message "K:-drevet er ikke tilgaengeligt. Forbind drevet og koer manuelt sync, eller log paa igen senere." `
      -Icon "Warning"
    Write-Warning "K: ikke tilgaengeligt - sync ikke koert"
    exit 1
  }
}

# === Indlæs .env hvis miljøvariabler mangler ===
function Load-DotEnv {
  param([string]$Path)
  if (-not (Test-Path $Path)) { return }
  Get-Content $Path | ForEach-Object {
    if ($_ -match '^\s*([A-Z_][A-Z0-9_]*)\s*=\s*(.+?)\s*$') {
      $name = $Matches[1]
      $value = $Matches[2].Trim('"').Trim("'")
      if (-not [Environment]::GetEnvironmentVariable($name)) {
        [Environment]::SetEnvironmentVariable($name, $value)
      }
    }
  }
}

Load-DotEnv -Path $EnvFile

$SUPABASE_URL  = $env:SUPABASE_URL
$SUPABASE_KEY  = $env:SUPABASE_SERVICE_KEY
if (-not $SUPABASE_URL -or -not $SUPABASE_KEY) {
  Write-Error "SUPABASE_URL og SUPABASE_SERVICE_KEY skal vaere sat (i miljoe eller .env)"
  exit 1
}

# === Verificer Excel-fil ===
if (-not (Test-Path $ExcelPath)) {
  Write-Error "Excel-fil ikke fundet: $ExcelPath"
  exit 1
}

$months = @(
  @{ Num = 1;  Name = "Januar" },
  @{ Num = 2;  Name = "Februar" },
  @{ Num = 3;  Name = "Marts" },
  @{ Num = 4;  Name = "April" },
  @{ Num = 5;  Name = "Maj" },
  @{ Num = 6;  Name = "Juni" },
  @{ Num = 7;  Name = "Juli" },
  @{ Num = 8;  Name = "August" },
  @{ Num = 9;  Name = "September" },
  @{ Num = 10; Name = "Oktober" },
  @{ Num = 11; Name = "November" },
  @{ Num = 12; Name = "December" }
)

# === Aabn Excel via COM (read-only) ===
Write-Host "Aabner $ExcelPath ..."
$excel = New-Object -ComObject Excel.Application
$excel.Visible = $false
$excel.DisplayAlerts = $false
$wb = $null
$rows = @()

try {
  $wb = $excel.Workbooks.Open($ExcelPath, 0, $true)  # ReadOnly=$true

  foreach ($m in $months) {
    $sheetName = $m.Name
    $sheet = $null
    try { $sheet = $wb.Sheets.Item($sheetName) } catch {
      Write-Warning "Fane '$sheetName' ikke fundet - springes over"
      continue
    }

    # Find sidste raekke med data i B-kolonnen (turkode)
    $usedRows = $sheet.UsedRange.Rows.Count
    $found = 0

    # FORMEL: diff = (I or 0) - (C or 0)
    # - Almindelige turer (Topas/GBT/VBT): krav om I udfyldt. Diff = I - C.
    # - 'Budget [maaned]'-raekker: inkluder selv uden I (ufordelt budget).
    #   Diff = -C. Track current section saa GBT og VBT 'Budget Januar' faar
    #   unikke tour_codes ('Budget Januar (GBT)' / 'Budget Januar (VBT)').
    # - Kolonne F ignoreres helt.
    $currentSection = "TOPAS"  # default; opdateres naar vi rammer en sektion-header
    for ($r = 6; $r -le $usedRows; $r++) {
      $a = $sheet.Cells.Item($r, 1).Value2   # Hjemkomst dato eller sektion-header
      $b = $sheet.Cells.Item($r, 2).Value2   # Turkode eller "Budget Januar"
      $c = $sheet.Cells.Item($r, 3).Value2   # Budget DB
      $i = $sheet.Cells.Item($r, 9).Value2   # Realiseret DB
      $l = $sheet.Cells.Item($r, 12).Value2  # Pax-diff

      # Track current section ud fra A-vaerdien (kategori-header)
      if ($null -ne $a) {
        $aStr = [string]$a
        if ($aStr -match "GREENLAND BY TOPAS") { $currentSection = "GBT" }
        elseif ($aStr -match "VIETNAM BY TOPAS") { $currentSection = "VBT" }
        elseif ($aStr -eq "TOPAS") { $currentSection = "TOPAS" }
      }

      if ($null -eq $b) { continue }
      $bStr = [string]$b
      if ($bStr -eq "Turkode" -or $bStr.Length -lt 4) { continue }

      # Klassificer raekke:
      $isBudgetMonth = $bStr -match "^Budget\s+\w+"
      $hasI = $null -ne $i -and $i -is [double]
      $hasC = $null -ne $c -and $c -is [double]

      if ($isBudgetMonth) {
        # 'Budget [maaned]'-raekke: krav om C, men I maa vaere tom (ufordelt)
        if (-not $hasC) { continue }
        # Unik tour_code via section-prefix
        $tourCode = "$bStr ($currentSection)"
        $homecoming = $null
        $homecomingDt = $null
        if ($null -ne $a -and $a -is [double] -and $a -gt 40000 -and $a -lt 60000) {
          $homecomingDt = [DateTime]::FromOADate($a)
          $homecoming = $homecomingDt.ToString("yyyy-MM-dd")
        }
        # Krav: dato i 2026 + samme maaned som fanen
        if ($null -eq $homecomingDt -or
            $homecomingDt.Year -ne 2026 -or
            $homecomingDt.Month -ne $m.Num) {
          continue
        }
        $iVal = if ($hasI) { [double]$i } else { 0.0 }
        $cVal = [double]$c
        $diff = $iVal - $cVal
        if ($diff -eq 0) { continue }

        $rows += [PSCustomObject]@{
          month            = $sheetName
          month_num        = $m.Num
          tour_code        = $tourCode
          homecoming_date  = $homecoming
          budget_db        = $cVal
          realiseret_db    = if ($hasI) { [double]$i } else { $null }
          db_budget_diff   = $diff
          pax_diff         = $null
          dg_diff          = $null
        }
        $found++
        continue
      }

      # Almindelige turer: krav om I udfyldt
      if (-not $hasI) { continue }

      $iVal = [double]$i
      $cVal = if ($hasC) { [double]$c } else { 0.0 }
      $diff = $iVal - $cVal
      if ($diff -eq 0) { continue }

      $homecoming = $null
      $homecomingDt = $null
      if ($null -ne $a -and $a -is [double] -and $a -gt 40000 -and $a -lt 60000) {
        $homecomingDt = [DateTime]::FromOADate($a)
        $homecoming = $homecomingDt.ToString("yyyy-MM-dd")
      }
      if ($null -eq $homecomingDt -or
          $homecomingDt.Year -ne 2026 -or
          $homecomingDt.Month -ne $m.Num) {
        continue
      }

      $paxDiff = $null
      if ($null -ne $l -and $l -is [double]) { $paxDiff = [int][Math]::Round($l) }

      $rows += [PSCustomObject]@{
        month            = $sheetName
        month_num        = $m.Num
        tour_code        = $bStr
        homecoming_date  = $homecoming
        budget_db        = if ($hasC) { [double]$c } else { $null }
        realiseret_db    = $iVal
        db_budget_diff   = $diff
        pax_diff         = $paxDiff
        dg_diff          = $null
      }
      $found++
    }

    # === Saerlige raekker: Oplæring + Research ===
    # Disse er angivet via kolonne A som label (B er tom). Budget i C, realiseret i I.
    # DB-budget-forskel udregnes som I - C.
    #
    # Skip hvis fanen ikke har 2026-turdata (sandsynligvis stadig 2025-rester):
    # i saa fald er Oplæring/Research ogsaa fra 2025 og hoerer ikke i 2026-forecast.
    if ($found -eq 0) {
      Write-Host "  $sheetName : 0 raekker (fanen mangler 2026-data, skipper ogsaa Oplaering/Research)"
      continue
    }

    for ($r = 6; $r -le $usedRows; $r++) {
      $a = $sheet.Cells.Item($r, 1).Value2
      if ($null -eq $a) { continue }
      $aStr = [string]$a
      if ($aStr -notmatch "^(Oplæring|Research)\s*$") { continue }

      $c = $sheet.Cells.Item($r, 3).Value2   # Budget
      $i = $sheet.Cells.Item($r, 9).Value2   # Realiseret

      # Krav: I skal have vaerdi (ellers er udgiften ikke realiseret endnu)
      if ($null -eq $i) { continue }

      $cVal = if ($null -ne $c) { [double]$c } else { 0.0 }
      $iVal = [double]$i
      $diff = $iVal - $cVal

      $rows += [PSCustomObject]@{
        month            = $sheetName
        month_num        = $m.Num
        tour_code        = $aStr.Trim()
        homecoming_date  = $null
        budget_db        = $cVal
        realiseret_db    = $iVal
        db_budget_diff   = $diff
        pax_diff         = $null
        dg_diff          = $null
      }
      $found++
    }

    Write-Host "  $sheetName : $found raekker"
  }
}
finally {
  if ($wb)    { $wb.Close($false) }
  if ($excel) { $excel.Quit() }
  [GC]::Collect()
  [GC]::WaitForPendingFinalizers()
}

Write-Host ""
Write-Host "Total: $($rows.Count) raekker indsamlet"

if ($DryRun) {
  Write-Host ""
  Write-Host "=== DRY RUN - foerste 5 raekker ==="
  $rows | Select-Object -First 5 | Format-Table -AutoSize
  Write-Host "(Ingen upload til Supabase)"
  exit 0
}

if ($rows.Count -eq 0) {
  Write-Warning "Ingen raekker at uploade - afbryder"
  exit 0
}

# === Upload til Supabase ===
# Strategi: TRUNCATE + bulk INSERT for at undgaa stale rows.
# Bruger PostgREST upsert (on_conflict) som primaer vej.
$endpoint = "$SUPABASE_URL/rest/v1/tour_pl_forecast"
$headers = @{
  "apikey"        = $SUPABASE_KEY
  "Authorization" = "Bearer $SUPABASE_KEY"
  "Content-Type"  = "application/json"
  "Prefer"        = "resolution=merge-duplicates,return=minimal"
}

# Slet alle eksisterende rows (vi laver fuld snapshot hver gang)
Write-Host "Sletter eksisterende rows ..."
$deleteHeaders = @{
  "apikey"        = $SUPABASE_KEY
  "Authorization" = "Bearer $SUPABASE_KEY"
}
try {
  $deleteUrl = $endpoint + "?id=gt.0"
  Invoke-RestMethod -Uri $deleteUrl -Method Delete -Headers $deleteHeaders | Out-Null
} catch {
  Write-Warning "DELETE fejlede: $_"
}

# Upload i batches af 100 (PostgREST har payload-limit)
$batchSize = 100
$totalUploaded = 0
for ($i = 0; $i -lt $rows.Count; $i += $batchSize) {
  $batch = $rows[$i..([Math]::Min($i + $batchSize - 1, $rows.Count - 1))]
  $body = $batch | ConvertTo-Json -Depth 3 -Compress
  if (-not $body.StartsWith("[")) { $body = "[$body]" }  # ConvertTo-Json med 1 row laver ikke array

  try {
    Invoke-RestMethod -Uri $endpoint -Method Post -Headers $headers -Body $body -ContentType "application/json; charset=utf-8" | Out-Null
    $totalUploaded += $batch.Count
    $batchNum = [int]($i / $batchSize) + 1
    Write-Host "  Batch ${batchNum}: $($batch.Count) raekker (total $totalUploaded)"
  } catch {
    Write-Error "Upload-batch fejlede: $_"
    exit 1
  }
}

Write-Host ""
Write-Host "Sync faerdig: $totalUploaded raekker i Supabase tour_pl_forecast"

# Skriv sync-log saa LogonCheck ved at vi koerte i dag
try {
  $stateDir = Split-Path $SyncLogFile -Parent
  if (-not (Test-Path $stateDir)) {
    New-Item -ItemType Directory -Path $stateDir -Force | Out-Null
  }
  (Get-Date).ToString("yyyy-MM-dd") | Out-File -FilePath $SyncLogFile -Encoding utf8 -NoNewline
} catch {
  Write-Warning "Kunne ikke skrive sync-log: $_"
}
