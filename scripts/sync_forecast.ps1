# sync_forecast.ps1 - Daglig sync af Turomkostninger 2026.xls → Supabase
#
# Læser K:\OFFICE\Operations\Turregnskab\Opfølgning\Turomkostninger 2026.xls
# via Excel COM (read-only, ingen ændringer i kilde-filen) og uploader
# data til Supabase-tabellen `tour_pl_forecast`.
#
# Brug:
#   pwsh scripts\sync_forecast.ps1            # kører sync med standardværdier
#   pwsh scripts\sync_forecast.ps1 -DryRun    # vis hvad der ville blive uploadet
#
# Krav:
#   - Excel installeret (COM-automation)
#   - Adgang til K:-drevet
#   - Miljøvariabler: SUPABASE_URL, SUPABASE_SERVICE_KEY
#     (læses fra .env hvis ikke sat)
#
# Setup i Task Scheduler (hver dag kl. 06:00):
#   $action = New-ScheduledTaskAction -Execute "pwsh.exe" `
#     -Argument "-NoProfile -File `"C:\Users\gs\Downloads\topas-scraper\scripts\sync_forecast.ps1`""
#   $trigger = New-ScheduledTaskTrigger -Daily -At "06:00"
#   Register-ScheduledTask -TaskName "TopasForecastSync" -Action $action -Trigger $trigger

[CmdletBinding()]
param(
  [string]$ExcelPath = "K:\OFFICE\Operations\Turregnskab\Opfølgning\Turomkostninger 2026.xls",
  [string]$EnvFile   = "$PSScriptRoot\..\.env",
  [switch]$DryRun
)

$ErrorActionPreference = "Stop"

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

    # Data starter typisk omkring raekke 9 (efter headers paa 4-5 + TOPAS-sektion 6-8)
    for ($r = 6; $r -le $usedRows; $r++) {
      $a = $sheet.Cells.Item($r, 1).Value2   # Hjemkomst dato (Excel serial)
      $b = $sheet.Cells.Item($r, 2).Value2   # Turkode
      $c = $sheet.Cells.Item($r, 3).Value2   # Budget DB
      $i = $sheet.Cells.Item($r, 9).Value2   # Realiseret DB
      $l = $sheet.Cells.Item($r, 12).Value2  # Pax-diff
      $mVal = $sheet.Cells.Item($r, 13).Value2  # DB budget diff
      $n = $sheet.Cells.Item($r, 14).Value2  # DG-diff

      # Filter: B, I og M skal alle have vaerdi
      if ($null -eq $b -or $null -eq $i -or $null -eq $mVal) { continue }

      # B skal vaere en turkode-streng, ikke fx "Hjemkomst dato"-header
      $bStr = [string]$b
      if ($bStr -eq "Turkode" -or $bStr.Length -lt 4) { continue }

      # Drop turer hvor DB-forskel er noejagtigt 0 (Oplaering/Research bevares
      # separat senere). Disse traekker stoejen i UI'et uden at tilfoere noget.
      if ($mVal -is [double] -and $mVal -eq 0) { continue }

      # Konverter Excel-dato (serial) til DateTime
      $homecoming = $null
      $homecomingDt = $null
      if ($null -ne $a -and $a -is [double] -and $a -gt 40000 -and $a -lt 60000) {
        $homecomingDt = [DateTime]::FromOADate($a)
        $homecoming = $homecomingDt.ToString("yyyy-MM-dd")
      }

      # Filter: A-vaerdien (hjemkomst dato) skal vaere i 2026 OG samme maaned
      # som fanen. Excel-fanen indeholder rester af 2025-data + andre maaneder
      # som vi ikke vil have med i forecast.
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
        budget_db        = if ($null -ne $c) { [double]$c } else { $null }
        realiseret_db    = [double]$i
        db_budget_diff   = [double]$mVal
        pax_diff         = $paxDiff
        dg_diff          = if ($null -ne $n -and $n -is [double]) { [double]$n } else { $null }
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
