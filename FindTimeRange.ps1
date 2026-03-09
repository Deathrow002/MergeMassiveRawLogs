# FindTimeRange.ps1
# PowerShell script to find the earliest and latest timestamp across all log files
# Scans recursively under data/input/{folder}/{file.log} (and flat files)
# Extracts timestamps in format [yyyy-MM-ddTHH:mm:ss.SSS+ZZZZ] from log header lines
# Uses parallel processing (Runspaces) for efficiency with large numbers of files
#
# Usage:
#   # Default path (.\data\input)
#   .\FindTimeRange.ps1
#
#   # Custom input path
#   .\FindTimeRange.ps1 -InputPath ".\logs"
#   .\FindTimeRange.ps1 -InputPath "C:\server\logs\input"
#
#   # Customize thread count
#   .\FindTimeRange.ps1 -InputPath ".\data\input" -MaxThreads 8
#
# Parameters:
#   -InputPath   : Root directory to scan recursively (default: .\data\input)
#   -MaxThreads  : Number of parallel threads (default: CPU core count)
#
# Output:
#   - Lists all discovered files grouped by folder with sizes
#   - Shows per-file earliest/latest timestamps
#   - Shows global earliest/latest with source file and total duration
param(
    [string]$InputPath = ".\data\input",
    [int]$MaxThreads = [Environment]::ProcessorCount
)

$TimestampPattern = '\[(\d{4}-\d{2}-\d{2}T[\d:\.]+[+-]\d{4})\]'

# Collect all log files recursively
$FileItems = Get-ChildItem -Path $InputPath -File -Recurse | Sort-Object FullName
$Files = $FileItems | Select-Object -ExpandProperty FullName

if ($null -eq $Files -or $Files.Count -eq 0) {
    Write-Error "No files found under '$InputPath'."
    exit 1
}

$ResolvedInput = (Resolve-Path $InputPath).Path
$TotalSize = ($FileItems | Measure-Object -Property Length -Sum).Sum
$TotalSizeMB = [math]::Round($TotalSize / 1MB, 2)

# List all found files grouped by folder
Write-Host "=========================================="
Write-Host "  Found $($Files.Count) file(s) under '$InputPath'"
Write-Host "  Total size: $TotalSizeMB MB"
Write-Host "=========================================="
Write-Host ""

$Grouped = $FileItems | Group-Object { Split-Path $_.FullName -Parent }
foreach ($Group in $Grouped) {
    $FolderRel = $Group.Name
    if ($FolderRel.StartsWith($ResolvedInput)) {
        $FolderRel = $FolderRel.Substring($ResolvedInput.Length).TrimStart('\', '/')
    }
    if ([string]::IsNullOrEmpty($FolderRel)) { $FolderRel = "." }

    Write-Host ("  [{0}]" -f $FolderRel)
    foreach ($F in $Group.Group) {
        $SizeMB = [math]::Round($F.Length / 1MB, 2)
        Write-Host ("    {0}  ({1} MB)" -f $F.Name, $SizeMB)
    }
    Write-Host ""
}

Write-Host "Scanning timestamps using $MaxThreads threads..."
Write-Host ""

$RunspacePool = [runspacefactory]::CreateRunspacePool(1, $MaxThreads)
$RunspacePool.Open()
$Jobs = @()

foreach ($File in $Files) {
    $PS = [powershell]::Create()
    $PS.RunspacePool = $RunspacePool

    [void]$PS.AddScript({
        param($FilePath, $Pattern)

        $Earliest = $null
        $Latest   = $null

        $Reader = [System.IO.StreamReader]::new($FilePath)
        try {
            while ($null -ne ($Line = $Reader.ReadLine())) {
                if ($Line -match $Pattern) {
                    $TsStr = $Matches[1]
                    try {
                        $Ts = [DateTimeOffset]::ParseExact(
                            $TsStr,
                            "yyyy-MM-dd'T'HH:mm:ss.fffzzz",
                            [System.Globalization.CultureInfo]::InvariantCulture
                        )
                    } catch {
                        # Try without milliseconds
                        try {
                            $Ts = [DateTimeOffset]::ParseExact(
                                $TsStr,
                                "yyyy-MM-dd'T'HH:mm:sszzz",
                                [System.Globalization.CultureInfo]::InvariantCulture
                            )
                        } catch {
                            continue
                        }
                    }

                    if ($null -eq $Earliest -or $Ts -lt $Earliest) { $Earliest = $Ts }
                    if ($null -eq $Latest   -or $Ts -gt $Latest)   { $Latest   = $Ts }
                }
            }
        } finally {
            $Reader.Close()
        }

        return @{
            File     = $FilePath
            Earliest = $Earliest
            Latest   = $Latest
        }
    })

    [void]$PS.AddArgument($File)
    [void]$PS.AddArgument($TimestampPattern)

    $Jobs += @{
        PowerShell = $PS
        Handle     = $PS.BeginInvoke()
    }
}

# Collect results
$GlobalEarliest     = $null
$GlobalEarliestFile = ""
$GlobalLatest       = $null
$GlobalLatestFile   = ""

foreach ($Job in $Jobs) {
    $Result = $Job.PowerShell.EndInvoke($Job.Handle)
    $Job.PowerShell.Dispose()

    if ($null -eq $Result -or $Result.Count -eq 0) { continue }

    $R = $Result[0]
    $RelPath = $R.File
    if ($R.File.StartsWith((Resolve-Path $InputPath).Path)) {
        $RelPath = $R.File.Substring((Resolve-Path $InputPath).Path.Length + 1)
    }

    if ($null -ne $R.Earliest) {
        Write-Host ("  {0}" -f $RelPath)
        Write-Host ("    Earliest : {0}" -f $R.Earliest.ToString("yyyy-MM-dd HH:mm:ss.fff zzz"))
        Write-Host ("    Latest   : {0}" -f $R.Latest.ToString("yyyy-MM-dd HH:mm:ss.fff zzz"))
        Write-Host ""
    } else {
        Write-Host ("  {0}  (no timestamps found)" -f $RelPath)
        Write-Host ""
    }

    if ($null -ne $R.Earliest -and ($null -eq $GlobalEarliest -or $R.Earliest -lt $GlobalEarliest)) {
        $GlobalEarliest     = $R.Earliest
        $GlobalEarliestFile = $RelPath
    }
    if ($null -ne $R.Latest -and ($null -eq $GlobalLatest -or $R.Latest -gt $GlobalLatest)) {
        $GlobalLatest     = $R.Latest
        $GlobalLatestFile = $RelPath
    }
}

$RunspacePool.Close()
$RunspacePool.Dispose()

# Summary
Write-Host "=========================================="
if ($null -ne $GlobalEarliest) {
    Write-Host ("  Earliest : {0}  [{1}]" -f $GlobalEarliest.ToString("yyyy-MM-dd HH:mm:ss.fff zzz"), $GlobalEarliestFile)
    Write-Host ("  Latest   : {0}  [{1}]" -f $GlobalLatest.ToString("yyyy-MM-dd HH:mm:ss.fff zzz"), $GlobalLatestFile)

    $Duration = $GlobalLatest - $GlobalEarliest
    $Parts = @()
    if ($Duration.Days -gt 0)    { $Parts += "$($Duration.Days)d" }
    if ($Duration.Hours -gt 0)   { $Parts += "$($Duration.Hours)h" }
    if ($Duration.Minutes -gt 0) { $Parts += "$($Duration.Minutes)m" }
    $Parts += "$($Duration.Seconds).$($Duration.Milliseconds.ToString('000'))s"
    Write-Host ("  Duration : {0}" -f ($Parts -join " "))
} else {
    Write-Host "  No timestamps found in any file."
}
Write-Host "=========================================="
