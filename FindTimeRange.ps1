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
#   # Use as function in other scripts:
#   #   . .\FindTimeRange.ps1   # dot-source to load function
#   #   $result = Find-TimeRange -InputPath ".\data\input"
#   #   $result.Earliest       # global earliest DateTimeOffset
#   #   $result.Latest         # global latest DateTimeOffset
#   #   $result.FileResults    # per-file details array
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

function Find-TimeRange {
    <#
    .SYNOPSIS
        Find the earliest and latest timestamp across all log files in a directory.
    .DESCRIPTION
        Scans all files recursively, extracts timestamps in [yyyy-MM-ddTHH:mm:ss.SSS+ZZZZ] format,
        and returns one consolidated result with per-file details and global earliest/latest.
        Uses parallel processing (Runspaces) for efficiency.
    .PARAMETER InputPath
        Root directory to scan recursively.
    .PARAMETER MaxThreads
        Number of parallel threads (default: CPU core count).
    .OUTPUTS
        PSCustomObject with properties:
          - TotalFiles     : int
          - TotalSizeMB    : double
          - Earliest       : DateTimeOffset (global)
          - EarliestFile   : string (relative path)
          - Latest         : DateTimeOffset (global)
          - LatestFile     : string (relative path)
          - Duration       : TimeSpan
          - DurationText   : string (human-readable)
          - FileResults    : array of per-file objects (File, SizeMB, Earliest, Latest)
    .EXAMPLE
        $result = Find-TimeRange -InputPath ".\data\input"
        $result                  # shows full summary
        $result.Earliest         # 2026-02-03T22:26:49.796+07:00
        $result.FileResults      # per-file details
    #>
    param(
        [Parameter(Mandatory=$true)]
        [string]$InputPath,
        [int]$MaxThreads = [Environment]::ProcessorCount
    )

    $TimestampPattern = '\[(\d{4}-\d{2}-\d{2}T[\d:\.]+[+-]\d{4})\]'

    # Collect all log files recursively
    $FileItems = Get-ChildItem -Path $InputPath -File -Recurse | Sort-Object FullName
    $Files = $FileItems | Select-Object -ExpandProperty FullName

    if ($null -eq $Files -or $Files.Count -eq 0) {
        Write-Error "No files found under '$InputPath'."
        return $null
    }

    $ResolvedInput = (Resolve-Path $InputPath).Path
    $TotalSize = ($FileItems | Measure-Object -Property Length -Sum).Sum
    $TotalSizeMB = [math]::Round($TotalSize / 1MB, 2)

    # Parallel scan using Runspaces
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
    $FileResults        = @()

    foreach ($Job in $Jobs) {
        $Result = $Job.PowerShell.EndInvoke($Job.Handle)
        $Job.PowerShell.Dispose()

        if ($null -eq $Result -or $Result.Count -eq 0) { continue }

        $R = $Result[0]
        $RelPath = $R.File
        if ($R.File.StartsWith($ResolvedInput)) {
            $RelPath = $R.File.Substring($ResolvedInput.Length).TrimStart('\', '/')
        }

        $FileSizeMB = [math]::Round((Get-Item $R.File).Length / 1MB, 2)

        $FileResults += [PSCustomObject]@{
            File     = $RelPath
            SizeMB   = $FileSizeMB
            Earliest = $R.Earliest
            Latest   = $R.Latest
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

    # Build duration text
    $Duration     = $null
    $DurationText = ""
    if ($null -ne $GlobalEarliest -and $null -ne $GlobalLatest) {
        $Duration = $GlobalLatest - $GlobalEarliest
        $Parts = @()
        if ($Duration.Days -gt 0)    { $Parts += "$($Duration.Days)d" }
        if ($Duration.Hours -gt 0)   { $Parts += "$($Duration.Hours)h" }
        if ($Duration.Minutes -gt 0) { $Parts += "$($Duration.Minutes)m" }
        $Parts += "$($Duration.Seconds).$($Duration.Milliseconds.ToString('000'))s"
        $DurationText = $Parts -join " "
    }

    return [PSCustomObject]@{
        TotalFiles   = $Files.Count
        TotalSizeMB  = $TotalSizeMB
        Earliest     = $GlobalEarliest
        EarliestFile = $GlobalEarliestFile
        Latest       = $GlobalLatest
        LatestFile   = $GlobalLatestFile
        Duration     = $Duration
        DurationText = $DurationText
        FileResults  = $FileResults
    }
}

# --- Main execution ---

$Result = Find-TimeRange -InputPath $InputPath -MaxThreads $MaxThreads

if ($null -eq $Result) { exit 1 }

# List all found files grouped by folder
$ResolvedInput = (Resolve-Path $InputPath).Path
$FileItems = Get-ChildItem -Path $InputPath -File -Recurse | Sort-Object FullName

Write-Host "=========================================="
Write-Host "  Found $($Result.TotalFiles) file(s) under '$InputPath'"
Write-Host "  Total size: $($Result.TotalSizeMB) MB"
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

# Per-file details
foreach ($FR in $Result.FileResults | Sort-Object File) {
    if ($null -ne $FR.Earliest) {
        Write-Host ("  {0}" -f $FR.File)
        Write-Host ("    Earliest : {0}" -f $FR.Earliest.ToString("yyyy-MM-dd HH:mm:ss.fff zzz"))
        Write-Host ("    Latest   : {0}" -f $FR.Latest.ToString("yyyy-MM-dd HH:mm:ss.fff zzz"))
        Write-Host ""
    } else {
        Write-Host ("  {0}  (no timestamps found)" -f $FR.File)
        Write-Host ""
    }
}

# Summary
Write-Host "=========================================="
if ($null -ne $Result.Earliest) {
    Write-Host ("  Earliest : {0}  [{1}]" -f $Result.Earliest.ToString("yyyy-MM-dd HH:mm:ss.fff zzz"), $Result.EarliestFile)
    Write-Host ("  Latest   : {0}  [{1}]" -f $Result.Latest.ToString("yyyy-MM-dd HH:mm:ss.fff zzz"), $Result.LatestFile)
    Write-Host ("  Duration : {0}" -f $Result.DurationText)
} else {
    Write-Host "  No timestamps found in any file."
}
Write-Host "=========================================="
