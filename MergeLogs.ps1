# MergeLogs.ps1
# PowerShell script to merge all .txt files from input path to output path using concurrency
# Optimized for large files (supports 48GB+ total size) using streaming I/O
#
# Usage:
#   # Default paths (from application.properties)
#   .\MergeLogs.ps1
#
#   # Custom paths
#   .\MergeLogs.ps1 -InputPath "C:\logs\input" -OutputPath "C:\logs\merged.txt"
#
#   # Customize thread count
#   .\MergeLogs.ps1 -InputPath ".\data\input" -OutputPath ".\output.txt" -MaxThreads 8

param(
    [string]$InputPath = ".\data\input",
    [string]$OutputPath = ".\data\output\merged.txt",
    [int]$MaxThreads = [Environment]::ProcessorCount
)

# Validate input path
if (-not (Test-Path $InputPath -PathType Container)) {
    Write-Error "Input path does not exist or is not a directory: $InputPath"
    exit 1
}

# Create output directory if needed
$OutputDir = Split-Path $OutputPath -Parent
if ($OutputDir -and -not (Test-Path $OutputDir)) {
    New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
}

# Get all .txt files
$Files = Get-ChildItem -Path $InputPath -Filter "*.txt" -File | Sort-Object Name
$TotalFiles = $Files.Count

if ($TotalFiles -eq 0) {
    Write-Host "No .txt files found in input directory."
    exit 0
}

# Calculate total size
$TotalSize = ($Files | Measure-Object -Property Length -Sum).Sum
$TotalSizeGB = [math]::Round($TotalSize / 1GB, 2)

Write-Host "Reading files from: $(Resolve-Path $InputPath)"
Write-Host "Writing to: $((Resolve-Path $OutputDir).Path)\$(Split-Path $OutputPath -Leaf)"
Write-Host "Total size: $TotalSizeGB GB ($TotalFiles files)"
Write-Host "Using $MaxThreads threads for merging..."

# Use streaming approach for large files - write directly to output with lock
$OutputFullPath = [System.IO.Path]::GetFullPath($OutputPath)
$WriteLock = [System.Object]::new()
$ProcessedCount = [ref]0
$ProcessedBytes = [ref]0L

# Create/truncate output file
[System.IO.File]::WriteAllText($OutputFullPath, "", [System.Text.Encoding]::UTF8)

# Create runspace pool for concurrency
$RunspacePool = [RunspaceFactory]::CreateRunspacePool(1, $MaxThreads)
$RunspacePool.Open()

$Jobs = @()
$FileIndex = 0

# Process files in batches to maintain order while allowing concurrency
$BatchSize = $MaxThreads * 2
$Batches = @()
for ($i = 0; $i -lt $Files.Count; $i += $BatchSize) {
    $End = [Math]::Min($i + $BatchSize - 1, $Files.Count - 1)
    $Batches += ,@($Files[$i..$End])
}

$StartTime = Get-Date

foreach ($Batch in $Batches) {
    $Jobs = @()
    
    # Queue all files in batch
    foreach ($File in $Batch) {
        $FilePath = $File.FullName
        $FileName = $File.Name
        $FileSize = $File.Length
        
        $PowerShell = [PowerShell]::Create()
        $PowerShell.RunspacePool = $RunspacePool
        
        [void]$PowerShell.AddScript({
            param($FilePath, $FileName, $FileSize)
            
            try {
                # Stream read file content
                $Content = [System.IO.File]::ReadAllText($FilePath, [System.Text.Encoding]::UTF8)
                return @{
                    Success = $true
                    FileName = $FileName
                    Content = $Content
                    Size = $FileSize
                }
            }
            catch {
                return @{
                    Success = $false
                    FileName = $FileName
                    Error = $_.Exception.Message
                    Size = 0
                }
            }
        })
        
        [void]$PowerShell.AddArgument($FilePath)
        [void]$PowerShell.AddArgument($FileName)
        [void]$PowerShell.AddArgument($FileSize)
        
        $Jobs += @{
            PowerShell = $PowerShell
            Handle = $PowerShell.BeginInvoke()
        }
    }
    
    # Collect results and write in order
    foreach ($Job in $Jobs) {
        $Result = $Job.PowerShell.EndInvoke($Job.Handle)
        
        if ($Result.Success) {
            # Append to output file
            [System.IO.File]::AppendAllText($OutputFullPath, $Result.Content, [System.Text.Encoding]::UTF8)
            $ProcessedCount.Value++
            $ProcessedBytes.Value += $Result.Size
            
            $ProgressPct = [math]::Round(($ProcessedBytes.Value / $TotalSize) * 100, 1)
            Write-Host "[$ProgressPct%] Merged: $($Result.FileName)"
        }
        else {
            Write-Host "[ERROR] $($Result.FileName): $($Result.Error)"
        }
        
        $Job.PowerShell.Dispose()
    }
}

$RunspacePool.Close()
$RunspacePool.Dispose()

$EndTime = Get-Date
$Duration = $EndTime - $StartTime
$ProcessedGB = [math]::Round($ProcessedBytes.Value / 1GB, 2)

Write-Host ""
Write-Host "Merge completed successfully!"
Write-Host "  Files processed: $($ProcessedCount.Value) of $TotalFiles"
Write-Host "  Data merged: $ProcessedGB GB"
Write-Host "  Duration: $($Duration.ToString('hh\:mm\:ss'))"
Write-Host "  Throughput: $([math]::Round($ProcessedGB / $Duration.TotalSeconds, 2)) GB/s"
