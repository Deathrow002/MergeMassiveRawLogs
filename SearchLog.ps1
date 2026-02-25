param(
    [Parameter(Mandatory=$true)]
    [string[]]$Path,

    [Parameter(Mandatory=$true)]
    [string]$Keyword,

    [string]$OutputPath,
    
    [int]$MaxThreads = [Environment]::ProcessorCount
)

# Function to expand wildcards and directories
function Get-FileList {
    param([string[]]$Paths)
    foreach ($P in $Paths) {
        if (Test-Path $P -PathType Container) {
            Get-ChildItem -Path $P -File -Recurse | Select-Object -ExpandProperty FullName
        } elseif (Test-Path $P -PathType Leaf) {
            Get-Item -Path $P | Select-Object -ExpandProperty FullName
        } else {
            Write-Warning "Path not found: $P"
        }
    }
}

$Files = Get-FileList -Paths $Path | Sort-Object -Unique

if ($null -eq $Files -or $Files.Count -eq 0) {
    Write-Error "No files found to search."
    exit 1
}

Write-Host "Searching for '$Keyword' in $($Files.Count) files using $MaxThreads threads..."
$TotalHits = 0

# Set up RunspacePool for parallel execution
$RunspacePool = [runspacefactory]::CreateRunspacePool(1, $MaxThreads)
$RunspacePool.Open()
$Jobs = @()

foreach ($File in $Files) {
    $PowerShell = [powershell]::Create()
    $PowerShell.RunspacePool = $RunspacePool
    
    # ScriptBlock to run in parallel
    [void]$PowerShell.AddScript({
        param($FilePath, $SearchTerm)
        $Results = Select-String -Path $FilePath -Pattern $SearchTerm -SimpleMatch
        if ($Results) {
            # Format results as "FilePath:LineNumber:Content"
            $FormattedMatches = $Results | ForEach-Object {
                "{0}:{1}:{2}" -f $FilePath, $_.LineNumber, $_.Line
            }
            return @{
                File = $FilePath
                Count = $Results.Count
                Formatted = $FormattedMatches
            }
        }
        return $null
    }).AddArgument($File).AddArgument($Keyword)

    $Jobs += New-Object PSObject -Property @{
        File = $File
        PowerShell = $PowerShell
        Handle = $PowerShell.BeginInvoke()
    }
}

# Monitor jobs and collect results
$CompletedCount = 0
$TotalCount = $Files.Count

while ($Jobs.Count -gt 0) {
    $CompletedJobs = $Jobs | Where-Object { $_.Handle.IsCompleted }
    
    foreach ($Job in $CompletedJobs) {
        $Result = $Job.PowerShell.EndInvoke($Job.Handle)
        $Job.PowerShell.Dispose()
        
        if ($Result) {
            $Count = $Result.Count
            $TotalHits += $Count
            Write-Host "[$($Result.File)] Found $Count hits" -ForegroundColor Green
            
            if ($OutputPath) {
                # Write to file
                $Result.Formatted | Out-File -FilePath $OutputPath -Append -Encoding UTF8
            } else {
                # Print first few to console
                $Result.Formatted | Select-Object -First 5 | Write-Host
                if ($Count -gt 5) { Write-Host "... ($($Count - 5) more)" -ForegroundColor Gray }
            }
        }
        
        $CompletedCount++
        Write-Progress -Activity "Searching Logs" -Status "$CompletedCount / $TotalCount files completed" -PercentComplete (($CompletedCount / $TotalCount) * 100)
    }
    
    # Remove completed jobs from the list
    $Jobs = $Jobs | Where-Object { -not $_.Handle.IsCompleted }
    Start-Sleep -Milliseconds 100
}

$RunspacePool.Close()
$RunspacePool.Dispose()

Write-Host "Search completed. Total hits: $TotalHits"
if ($OutputPath) {
    Write-Host "All results saved to: $OutputPath"
}
