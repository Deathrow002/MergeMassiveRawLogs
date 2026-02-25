# ParseLogs.ps1
# PowerShell script to parse raw logs and convert to structured JSON format
# Optimized for massive log files (tested up to 50GB+)
# Uses parallel processing + streaming I/O to minimize memory usage
#
# Output: JSON format (.json) - standard JSON array
#
# Usage:
#   .\ParseLogs.ps1 -InputPath ".\data\input" -OutputPath ".\data\output\parsed.json"
#   .\ParseLogs.ps1 -InputFile ".\data\input\log1.txt" -OutputPath ".\output.json"
#   .\ParseLogs.ps1 -InputPath ".\logs" -SplitOutput -MaxEntriesPerFile 500000
#   .\ParseLogs.ps1 -InputPath ".\logs" -MaxThreads 16

param(
    [string]$InputPath = ".\data\input",
    [string]$InputFile = "",
    [string]$OutputPath = ".\data\output\parsed.json",
    [int]$MaxThreads = [Environment]::ProcessorCount,
    [switch]$SplitOutput,     # Split into multiple output files
    [int]$MaxEntriesPerFile = 100000,  # Max entries per output file when splitting
    [switch]$OneToOne         # Create one JSON file per input log file (matches filename)
)

function Parse-LogEntry {
    param([string]$RawEntry)
    
    $Entry = @{
        Timestamp = $null
        ThreadName = $null
        Level = $null
        TagId = $null
        ClassName = $null
        Action = $null
        CamelBreadcrumbId = $null
        ResponseCode = $null
        Url = $null
        ExecutionTimeMs = $null
        ConnectTimeout = $null
        ReadTimeout = $null
        RequestMethod = $null
        Headers = @{}
        Body = $null
    }
    
    $Lines = $RawEntry -split "`n" | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" -and $_ -ne "--------------------------------------" }
    
    if ($Lines.Count -eq 0) { return $null }
    
    # Parse header line: [thread][level][timestamp][tagId:xxx][class] [ACTION][camelId]
    $HeaderLine = $Lines[0]
    
    # Extract thread name
    if ($HeaderLine -match '^\[([^\]]+)\]') {
        $Entry.ThreadName = $Matches[1]
    }
    
    # Extract level
    if ($HeaderLine -match '\]\[(INFO|DEBUG|WARN|ERROR|TRACE)\]\[') {
        $Entry.Level = $Matches[1]
    }
    
    # Extract timestamp
    if ($HeaderLine -match '\[(\d{4}-\d{2}-\d{2}T[\d:\.]+[+-]\d{4})\]') {
        $Entry.Timestamp = $Matches[1]
    }
    
    # Extract tagId
    if ($HeaderLine -match '\[tagId:([^\]]+)\]') {
        $Entry.TagId = $Matches[1]
    }
    
    # Extract class name (e.g., com.sde.modelsuite.proxy.BaseProxy)
    if ($HeaderLine -match '\[(com\.[a-zA-Z\._]+)\]') {
        $Entry.ClassName = $Matches[1]
    }
    
    # Extract action (SEND/RECEIVE)
    if ($HeaderLine -match '\[(SEND|RECEIVE)\]') {
        $Entry.Action = $Matches[1]
    }
    
    # Extract camelBreadcrumbId
    if ($HeaderLine -match '\[camelbreadcrumbId:([^\]]+)\]') {
        $Entry.CamelBreadcrumbId = $Matches[1]
    }
    
    # Parse remaining lines
    $JsonStartIndex = -1
    $HeadersSection = $false
    
    for ($i = 1; $i -lt $Lines.Count; $i++) {
        $Line = $Lines[$i]
        
        # Check if this is the start of JSON body
        if ($Line -match '^\{') {
            $JsonStartIndex = $i
            break
        }
        
        # Parse Response Code (RECEIVE)
        if ($Line -match 'Response Code = \[(\d+)\]') {
            $Entry.ResponseCode = [int]$Matches[1]
        }
        
        # Parse Receive URL and execution time
        if ($Line -match 'Receive URL = \[([^\]]+)\], execution time = \[(\d+)\]') {
            $Entry.Url = $Matches[1]
            $Entry.ExecutionTimeMs = [int]$Matches[2]
        }
        
        # Parse Send URL with parameters
        if ($Line -match 'Send URL = \[([^\]]+)\]') {
            $Entry.Url = $Matches[1]
            
            if ($Line -match 'connectTimeout = \[(\d+)\]') {
                $Entry.ConnectTimeout = [int]$Matches[1]
            }
            if ($Line -match 'readTimeout = \[(\d+)\]') {
                $Entry.ReadTimeout = [int]$Matches[1]
            }
            if ($Line -match 'requestMethod = \[([^\]]+)\]') {
                $Entry.RequestMethod = $Matches[1]
            }
        }
        
        # Parse headers (Key : Value format)
        if ($Line -match '^([A-Za-z\-]+)\s*:\s*(.+)$') {
            $HeaderName = $Matches[1].Trim()
            $HeaderValue = $Matches[2].Trim()
            $Entry.Headers[$HeaderName] = $HeaderValue
        }
    }
    
    # Extract JSON body
    if ($JsonStartIndex -ge 0) {
        $JsonLines = $Lines[$JsonStartIndex..($Lines.Count - 1)] -join "`n"
        try {
            $Entry.Body = $JsonLines | ConvertFrom-Json
        }
        catch {
            $Entry.Body = $JsonLines  # Keep as raw string if not valid JSON
        }
    }
    
    # Clean up null/empty values
    $CleanEntry = @{}
    foreach ($Key in $Entry.Keys) {
        $Value = $Entry[$Key]
        if ($null -ne $Value) {
            if ($Value -is [hashtable] -and $Value.Count -eq 0) { continue }
            if ($Value -is [string] -and [string]::IsNullOrEmpty($Value)) { continue }
            $CleanEntry[$Key] = $Value
        }
    }
    
    return $CleanEntry
}

function Parse-LogFile {
    param([string]$FilePath)
    
    $Content = [System.IO.File]::ReadAllText($FilePath, [System.Text.Encoding]::UTF8)
    
    # Split by separator line
    $RawEntries = $Content -split '(?=--------------------------------------\r?\n\[)'
    
    $ParsedEntries = @()
    
    foreach ($RawEntry in $RawEntries) {
        if ([string]::IsNullOrWhiteSpace($RawEntry)) { continue }
        
        $Parsed = Parse-LogEntry -RawEntry $RawEntry
        if ($null -ne $Parsed -and $Parsed.Count -gt 0) {
            $ParsedEntries += $Parsed
        }
    }
    
    return $ParsedEntries
}

# Main execution
Write-Host "Log Parser - Converting raw logs to structured JSON (Parallel + Streaming)"
Write-Host "==========================================================================="
if ($OneToOne) {
    Write-Host "Mode: One-to-One (1 log file -> 1 JSON file)"
} else {
    Write-Host "Mode: $(if ($SplitOutput) { 'Split output files' } else { 'Single JSON file' })"
}
Write-Host "Max threads: $MaxThreads"

# Create output directory if needed
if ($OneToOne) {
    # In OneToOne mode, OutputPath is treated as the output DIRECTORY
    # If it has an extension, we strip it and use the parent folder
    if ([System.IO.Path]::HasExtension($OutputPath)) {
        $OutputDir = Split-Path $OutputPath -Parent
    } else {
        $OutputDir = $OutputPath
    }
} else {
    $OutputDir = Split-Path $OutputPath -Parent
}

if ($OutputDir -and -not (Test-Path $OutputDir)) {
    New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
}

# Output configuration
$OutputBaseName = [System.IO.Path]::GetFileNameWithoutExtension($OutputPath)
$OutputExtension = ".json"
$OutputDirectory = if ($OutputDir) { $OutputDir } else { "." }
# FinalOutputPath used only for Single/Split mode
$FinalOutputPath = Join-Path $OutputDirectory "$OutputBaseName$OutputExtension"

# Collect all entries
$AllEntries = [System.Collections.Generic.List[object]]::new()
$CurrentFileIndex = 0

function Add-EntryToCollection {
    param($Entry)
    
    $script:AllEntries.Add($Entry)
    
    # Split file if needed
    if ($SplitOutput -and $script:AllEntries.Count -ge $MaxEntriesPerFile) {
        $FilePath = if ($script:CurrentFileIndex -eq 0) {
            Join-Path $OutputDirectory "$OutputBaseName$OutputExtension"
        } else {
            Join-Path $OutputDirectory "$OutputBaseName`_$($script:CurrentFileIndex.ToString('D4'))$OutputExtension"
        }
        
        $JsonOutput = $script:AllEntries.ToArray() | ConvertTo-Json -Depth 10
        [System.IO.File]::WriteAllText($FilePath, $JsonOutput, [System.Text.Encoding]::UTF8)
        Write-Host "  Wrote $($script:AllEntries.Count) entries to: $([System.IO.Path]::GetFileName($FilePath))"
        
        $script:CurrentFileIndex++
        $script:AllEntries.Clear()
    }
}

# Runspace script block - returns parsed entries array
$ProcessFileScriptBlock = {
    param([string]$FilePath)
    
    $Results = [System.Collections.Generic.List[hashtable]]::new()
    
    # Inline parse function
    function Parse-Entry {
        param([string]$RawEntry)
        
        $Entry = @{
            Timestamp = $null; ThreadName = $null; Level = $null; TagId = $null
            ClassName = $null; Action = $null; CamelBreadcrumbId = $null
            ResponseCode = $null; Url = $null; ExecutionTimeMs = $null
            ConnectTimeout = $null; ReadTimeout = $null; RequestMethod = $null
            Headers = @{}; Body = $null
        }
        
        $Lines = $RawEntry -split "`n" | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" -and $_ -ne "--------------------------------------" }
        if ($Lines.Count -eq 0) { return $null }
        
        $HeaderLine = $Lines[0]
        if ($HeaderLine -match '^\[([^\]]+)\]') { $Entry.ThreadName = $Matches[1] }
        if ($HeaderLine -match '\]\[(INFO|DEBUG|WARN|ERROR|TRACE)\]\[') { $Entry.Level = $Matches[1] }
        if ($HeaderLine -match '\[(\d{4}-\d{2}-\d{2}T[\d:\.]+[+-]\d{4})\]') { $Entry.Timestamp = $Matches[1] }
        if ($HeaderLine -match '\[tagId:([^\]]+)\]') { $Entry.TagId = $Matches[1] }
        if ($HeaderLine -match '\[(com\.[a-zA-Z\._]+)\]') { $Entry.ClassName = $Matches[1] }
        if ($HeaderLine -match '\[(SEND|RECEIVE)\]') { $Entry.Action = $Matches[1] }
        if ($HeaderLine -match '\[camelbreadcrumbId:([^\]]+)\]') { $Entry.CamelBreadcrumbId = $Matches[1] }
        
        $JsonStartIndex = -1
        for ($i = 1; $i -lt $Lines.Count; $i++) {
            $Line = $Lines[$i]
            if ($Line -match '^\{') { $JsonStartIndex = $i; break }
            if ($Line -match 'Response Code = \[(\d+)\]') { $Entry.ResponseCode = [int]$Matches[1] }
            if ($Line -match 'Receive URL = \[([^\]]+)\], execution time = \[(\d+)\]') {
                $Entry.Url = $Matches[1]; $Entry.ExecutionTimeMs = [int]$Matches[2]
            }
            if ($Line -match 'Send URL = \[([^\]]+)\]') {
                $Entry.Url = $Matches[1]
                if ($Line -match 'connectTimeout = \[(\d+)\]') { $Entry.ConnectTimeout = [int]$Matches[1] }
                if ($Line -match 'readTimeout = \[(\d+)\]') { $Entry.ReadTimeout = [int]$Matches[1] }
                if ($Line -match 'requestMethod = \[([^\]]+)\]') { $Entry.RequestMethod = $Matches[1] }
            }
            if ($Line -match '^([A-Za-z\-]+)\s*:\s*(.+)$') {
                $Entry.Headers[$Matches[1].Trim()] = $Matches[2].Trim()
            }
        }
        
        if ($JsonStartIndex -ge 0) {
            $JsonLines = $Lines[$JsonStartIndex..($Lines.Count - 1)] -join "`n"
            try { $Entry.Body = $JsonLines | ConvertFrom-Json } catch { $Entry.Body = $JsonLines }
        }
        
        # Clean up
        $CleanEntry = @{}
        foreach ($Key in $Entry.Keys) {
            $Value = $Entry[$Key]
            if ($null -ne $Value) {
                if ($Value -is [hashtable] -and $Value.Count -eq 0) { continue }
                if ($Value -is [string] -and [string]::IsNullOrEmpty($Value)) { continue }
                $CleanEntry[$Key] = $Value
            }
        }
        return $CleanEntry
    }
    
    $FileSize = (Get-Item $FilePath).Length
    
    if ($FileSize -gt 100MB) {
        # Large file: stream read
        $Reader = [System.IO.StreamReader]::new($FilePath, [System.Text.Encoding]::UTF8)
        $Buffer = [System.Text.StringBuilder]::new()
        $Separator = "--------------------------------------"
        
        try {
            while (-not $Reader.EndOfStream) {
                $Line = $Reader.ReadLine()
                
                if ($Line -eq $Separator -and $Buffer.Length -gt 0) {
                    $RawEntry = $Buffer.ToString()
                    $Parsed = Parse-Entry -RawEntry $RawEntry
                    if ($null -ne $Parsed -and $Parsed.Count -gt 0) {
                        $Results.Add($Parsed)
                    }
                    $Buffer.Clear()
                }
                [void]$Buffer.AppendLine($Line)
            }
            
            if ($Buffer.Length -gt 0) {
                $RawEntry = $Buffer.ToString()
                $Parsed = Parse-Entry -RawEntry $RawEntry
                if ($null -ne $Parsed -and $Parsed.Count -gt 0) {
                    $Results.Add($Parsed)
                }
            }
        }
        finally {
            $Reader.Close()
        }
    }
    else {
        # Small file: load into memory
        $Content = [System.IO.File]::ReadAllText($FilePath, [System.Text.Encoding]::UTF8)
        $RawEntries = $Content -split '(?=--------------------------------------\r?\n\[)'
        
        foreach ($RawEntry in $RawEntries) {
            if ([string]::IsNullOrWhiteSpace($RawEntry)) { continue }
            $Parsed = Parse-Entry -RawEntry $RawEntry
            if ($null -ne $Parsed -and $Parsed.Count -gt 0) {
                $Results.Add($Parsed)
            }
        }
    }
    
    return @{
        FileName = [System.IO.Path]::GetFileName($FilePath)
        Entries = $Results.ToArray()
        Count = $Results.Count
    }
}

try {
    # Main logic
    if ($OneToOne) {
        if ($InputFile) {
            # Single file input in OneToOne mode
            Write-Host "Single file: $InputFile"
            $Base = [System.IO.Path]::GetFileNameWithoutExtension($InputFile)
            $Output = Join-Path $OutputDirectory "$Base.json"
            $FinalOutputPath = $Output
            # Use streaming mode as fallback since it handles memory well
        } 
        # For directory, logic falls through to directory block below
    }

    if ($InputFile -ne "" -and (Test-Path $InputFile -PathType Leaf)) {
        # Single file mode - STREAMING PROCESS to avoid OOM
        # If OneToOne is set, we just set the output path correctly above basically.
        
        Write-Host "Parsing single file: $InputFile"
        $FileSize = (Get-Item $InputFile).Length

        Write-Host "File size: $([math]::Round($FileSize / 1GB, 2)) GB"
        Write-Host "Using STREAMING MODE to avoid memory issues."
        
        # Prepare output
        $Writer = [System.IO.StreamWriter]::new($FinalOutputPath, $false, [System.Text.Encoding]::UTF8)
        $Writer.Write("[")
        $FirstEntry = $true
        $EntryCount = 0
        
        # Setup Reader
        $Reader = [System.IO.StreamReader]::new($InputFile, [System.Text.Encoding]::UTF8)
        $Buffer = [System.Text.StringBuilder]::new()
        $Separator = "--------------------------------------"
        
        # Re-using the Parse-LogEntry function logic inline or from global scope if available.
        # But Parse-LogEntry is defined globally at line 18, so we can use it!

        try {
            while ($null -ne ($Line = $Reader.ReadLine())) {
                if ($Line -eq $Separator -and $Buffer.Length -gt 0) {
                    $RawEntry = $Buffer.ToString()
                    $Parsed = Parse-LogEntry -RawEntry $RawEntry
                    
                    if ($null -ne $Parsed -and $Parsed.Count -gt 0) {
                        if (-not $FirstEntry) { $Writer.Write(",") }
                        
                        $JsonFragment = $Parsed | ConvertTo-Json -Depth 10 -Compress
                        $Writer.Write($JsonFragment)
                        $FirstEntry = $false
                        $EntryCount++
                        
                        if ($EntryCount % 1000 -eq 0) {
                            Write-Host "Processed $EntryCount entries..." -ForegroundColor Cyan
                        }
                    }
                    $Buffer.Clear()
                }
                [void]$Buffer.AppendLine($Line)
            }
            
            # Flush remaining buffer
            if ($Buffer.Length -gt 0) {
                 $RawEntry = $Buffer.ToString()
                 $Parsed = Parse-LogEntry -RawEntry $RawEntry
                 if ($null -ne $Parsed -and $Parsed.Count -gt 0) {
                    if (-not $FirstEntry) { $Writer.Write(",") }
                    $JsonFragment = $Parsed | ConvertTo-Json -Depth 10 -Compress
                    $Writer.Write($JsonFragment)
                    $EntryCount++
                 }
            }
            
            $Writer.Write("]")
            $Writer.Close()
            $Reader.Close()
            
            Write-Host "`nParsed $EntryCount entries."
            Write-Host "Output written to: $FinalOutputPath"
        }
        catch {
            Write-Error "Error during streaming: $_"
            if ($null -ne $Writer) { $Writer.Close() }
            if ($null -ne $Reader) { $Reader.Close() }
        }
    }
    elseif (Test-Path $InputPath -PathType Container) {
        # Directory mode with parallel processing
        $Files = Get-ChildItem -Path $InputPath -Include "*.log","*.txt" -File -Recurse | Sort-Object Name
        $TotalFiles = $Files.Count
        $TotalSize = ($Files | Measure-Object -Property Length -Sum).Sum
        
        if ($TotalFiles -eq 0) {
            Write-Host "No .log files found in input directory."
            $Writer.Close()
            exit 0
        }
        
        Write-Host "Found $TotalFiles files ($([math]::Round($TotalSize / 1GB, 2)) GB) to parse..."
        $StartTime = Get-Date
        
        # Create runspace pool
        $RunspacePool = [System.Management.Automation.Runspaces.RunspaceFactory]::CreateRunspacePool(1, $MaxThreads)
        $RunspacePool.Open()
        
        $Jobs = [System.Collections.Generic.List[object]]::new()
        
        # Submit all files to runspace pool
        foreach ($File in $Files) {
            $PowerShell = [System.Management.Automation.PowerShell]::Create()
            $PowerShell.RunspacePool = $RunspacePool
            
            [void]$PowerShell.AddScript($ProcessFileScriptBlock)
            [void]$PowerShell.AddParameter("FilePath", $File.FullName)
            
            $Jobs.Add(@{
                PowerShell = $PowerShell
                Handle = $PowerShell.BeginInvoke()
                FileName = $File.Name
            })
        }
        
        Write-Host "Processing $TotalFiles files with $MaxThreads threads..."
        Write-Host ""
        
        # Collect results as they complete (not in submission order)
        $CompletedCount = 0
        $TotalEntriesParsed = 0
        $ProcessedBytes = 0L
        $PendingJobs = [System.Collections.Generic.List[object]]::new($Jobs)
        
        if ($OneToOne) {
            $TotalEntriesWritten = 0
            Write-Host "Starting parallel processing..."
        }
        
        while ($PendingJobs.Count -gt 0) {

            $CompletedJob = $null
            
            # Find any completed job
            foreach ($Job in $PendingJobs) {
                if ($Job.Handle.IsCompleted) {
                    $CompletedJob = $Job
                    break
                }
            }
            
            if ($null -eq $CompletedJob) {
                # No job completed yet, wait briefly
                if ($OneToOne) {
                    Write-Host "." -NoNewline
                    if ($i++ % 60 -eq 0) { Write-Host "" }
                }
                Start-Sleep -Milliseconds 1000
                continue
            }
            
            # Remove from pending list
            [void]$PendingJobs.Remove($CompletedJob)
            
            try {
                $Result = $CompletedJob.PowerShell.EndInvoke($CompletedJob.Handle)
                $CompletedCount++
                
if ($OneToOne) {
            # In One-To-One mode, we write inside processing logic
            $FileBase = [System.IO.Path]::GetFileNameWithoutExtension($CompletedJob.FileName)
            $NewFileName = "$FileBase.json"
            $NewFilePath = Join-Path $OutputDirectory $NewFileName
            
            # Since Parse-Entry returns entries in memory, write them here
            if ($Result.Entries -and $Result.Entries.Count -gt 0) {
               $JsonOutput = $Result.Entries | ConvertTo-Json -Depth 10
               [System.IO.File]::WriteAllText($NewFilePath, $JsonOutput, [System.Text.Encoding]::UTF8)
               $TotalEntriesWritten += $Result.Entries.Count
               Write-Host "  Written to: $NewFileName"
            }
        }
        else {
             if ($Result -and $Result.Entries) {
                 $TotalEntriesParsed += $Result.Entries.Count
                 foreach ($Entry in $Result.Entries) {
                     Add-EntryToCollection -Entry $Entry
                 }
             }
        }
                # Get file size for progress calculation
                $FileInfo = $Files | Where-Object { $_.Name -eq $CompletedJob.FileName } | Select-Object -First 1
                if ($FileInfo) {
                    $ProcessedBytes += $FileInfo.Length
                }
                
                # Show progress for each file (like MergeLogs)
                $ProgressPct = [math]::Round(($ProcessedBytes / $TotalSize) * 100, 1)
                $EntryCount = if ($Result) { $Result.Count } else { 0 }
                Write-Host "[$ProgressPct%] Parsed: $($CompletedJob.FileName) ($EntryCount entries)"
            }
            catch {
                Write-Host "[ERROR] $($CompletedJob.FileName): $_"
            }
            finally {
                $CompletedJob.PowerShell.Dispose()
            }
        }
        
        # Final progress report
        $FinalElapsed = (Get-Date) - $StartTime
        
        $RunspacePool.Close()
        $RunspacePool.Dispose()
        
        Write-Host ""
        Write-Host "Parallel processing completed in $($FinalElapsed.TotalSeconds.ToString('F2')) seconds"
    }
    else {
        Write-Error "Input path does not exist: $InputPath"
        exit 1
    }
}
catch {
    Write-Error "Error during processing: $_"
    exit 1
}

# Write remaining entries to final file
if (-not $OneToOne) {
    if ($AllEntries.Count -gt 0) {
        $FilePath = if ($CurrentFileIndex -eq 0) {
            $FinalOutputPath
        } else {
            Join-Path $OutputDirectory "$OutputBaseName`_$($CurrentFileIndex.ToString('D4'))$OutputExtension"
        }
        
        Write-Host "Writing $($AllEntries.Count) entries to: $([System.IO.Path]::GetFileName($FilePath))"
        $JsonOutput = $AllEntries.ToArray() | ConvertTo-Json -Depth 10
        [System.IO.File]::WriteAllText($FilePath, $JsonOutput, [System.Text.Encoding]::UTF8)
    }
}

if ($OneToOne) {
    # No-op, already written
} else {
    $TotalEntriesWritten = $AllEntries.Count + ($CurrentFileIndex * $MaxEntriesPerFile)
}




Write-Host ""
Write-Host "========================================="
Write-Host "Parse completed successfully!"
Write-Host "Total entries written: $TotalEntriesWritten"
if ($SplitOutput -and $CurrentFileIndex -gt 0) {
    Write-Host "Output files: $($CurrentFileIndex + 1) files created"
}
if ($OneToOne) {
    Write-Host "Output Directory: $OutputDirectory"
    Write-Host "Format: JSON (.json) per file"
} else {
    Write-Host "Output: $FinalOutputPath"
    Write-Host "Format: JSON (.json)"
}
