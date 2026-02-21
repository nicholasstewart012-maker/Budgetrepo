# Generate-SourceTree.ps1
# This script reads the src directory and outputs a tree view to source_tree.txt

$targetDir = Join-Path $PSScriptRoot "src"
$outputFile = Join-Path $PSScriptRoot "source_tree.txt"

if (-not (Test-Path $targetDir)) {
    Write-Error "Could not find 'src' directory."
    exit 1
}

Write-Host "Generating tree for $targetDir..."

function Show-Tree($path, $indent = "") {
    $items = Get-ChildItem $path | Sort-Object PSIsContainer, Name -Descending
    $count = $items.Count
    for ($i = 0; $i -lt $count; $i++) {
        $item = $items[$i]
        $isLast = ($i -eq ($count - 1))
        
        # Using simple ASCII for maximum compatibility
        $prefix = if ($isLast) { "+-- " } else { "|-- " }
        
        $line = "$indent$prefix$($item.Name)"
        Add-Content -Path $outputFile -Value $line
        
        if ($item.PSIsContainer) {
            $newIndent = if ($isLast) { "$indent    " } else { "$indent|   " }
            Show-Tree $item.FullName $newIndent
        }
    }
}

# Initialize file
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
"Source Tree for src/ (Generated $timestamp)" | Out-File -FilePath $outputFile -Encoding ascii
"====================================================" | Add-Content -Path $outputFile

Show-Tree $targetDir

Write-Host "Done! Output saved to $outputFile"
