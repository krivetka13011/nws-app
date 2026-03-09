@echo off
cd /d "%~dp0"
echo Git push from: %CD%
git add -A
git status
git commit -m "Update" 2>nul
git push
pause
