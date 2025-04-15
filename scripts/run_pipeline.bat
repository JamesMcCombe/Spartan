@echo off
setlocal EnableDelayedExpansion

:: Set console to UTF-8
chcp 65001 >nul

:: Set timestamp for root folder
for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /value') do set "DATETIME=%%I"
set "TIMESTAMP=%DATETIME:~0,8%_%DATETIME:~8,6%"
set "ROOT_DIR=pipeline_%TIMESTAMP%"
echo Creating pipeline directory: %ROOT_DIR%
mkdir "%ROOT_DIR%"

:: Step 1: Scan Tokens (in .\config\ from scripts)
echo Running scan_tokens.py...
python .\scan_tokens.py --output-dir "%ROOT_DIR%" --scan-period "1"
if %ERRORLEVEL% neq 0 (
    echo Error in scan_tokens.py
    goto :cleanup
)

:: Step 2: Inspect Repeat Creators (in .\config\ from scripts)
echo Running inspect_repeat_creators.py...
python .\inspect_repeat_creators.py --input-dir "%ROOT_DIR%"
if %ERRORLEVEL% neq 0 (
    echo Error in inspect_repeat_creators.py
    goto :cleanup
)

:: Step 3: Bulk Token Analysis (in .\scripts\)
echo Running bulk_token_analysis.py...
python .\bulk_token_analysis.py --input-dir "%ROOT_DIR%"
if %ERRORLEVEL% neq 0 (
    echo Error in bulk_token_analysis.py
    goto :cleanup
)

:: Step 4: Rank Creators (in .\scripts\)
echo Running rank_creators.py...
python .\rank_creators.py --input-dir "%ROOT_DIR%"
if %ERRORLEVEL% neq 0 (
    echo Error in rank_creators.py
    goto :cleanup
)

:: Step 5: Bulk Deep Creator Analysis (in .\scripts\)
echo Running bulk_deep_creator_analysis.py...
python .\bulk_deep_creator_analysis.py --input-dir "%ROOT_DIR%"
if %ERRORLEVEL% neq 0 (
    echo Error in bulk_deep_creator_analysis.py
    goto :cleanup
)

:: Step 6: Bulk Optimization Strategy (in .\scripts\)
echo Running bulk_optimization_strategy.py...
python .\bulk_optimization_strategy.py --input-dir "%ROOT_DIR%"
if %ERRORLEVEL% neq 0 (
    echo Error in bulk_optimization_strategy.py
    goto :cleanup
)

:: Step 7: Final Boss (in .\scripts\)
echo Running FINAL_BOSS.py...
python .\FINAL_BOSS.py --input-dir "%ROOT_DIR%"
if %ERRORLEVEL% neq 0 (
    echo Error in FINAL_BOSS.py
    goto :cleanup
)

:: Step 8: Verify Token Completeness (in .\scripts\)
echo Running verify_token_completeness.py...
python .\verify_token_completeness.py --input-dir "%ROOT_DIR%"
if %ERRORLEVEL% neq 0 (
    echo Error in verify_token_completeness.py
    goto :cleanup
)

echo #### Pipeline completed successfully. #### LFG!!!! #### Results are in %ROOT_DIR%
goto :end

:cleanup
echo Pipeline failed. FUCKKKKK.... :( Check errors above.
if exist temp_input.txt del temp_input.txt

:end
pause