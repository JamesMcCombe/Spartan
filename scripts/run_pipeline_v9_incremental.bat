@echo off
setlocal EnableDelayedExpansion

::======================================================================
:: SPARTAN v9 - Incremental Analysis Pipeline Orchestrator
::======================================================================

:: Set console to UTF-8
chcp 65001 >nul

:: --- Configuration ---

:: 1. Define Stages (Days Back Start/End)
::    Stage 1 analyzes tokens created from End to Start days ago.
::    Subsequent stages fetch tokens created between the new Start/End.
set STAGE1_DAYS_START=1
set STAGE1_DAYS_END=0   
:: Fetches 0-7 days ago (relative to now) for initial analysis

set STAGE2_DAYS_START=30
set STAGE2_DAYS_END=7
:: Fetches 7-30 days ago using Stage 1 survivors

set STAGE3_DAYS_START=90 
set STAGE3_DAYS_END=30
:: Fetches 30-90 days ago using Stage 2 survivors

:: Add more stages here if needed (e.g., STAGE4_DAYS_START=180, STAGE4_DAYS_END=90)


:: 2. Define Filter Thresholds per Stage (Passed as args to analyze_and_filter_stage.py)
::    Suffix corresponds to the STAGE number (e.g., _S1 for Stage 1)
::    These are examples - **TUNE THESE BASED ON YOUR FINDINGS!**

:: Stage 1 - Lenient Filtering (Focus: Eliminate obvious non-starters)
set LOW_VOL_PCT_S1=95.0
set FAST_PUMP_PCT_S1=100.0
set MIN_CONSISTENCY_S1=0.0
set MIN_BUNDLING_S1=0.0
set MIN_TOKENS_S1=2

:: Stage 2 - Moderate Filtering (Focus: Find creators showing some promise over ~30 days)
set LOW_VOL_PCT_S2=70.0
set FAST_PUMP_PCT_S2=70.0
set MIN_CONSISTENCY_S2=30.0
set MIN_BUNDLING_S2=0.05
set MIN_TOKENS_S2=3  

:: Stage 3 - Strict Filtering (Focus: High confidence signals over ~90 days)
set LOW_VOL_PCT_S3=50.0
set FAST_PUMP_PCT_S3=40.0
set MIN_CONSISTENCY_S3=50.0
set MIN_BUNDLING_S3=0.1
set MIN_TOKENS_S3=5

:: Add more stage thresholds if more stages are defined above


:: 3. Script Paths (Adjust if scripts are in a subfolder like .\scripts\)
set SCAN_SCRIPT=.\scan_tokens.py
set INSPECT_SCRIPT=.\inspect_repeat_creators.py
set FETCH_HIST_SCRIPT=.\fetch_historical_tokens.py
set ANALYZE_STAGE_SCRIPT=.\analyze_and_filter_stage.py
set RANK_OPTIMIZE_SCRIPT=.\rank_and_optimize_v9.py :: Placeholder for later

:: --- Setup ---
for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /value') do set "DATETIME=%%I"
set "TIMESTAMP=%DATETIME:~0,8%_%DATETIME:~8,6%"
set "ROOT_DIR=pipeline_v9_incr_%TIMESTAMP%"
echo Creating pipeline directory: %ROOT_DIR%
mkdir "%ROOT_DIR%"
if not exist "%ROOT_DIR%" (
    echo Failed to create directory %ROOT_DIR%. Exiting.
    goto :end
)

:: Define stage-specific directories
set SCAN_DIR=%ROOT_DIR%\01_scan_tokens
set INSPECT_DIR=%ROOT_DIR%\02_inspect_creators
set ANALYZE1_DIR=%ROOT_DIR%\03_analysis_stage1
set FETCH2_DIR=%ROOT_DIR%\04_fetch_stage2
set ANALYZE2_DIR=%ROOT_DIR%\05_analysis_stage2
set FETCH3_DIR=%ROOT_DIR%\06_fetch_stage3
set ANALYZE3_DIR=%ROOT_DIR%\07_analysis_stage3
:: Add more directories if more stages are defined

:: --- Execution ---

:: ========================== STAGE 1 (Initial Scan & Analysis) ==========================
echo.
echo [--- Starting Stage 1: Scan Recent (%STAGE1_DAYS_END%-%STAGE1_DAYS_START% days ago) ---]

:: 1a. Scan recent tokens
echo Running %SCAN_SCRIPT%...
python %SCAN_SCRIPT% --output-dir "%ROOT_DIR%" --scan-period "3" --days-back %STAGE1_DAYS_START% 
:: Note: scan_tokens logic might need slight adjustment if days-back assumes 'up to N days ago' vs 'exactly N days ago'
:: Assuming current scan_tokens handles --days-back N as "0 to N days ago". If not, adjust call or script.
if %ERRORLEVEL% neq 0 ( echo ERROR in %SCAN_SCRIPT%. & goto :cleanup )
set SCAN_OUT_FILE=!SCAN_DIR!\pump_tokens_*.json :: Rely on finding the file later

:: 1b. Inspect creators from recent scan
echo Running %INSPECT_SCRIPT%...
python %INSPECT_SCRIPT% --input-dir "%ROOT_DIR%" 
if %ERRORLEVEL% neq 0 ( echo ERROR in %INSPECT_SCRIPT%. & goto :cleanup )
set INSPECT_OUT_JSON= :: Will find latest later

:: Find the latest creator_analysis JSON file from inspect step
for /f "delims=" %%F in ('dir /b /o-d /a-d "%INSPECT_DIR%\creator_analysis_*.json"') do (
    set "INSPECT_OUT_JSON=%INSPECT_DIR%\%%F"
    goto :found_inspect_json_s1
)
echo ERROR: Could not find creator_analysis_*.json in %INSPECT_DIR%
goto :cleanup
:found_inspect_json_s1
echo Using inspect output: !INSPECT_OUT_JSON!

:: 1c. Analyze Stage 1 tokens with lenient filters
echo Running %ANALYZE_STAGE_SCRIPT% for Stage 1...
mkdir "%ANALYZE1_DIR%"
set STAGE1_OUT_PKL=%ANALYZE1_DIR%\analysis_results_stage1.pkl
set STAGE1_OUT_JSON=%ANALYZE1_DIR%\interesting_creators_stage1.json

python %ANALYZE_STAGE_SCRIPT% ^
    --input-tokens-json "!INSPECT_OUT_JSON!" ^
    --input-results-pkl "" ^
    --output-results-pkl "%STAGE1_OUT_PKL%" ^
    --output-creators-json "%STAGE1_OUT_JSON%" ^
    --filter-low-vol-pct %LOW_VOL_PCT_S1% ^
    --filter-fast-pump-pct %FAST_PUMP_PCT_S1% ^
    --filter-min-consistency %MIN_CONSISTENCY_S1% ^
    --filter-min-bundling %MIN_BUNDLING_S1% ^
    --min-token-count %MIN_TOKENS_S1%

if %ERRORLEVEL% neq 0 ( echo ERROR in %ANALYZE_STAGE_SCRIPT% for Stage 1. & goto :cleanup )
echo [--- Stage 1 Complete ---]


:: ========================== STAGE 2 (Fetch & Analyze History) ==========================
echo.
echo [--- Starting Stage 2: Fetch History (%STAGE2_DAYS_END%-%STAGE2_DAYS_START% days ago) ---]

:: 2a. Fetch historical tokens for Stage 1 survivors
echo Running %FETCH_HIST_SCRIPT% for Stage 2...
mkdir "%FETCH2_DIR%"
set STAGE2_FETCH_OUT_JSON=%FETCH2_DIR%\creator_tokens_stage2_new.json

python %FETCH_HIST_SCRIPT% ^
    --input-creators-json "%STAGE1_OUT_JSON%" ^
    --days-back-start %STAGE2_DAYS_START% ^
    --days-back-end %STAGE2_DAYS_END% ^
    --output-tokens-json "%STAGE2_FETCH_OUT_JSON%"

if %ERRORLEVEL% neq 0 ( echo ERROR in %FETCH_HIST_SCRIPT% for Stage 2. & goto :cleanup )

:: 2b. Analyze Stage 2 (combining Stage 1 results) with moderate filters
echo Running %ANALYZE_STAGE_SCRIPT% for Stage 2...
mkdir "%ANALYZE2_DIR%"
set STAGE2_OUT_PKL=%ANALYZE2_DIR%\analysis_results_stage2.pkl
set STAGE2_OUT_JSON=%ANALYZE2_DIR%\interesting_creators_stage2.json

python %ANALYZE_STAGE_SCRIPT% ^
    --input-tokens-json "%STAGE2_FETCH_OUT_JSON%" ^
    --input-results-pkl "%STAGE1_OUT_PKL%" ^
    --output-results-pkl "%STAGE2_OUT_PKL%" ^
    --output-creators-json "%STAGE2_OUT_JSON%" ^
    --filter-low-vol-pct %LOW_VOL_PCT_S2% ^
    --filter-fast-pump-pct %FAST_PUMP_PCT_S2% ^
    --filter-min-consistency %MIN_CONSISTENCY_S2% ^
    --filter-min-bundling %MIN_BUNDLING_S2% ^
    --min-token-count %MIN_TOKENS_S2%

if %ERRORLEVEL% neq 0 ( echo ERROR in %ANALYZE_STAGE_SCRIPT% for Stage 2. & goto :cleanup )
echo [--- Stage 2 Complete ---]


:: ========================== STAGE 3 (Fetch & Analyze Deeper History) ==========================
echo.
echo [--- Starting Stage 3: Fetch History (%STAGE3_DAYS_END%-%STAGE3_DAYS_START% days ago) ---]

:: 3a. Fetch historical tokens for Stage 2 survivors
echo Running %FETCH_HIST_SCRIPT% for Stage 3...
mkdir "%FETCH3_DIR%"
set STAGE3_FETCH_OUT_JSON=%FETCH3_DIR%\creator_tokens_stage3_new.json

python %FETCH_HIST_SCRIPT% ^
    --input-creators-json "%STAGE2_OUT_JSON%" ^
    --days-back-start %STAGE3_DAYS_START% ^
    --days-back-end %STAGE3_DAYS_END% ^
    --output-tokens-json "%STAGE3_FETCH_OUT_JSON%"

if %ERRORLEVEL% neq 0 ( echo ERROR in %FETCH_HIST_SCRIPT% for Stage 3. & goto :cleanup )

:: 3b. Analyze Stage 3 (combining Stage 2 results) with strict filters
echo Running %ANALYZE_STAGE_SCRIPT% for Stage 3...
mkdir "%ANALYZE3_DIR%"
set STAGE3_OUT_PKL=%ANALYZE3_DIR%\analysis_results_stage3.pkl
set STAGE3_OUT_JSON=%ANALYZE3_DIR%\interesting_creators_stage3.json

python %ANALYZE_STAGE_SCRIPT% ^
    --input-tokens-json "%STAGE3_FETCH_OUT_JSON%" ^
    --input-results-pkl "%STAGE2_OUT_PKL%" ^
    --output-results-pkl "%STAGE3_OUT_PKL%" ^
    --output-creators-json "%STAGE3_OUT_JSON%" ^
    --filter-low-vol-pct %LOW_VOL_PCT_S3% ^
    --filter-fast-pump-pct %FAST_PUMP_PCT_S3% ^
    --filter-min-consistency %MIN_CONSISTENCY_S3% ^
    --filter-min-bundling %MIN_BUNDLING_S3% ^
    --min-token-count %MIN_TOKENS_S3%

if %ERRORLEVEL% neq 0 ( echo ERROR in %ANALYZE_STAGE_SCRIPT% for Stage 3. & goto :cleanup )
echo [--- Stage 3 Complete ---]


:: ========================== FINAL STEPS (Placeholders) ==========================
echo.
echo [--- Preparing for Final Steps ---]
echo Next: Run %RANK_OPTIMIZE_SCRIPT% using the final analysis results:
echo --input-analysis-pkl "%STAGE3_OUT_PKL%" 
echo --output-db-dir "path\to\ALL TRADE CREATORS" 
echo (Script %RANK_OPTIMIZE_SCRIPT% needs to be adapted for v9)
echo.
echo Then: Run FINAL_BOSS.py using the updated database.

:: --- Success ---
echo.
echo #### SPARTAN v9 Incremental Pipeline (up to Stage 3 Analysis) completed successfully. ####
echo Final interesting creators list (after Stage 3 filters): %STAGE3_OUT_JSON%
echo Full analysis data for these creators (input for optimization): %STAGE3_OUT_PKL%
echo Results are in %ROOT_DIR%
goto :end

:: --- Error Handling ---
:cleanup
echo.
echo #### Pipeline failed during execution. ####
echo Check errors above.

:end
echo.
pause