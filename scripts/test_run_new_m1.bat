@echo off
setlocal EnableDelayedExpansion

::======================================================================
:: SPARTAN v9 - Incremental Analysis Pipeline Orchestrator
:: RESUMING FROM STAGE 1 ANALYSIS (Assumes Scan/Inspect Done)
::======================================================================

:: Set console to UTF-8
chcp 65001 >nul

:: --- Configuration ---

:: 1. Define Stages (Days Back Start/End) - Needed for Stages 2+
set STAGE1_DAYS_START=1 :: *** This SHOULD MATCH the original scan run ***
set STAGE1_DAYS_END=0

set STAGE2_DAYS_START=30
set STAGE2_DAYS_END=7

set STAGE3_DAYS_START=90
set STAGE3_DAYS_END=30

:: Add more stages here if needed


:: 2. Define Filter Thresholds per Stage
::    **TUNE THESE BASED ON YOUR FINDINGS!**
:: Stage 1 - Lenient Filtering
set LOW_VOL_PCT_S1=95.0
set FAST_PUMP_PCT_S1=100.0
set MIN_CONSISTENCY_S1=0.0
set MIN_BUNDLING_S1=0.0
set MIN_TOKENS_S1=2

:: Stage 2 - Moderate Filtering
set LOW_VOL_PCT_S2=70.0
set FAST_PUMP_PCT_S2=70.0
set MIN_CONSISTENCY_S2=30.0
set MIN_BUNDLING_S2=0.05
set MIN_TOKENS_S2=3

:: Stage 3 - Strict Filtering
set LOW_VOL_PCT_S3=50.0
set FAST_PUMP_PCT_S3=40.0
set MIN_CONSISTENCY_S3=50.0
set MIN_BUNDLING_S3=0.1
set MIN_TOKENS_S3=5

:: Add more stage thresholds if more stages are defined above


:: 3. Script Paths (Adjust if necessary)
set FETCH_HIST_SCRIPT=.\fetch_historical_tokens.py
set ANALYZE_STAGE_SCRIPT=.\analyze_and_filter_stage.py
set REPORT_SCRIPT=.\generate_summary_report_v9.py
set RANK_OPTIMIZE_SCRIPT=.\rank_and_optimize_v9.py

:: 4. Master DB Location
set MASTER_DB_DIR=%ROOT_DIR%\DB


:: --- !!! IMPORTANT: Set ROOT_DIR to the PREVIOUS run's directory !!! ---
set ROOT_DIR=pipeline_v9_incr_20250419_001829

echo Using existing pipeline directory: %ROOT_DIR%
if not exist "%ROOT_DIR%" (
    echo ERROR: Specified ROOT_DIR does not exist: %ROOT_DIR%. Exiting.
    goto :end
)

:: Define stage-specific directories based on ROOT_DIR
set SCAN_DIR=%ROOT_DIR%\01_scan_tokens
set INSPECT_DIR=%ROOT_DIR%\02_inspect_creators
set ANALYZE1_DIR=%ROOT_DIR%\03_analysis_stage1
set FETCH2_DIR=%ROOT_DIR%\04_fetch_stage2
set ANALYZE2_DIR=%ROOT_DIR%\05_analysis_stage2
set FETCH3_DIR=%ROOT_DIR%\06_fetch_stage3
set ANALYZE3_DIR=%ROOT_DIR%\07_analysis_stage3
set OPTIMIZE_DIR=%ROOT_DIR%\08_optimization_results
:: Add more directories if more stages are defined

:: --- Execution ---

:: ========================== STAGE 1 (Scan/Inspect SKIPPED, Start Analysis) ==========================
echo.
echo [--- Resuming at Stage 1 Analysis (%STAGE1_DAYS_END%-%STAGE1_DAYS_START% days ago) ---]

:: Find the existing creator_analysis JSON file from inspect step
set "INSPECT_OUT_JSON="
for /f "delims=" %%F in ('dir /b /o-d /a-d "%INSPECT_DIR%\creator_analysis_*.json"') do (
    set "INSPECT_OUT_JSON=%INSPECT_DIR%\%%F"
    goto :found_inspect_json_s1_resume
)
echo ERROR: Could not find existing creator_analysis_*.json in %INSPECT_DIR%
echo Ensure Steps 1a and 1b completed successfully in the specified ROOT_DIR.
goto :cleanup
:found_inspect_json_s1_resume
echo Using existing inspect output: !INSPECT_OUT_JSON!

:: 1c. Analyze Stage 1 tokens with lenient filters
echo Running %ANALYZE_STAGE_SCRIPT% for Stage 1...
mkdir "%ANALYZE1_DIR%" > nul 2>&1
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

:: 1d. Generate Stage 1 Report
echo Generating Stage 1 Summary Report...
if exist "%STAGE1_OUT_JSON%" (
    python %REPORT_SCRIPT% ^
        --input-json "%STAGE1_OUT_JSON%" ^
        --output-dir "%ANALYZE1_DIR%"
    :: Make sure no stray comments here
) else (
    echo WARNING: Stage 1 output JSON not found after analysis, skipping report generation.
)

echo [--- Stage 1 Complete ---]


:: ========================== STAGE 2 (Fetch & Analyze History) ==========================
:: (Keep Stage 2 block exactly as it was in the full BAT script)
echo.
echo [--- Starting Stage 2: Fetch History (%STAGE2_DAYS_END%-%STAGE2_DAYS_START% days ago) ---]
if not exist "%STAGE1_OUT_JSON%" ( ... goto :skip_to_end )
echo Running %FETCH_HIST_SCRIPT% for Stage 2...
mkdir "%FETCH2_DIR%" > nul 2>&1
set STAGE2_FETCH_OUT_JSON=%FETCH2_DIR%\creator_tokens_stage2_new.json
python %FETCH_HIST_SCRIPT% ^
    --input-creators-json "%STAGE1_OUT_JSON%" ^
    --days-back-start %STAGE2_DAYS_START% ^
    --days-back-end %STAGE2_DAYS_END% ^
    --output-tokens-json "%STAGE2_FETCH_OUT_JSON%"
if %ERRORLEVEL% neq 0 ( echo ERROR in %FETCH_HIST_SCRIPT% for Stage 2. & goto :cleanup )
echo Running %ANALYZE_STAGE_SCRIPT% for Stage 2...
mkdir "%ANALYZE2_DIR%" > nul 2>&1
set STAGE2_OUT_PKL=%ANALYZE2_DIR%\analysis_results_stage2.pkl
set STAGE2_OUT_JSON=%ANALYZE2_DIR%\interesting_creators_stage2.json
if not exist "%STAGE2_FETCH_OUT_JSON%" ( ... goto :skip_stage2_analysis )
if not exist "%STAGE1_OUT_PKL%" ( ... goto :skip_stage2_analysis )
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
:skip_stage2_analysis
echo Generating Stage 2 Summary Report...
if exist "%STAGE2_OUT_JSON%" (
    python %REPORT_SCRIPT% ^
        --input-json "%STAGE2_OUT_JSON%" ^
        --output-dir "%ANALYZE2_DIR%"
) else ( ... )
echo [--- Stage 2 Complete ---]


:: ========================== STAGE 3 (Fetch & Analyze Deeper History) ==========================
:: (Keep Stage 3 block exactly as it was)
echo.
echo [--- Starting Stage 3: Fetch History (%STAGE3_DAYS_END%-%STAGE3_DAYS_START% days ago) ---]
if not exist "%STAGE2_OUT_JSON%" ( ... goto :skip_to_end )
echo Running %FETCH_HIST_SCRIPT% for Stage 3...
:: ... (fetch call) ...
if %ERRORLEVEL% neq 0 ( echo ERROR in %FETCH_HIST_SCRIPT% for Stage 3. & goto :cleanup )
echo Running %ANALYZE_STAGE_SCRIPT% for Stage 3...
:: ... (checks and analyze call) ...
if %ERRORLEVEL% neq 0 ( echo ERROR in %ANALYZE_STAGE_SCRIPT% for Stage 3. & goto :cleanup )
:skip_stage3_analysis
echo Generating Stage 3 Summary Report...
:: ... (report call) ...
echo [--- Stage 3 Complete ---]


:: ========================== FINAL STEP - Rank & Optimize ==========================
:: (Keep Rank & Optimize block exactly as it was)
echo.
echo [--- Preparing for Rank & Optimize ---]
set FINAL_ANALYSIS_PKL=%STAGE3_OUT_PKL%
set OPTIMIZE_REPORT_DIR=%ROOT_DIR%\08_optimization_results
if not exist "%FINAL_ANALYSIS_PKL%" ( ... goto :cleanup_no_opt )
echo Running %RANK_OPTIMIZE_SCRIPT% using %FINAL_ANALYSIS_PKL% ...
mkdir "%OPTIMIZE_REPORT_DIR%" > nul 2>&1
mkdir "%MASTER_DB_DIR%" > nul 2>&1
python %RANK_OPTIMIZE_SCRIPT% ^
    --input-analysis-pkl "%FINAL_ANALYSIS_PKL%" ^
    --output-dir "%OPTIMIZE_REPORT_DIR%" ^
    --output-db-dir "%MASTER_DB_DIR%"
if %ERRORLEVEL% neq 0 ( echo ERROR in %RANK_OPTIMIZE_SCRIPT%. & goto :cleanup )
echo [--- Rank & Optimize Complete ---]


:: ========================== END ==========================
:: (Keep skip_to_end, cleanup_no_opt, cleanup, end labels)
:skip_to_end
echo.
echo #### SPARTAN v9 Incremental Pipeline completed (some stages may have been skipped). ####
echo Check logs and output directories for details.
echo Final interesting creators list (after last filter): %STAGE3_OUT_JSON%
echo Optimization report: %OPTIMIZE_REPORT_DIR%\optimization_backtest_report_v9.md
echo Master DB updated in: %MASTER_DB_DIR%
echo Full results are in %ROOT_DIR%
goto :end

:cleanup_no_opt
echo WARNING: Optimization step skipped as final analysis file was missing.
goto :end

:: --- Error Handling ---
:cleanup
echo.
echo #### Pipeline failed during execution. ####
echo Check errors above.

:end
echo.
pause