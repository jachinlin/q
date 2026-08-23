export type ApiError = {
  error: {
    code: string
    message: string
    severity: string
    retryable: boolean
    remediation: string | null
    request_id: string
  }
}

export type NotebookStatus = {
  status: 'READY' | 'UNAVAILABLE'
}

export type Task = {
  id: string
  subject_kind: string | null
  subject_id: string | null
  task_type: string
  status: string
  priority: number
  progress: Record<string, unknown>
  created_at: string
  started_at: string | null
  updated_at: string
  heartbeat_at: string | null
  completed_at: string | null
  worker_id: string | null
  error: Record<string, unknown> | null
  result: Record<string, unknown> | null
}

export type TaskAttempt = {
  id: string
  attempt_no: number
  status: string
  worker_id: string | null
  started_at: string
  heartbeat_at: string | null
  completed_at: string | null
  progress: Record<string, unknown>
  error: Record<string, unknown> | null
  has_log: boolean
}

export type TaskDetail = Task & {
  payload: Record<string, unknown>
  attempts: TaskAttempt[]
}

export type TaskDiagnostic = {
  code: string | null
  message: string | null
  exception_type: string | null
  stage: string | null
  retryable: boolean | null
  remediation: string | null
  traceback: string | null
}

export type TaskLog = {
  task_id: string
  attempt_id: string
  available: boolean
  lines: string[]
  total_lines: number
  truncated: boolean
  diagnostic: TaskDiagnostic | null
}

export type TaskPage = Page<Task> & { status_counts: Record<string, number> }

export type DataUpdateWindow = {
  dataset: string
  basis: 'EXPLICIT' | 'BOOTSTRAP' | 'INCREMENTAL' | 'SNAPSHOT_REFRESH' | 'DISCLOSURE_TRIGGER'
  start: string
  end: string
  overlap_days: number
  current_watermark?: string
  trigger_date?: string
}

export type ExperimentKind = 'STRATEGY_BACKTEST' | 'FACTOR_STUDY'
export type RunStatus = 'CREATED' | 'QUEUED' | 'RUNNING' | 'SUCCEEDED' | 'FAILED' | 'CANCELLED'
export type ResearchMark = 'UNREVIEWED' | 'BASELINE' | 'CANDIDATE' | 'DISCARDED'

export type ExperimentDefinitionDto = {
  name: string
  description: string
  kind: ExperimentKind
  tags: string[]
  sample_windows: Record<'train' | 'validation' | 'test', { start: string; end: string }>
  governance: { test_budget: number; correction: 'BONFERRONI' | 'BH_FDR' }
  initial_run: Record<string, unknown>
}

export type ExperimentSummary = {
  id: string
  definition: ExperimentDefinitionDto
  baseline_run_id: string | null
  created_at: string
}

export type ExperimentOverview = ExperimentSummary & {
  latest_run: ExperimentRun | null
  run_count: number
  test_uses: number
}

export type ExperimentRun = {
  id: string
  experiment_id: string
  config: Record<string, unknown>
  config_hash: string
  catalog_hash: string
  status: RunStatus
  stage: string
  research_mark: ResearchMark
  uses_test_region: boolean
  task_id: string | null
  artifact_dir: string | null
  manifest_hash: string | null
  error: Record<string, unknown> | null
  created_at: string
  started_at: string | null
  completed_at: string | null
  tags: string[]
  metrics: Array<{
    name: string
    value: number
    unit: string | null
    p_value: number | null
    adjusted_p_value: number | null
  }>
  artifacts: Array<{
    artifact_type: string
    relative_path: string
    content_hash: string
    byte_count: number
    row_count: number | null
    schema: Record<string, string> | null
  }>
}

export type ExperimentAggregate = {
  experiment: ExperimentSummary
  runs: ExperimentRun[]
  tags: string[]
}

export type ExperimentValidation = {
  config_hash: string
  normalized: ExperimentDefinitionDto
}

export type StrategyCatalog = {
  strategies: string[]
  components: Record<string, string[]>
  component_schemas: Record<string, Array<{ model_id: string; params_schema: Record<string, unknown> }>>
  capability_rules: Array<Record<string, unknown>>
}

export type DataUpdateSkip = {
  dataset: string
  reason: 'DISCLOSURE_DEADLINE_PENDING'
  trigger_date: string
}

export type DataUpdatePlan = {
  window_mode: 'AUTO_INCREMENTAL' | 'EXPLICIT'
  planned_at: string
  start: string
  end: string
  requested_start?: string
  requested_end?: string
  dataset_windows: DataUpdateWindow[]
  skipped_datasets: DataUpdateSkip[]
  plan_hash: string
}

export type Page<T> = { items: T[]; page: number; page_size: number; total: number }

export type Overview = {
  gate: {
    status: 'READY' | 'BLOCKED'
    reason: string
    catalog_hash: string
    validated_catalog_hash: string | null
    quality_run_id: string | null
    updated_at: string
    validated_at: string | null
  }
  freshness: {
    status: 'CURRENT' | 'STALE' | 'MISSING' | 'UNKNOWN'
    counts: Record<'CURRENT' | 'STALE' | 'MISSING' | 'UNKNOWN', number>
    evaluated_at: string | null
    latest_complete_session: string | null
  }
  latest_trade_date: string | null
  dataset_count: number
  gate_quality_run: QualityRun | null
  latest_quality_run: QualityRun | null
  worker: { worker_id: string | null; task_id: string; task_status: string; heartbeat_at: string | null } | null
  last_successful_update: Task | null
  tasks: {
    status_counts: Record<'QUEUED' | 'RUNNING' | 'SUCCEEDED' | 'FAILED' | 'CANCEL_REQUESTED' | 'CANCELLED' | 'ORPHANED', number>
    active: Task[]
  }
}

export type MarketReviewDates = {
  catalog_hash: string
  validated_at: string
  latest_trade_date: string
  dates: string[]
}

export type MarketReviewSeriesPoint = {
  trade_date: string
  value: number
  auxiliary: number | null
}

export type MarketReviewIndex = {
  index_id: string
  name: string
  daily_return: number | null
  amplitude: number | null
  return_5d: number | null
  return_20d: number | null
  series: MarketReviewSeriesPoint[]
}

export type MarketReview = {
  trade_date: string
  catalog_hash: string
  validated_at: string
  exclude_st: boolean
  data_quality: {
    expected_count: number
    priced_count: number
    suspended_count: number
    st_count: number
    missing_bar_count: number
    coverage_rate: number | null
  }
  indexes: MarketReviewIndex[]
  liquidity: {
    amount: number
    change_vs_previous: number | null
    average_5d: number | null
    average_20d: number | null
    percentile_20d: number | null
    series: MarketReviewSeriesPoint[]
  }
  breadth: {
    up_count: number
    down_count: number
    flat_count: number
    advance_rate: number | null
    net_advance_count: number
    equal_weight_return: number | null
    median_return: number | null
    p10_return: number | null
    p25_return: number | null
    p75_return: number | null
    p90_return: number | null
    buckets: Array<{ label: string; count: number }>
  }
  sentiment: {
    limit_up_count: number
    limit_down_count: number
    broken_limit_up_count: number
    one_price_limit_up_count: number
    eligible_count: number
    unresolved_count: number
    coverage_rate: number | null
    note: string
    events: Array<{
      instrument_id: string
      name: string
      board: string
      is_st: boolean
      pct_change: number
      amount: number | null
      event: 'LIMIT_UP' | 'LIMIT_DOWN' | 'BROKEN_LIMIT_UP' | 'ONE_PRICE_LIMIT_UP'
    }>
  }
  industries: {
    available: boolean
    taxonomy: string | null
    coverage_rate: number | null
    unavailable_reason: string | null
    items: Array<{
      industry_code: string
      industry_name: string
      equal_weight_return: number | null
      advance_rate: number | null
      amount_share: number | null
      instrument_count: number
      priced_count: number
      limit_up_count: number
      limit_down_count: number
    }>
  }
  valuation: {
    metrics: Array<{
      metric: 'pe_ttm' | 'pb_mrq' | 'ps_ttm'
      median: number | null
      p25: number | null
      p75: number | null
      valid_count: number
    }>
    turnover_median: number | null
    turnover_valid_count: number
  }
}

export type Freshness = {
  status: 'CURRENT' | 'STALE' | 'MISSING' | 'UNKNOWN'
  actual_watermark: string | null
  expected_watermark: string | null
  lag_days: number | null
  evaluated_at: string
  reason: string
  trigger_date: string | null
  update_required: boolean | null
}

export type Dataset = {
  dataset: string
  source: string | null
  start_date: string | null
  end_date: string | null
  partition_count: number
  row_count: number
  content_hash: string | null
  updated_at: string | null
  partitioning: string
  cadence: string
  fetch_granularity: string
  reuse: string
  overlap_days: number
  freshness: Freshness
  operational: {
    last_localized_at: string | null
    localized_through: string | null
    last_curated_at: string | null
    last_validated_at: string | null
  }
  quality_issue_count: number
  blocking_issue_count: number
}

export type QualityRun = {
  run_id: string
  scope: string
  input_hash: string
  status: string
  started_at: string
  completed_at: string | null
  issue_count: number
  blocking_issue_count: number
}

export type DataSummary = {
  gate: {
    status: 'READY' | 'BLOCKED'
    reason: string
    catalog_hash: string
    validated_catalog_hash: string | null
    quality_run_id: string | null
    updated_at: string
    validated_at: string | null
  }
  freshness: {
    status: Freshness['status']
    counts: Record<Freshness['status'], number>
    evaluated_at: string | null
    latest_complete_session: string | null
  }
  gate_quality_run: QualityRun | null
  latest_quality_run: QualityRun | null
  active_update: Task | null
  last_successful_update: Task | null
  worker: { worker_id: string | null; task_id: string; task_status: string; heartbeat_at: string | null } | null
  active_research_task_count: number
}

export type DatasetDetail = Dataset & {
  contract: {
    partitioning: string
    fetch_granularity: string
    cadence: string
    reuse: string
    overlap_days: number
    primary_key: string[]
    sort_key: string[]
    pit_fields: string[]
    schema: Array<{ name: string; type: string }>
    sources: Array<{ source: string; endpoints: string[] }>
  }
  partitions: Array<{
    partition_key: string
    ordinal: number
    row_count: number
    content_hash: string
    schema_fingerprint: string
    input_hash: string
  }>
}

export type QualityRunDetail = QualityRun & {
  dataset_hashes: Record<string, string>
  results_complete: boolean
  result_counts: Record<'PASS' | 'FAIL' | 'SKIPPED' | 'UNKNOWN', number>
  rule_results: QualityRuleResult[]
  issues: QualityIssue[]
}

export type QualityIssue = {
    rule_id: string
    severity: string
    dataset: string
    scope: unknown
    actual: unknown
    threshold: unknown
    message: string
    remediation: string
}

export type QualityRuleResult = {
  rule_id: string
  dataset: string
  status: 'PASS' | 'FAIL' | 'SKIPPED' | 'UNKNOWN'
  severity: string
  title: string
  description: string
  pass_criterion: string
  scope: unknown
  actual: unknown
  threshold: unknown
  skip_reason: string | null
  evidence: 'RUN_SNAPSHOT' | 'LEGACY_ISSUE' | 'MISSING'
  issues: QualityIssue[]
}

export type Backtest = {
  experiment_id: string
  manifest_hash: string
  metrics: Record<string, number | string | null | unknown[]>
  nav: Array<{ trade_date: string; portfolio_nav: number; benchmark_nav: number }>
  drawdown: Array<Record<string, unknown>>
  monthly_returns: Array<Record<string, unknown>>
  exposures: Array<Record<string, unknown>>
  attribution: Array<Record<string, unknown>>
  execution_summary: Array<Record<string, unknown>>
  quality: Record<string, unknown>
}

export type FactorDefinition = { factor_ref: string; name: string; direction: 1 | -1 }
export type FactorSignalVariant = 'DIRECTION_ADJUSTED' | 'INDUSTRY_NEUTRALIZED'
export type FactorIndustryConfig = {
  taxonomy: string
  unclassified_policy: 'EXCLUDE' | 'UNCLASSIFIED'
}
export type FactorCatalog = {
  items: FactorDefinition[]
  horizons: number[]
  return_definition: string
  signal_variants: FactorSignalVariant[]
  industry: { taxonomy: string; unclassified_policies: Array<'EXCLUDE' | 'UNCLASSIFIED'> }
}
export type FactorStudyConfig = {
  factor_refs: string[]
  start_date: string
  end_date: string
  horizons: number[]
  quantiles: number
  min_cross_section: number
  ic_rolling_window: number
  ic_rolling_min_valid: number
  ic_quantile_probabilities: number[]
  universe: string
  return_definition: string
  direction_adjusted: boolean
  industry: FactorIndustryConfig | null
}
export type FactorIcSummary = {
  signal_variant: FactorSignalVariant
  factor_ref: string
  horizon: number
  long_short_mean: number | null
  pearson_ic_mean: number | null
  pearson_ic_sample_std: number | null
  pearson_icir_unannualized: number | null
  pearson_ic_positive_rate: number | null
  pearson_ic_p05: number | null
  pearson_ic_p25: number | null
  pearson_ic_p50: number | null
  pearson_ic_p75: number | null
  pearson_ic_p95: number | null
  pearson_ic_valid_date_count: number
  pearson_ic_max_positive_streak: number
  pearson_ic_positive_streak_start: string | null
  pearson_ic_positive_streak_end: string | null
  pearson_ic_max_negative_streak: number
  pearson_ic_negative_streak_start: string | null
  pearson_ic_negative_streak_end: string | null
  rank_ic_mean: number | null
  rank_ic_sample_std: number | null
  rank_icir_unannualized: number | null
  rank_ic_positive_rate: number | null
  rank_ic_p05: number | null
  rank_ic_p25: number | null
  rank_ic_p50: number | null
  rank_ic_p75: number | null
  rank_ic_p95: number | null
  rank_ic_valid_date_count: number
  rank_ic_max_positive_streak: number
  rank_ic_positive_streak_start: string | null
  rank_ic_positive_streak_end: string | null
  rank_ic_max_negative_streak: number
  rank_ic_negative_streak_start: string | null
  rank_ic_negative_streak_end: string | null
}
export type FactorIcPoint = {
  signal_variant: FactorSignalVariant
  factor_ref: string
  horizon: number
  signal_date: string
  factor_valid_count: number
  sample_count: number
  pearson_ic: number | null
  rank_ic: number | null
  rolling_window: number
  rolling_valid_count: number
  pearson_ic_rolling_mean: number | null
  rank_ic_rolling_mean: number | null
  pearson_ic_cumulative_sum: number | null
  rank_ic_cumulative_sum: number | null
  is_valid: boolean
  invalid_reason: string | null
}
export type FactorQuantileReturn = {
  signal_variant: FactorSignalVariant
  factor_ref: string
  horizon: number
  signal_date: string
  quantile: number
  count: number
  mean_return: number | null
  quantiles: number
  is_empty: boolean
}
export type FactorLongShortReturn = {
  signal_variant: FactorSignalVariant
  factor_ref: string
  horizon: number
  signal_date: string
  long_short_return: number | null
  is_valid: boolean
  invalid_reason: string | null
}
export type FactorCoverage = {
  signal_variant: FactorSignalVariant
  factor_ref: string
  signal_date: string
  eligible_count: number
  valid_count: number
  coverage: number | null
  is_valid: boolean
  quality_reason: string | null
}
export type FactorRun = {
  id: string; study_id: string; task_id: string | null; status: string
  config: FactorStudyConfig; manifest_hash: string | null; created_at: string
  started_at: string | null; completed_at: string | null
  summary?: FactorIcSummary[]; error: Record<string, unknown> | null
}
export type FactorStudy = {
  id: string; name: string; config: FactorStudyConfig; created_at: string
  latest_run: FactorRun | null; runs?: FactorRun[]
}
export type FactorSeries = {
  run_id: string; manifest_hash: string; factor_ref: string; horizon: number
  signal_variant: FactorSignalVariant
  ic: FactorIcPoint[]
  quantile_returns: FactorQuantileReturn[]
  long_short_returns: FactorLongShortReturn[]
  coverage: FactorCoverage[]
}
export type FactorCorrelation = {
  signal_variant: FactorSignalVariant
  factor_x: string
  factor_y: string
  date_count: number
  pair_count: number
  correlation: number | null
  is_valid: boolean
}
export type FactorCorrelationResponse = {
  run_id: string
  manifest_hash: string
  signal_variant: FactorSignalVariant
  data: FactorCorrelation[]
}
export type FactorIndustryCoverage = {
  signal_date: string
  taxonomy: string
  unclassified_policy: 'EXCLUDE' | 'UNCLASSIFIED'
  eligible_count: number
  classified_count: number
  tombstone_count: number
  missing_state_count: number
  usable_count: number
  classified_coverage: number
  usable_coverage: number
}
export type FactorIndustryCoverageResponse = {
  run_id: string
  manifest_hash: string
  data: FactorIndustryCoverage[]
}
