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

export type TaskProgress = {
  stage: string
  completed: number
  total: number
  message: string
  context: Record<string, unknown>
}

export type Task = {
  id: string
  subject_kind: string | null
  subject_id: string | null
  task_type: string
  status: string
  priority: number
  progress: TaskProgress
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
  progress: TaskProgress
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
  substage: string | null
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

export type StrategyStudyStatus = 'QUEUED' | 'RUNNING' | 'SUCCEEDED' | 'FAILED' | 'CANCELLED'

export type DualMAStrategyParameters = {
  instrument_id: string
  short_window: number
  long_window: number
  long_weight: number
  flat_weight: number
  target_tolerance: number
}

export type StrategyComponentRef = {
  model_id: string
  params?: Record<string, unknown>
}

export type CrossSectionalPipelineParameters = {
  pipeline: {
    frequency: 'MONTHLY' | 'WEEKLY'
    target_tolerance: number
    alpha: StrategyComponentRef
    risk: StrategyComponentRef
    cost: StrategyComponentRef
    construction: StrategyComponentRef
    constraints: StrategyComponentRef
  }
}

export type StrategyStudyStrategy =
  | { strategy_id: 'dual_ma_trend'; parameters: DualMAStrategyParameters }
  | { strategy_id: 'etf_rotation'; parameters: CrossSectionalPipelineParameters }
  | { strategy_id: 'stock_multifactor'; parameters: CrossSectionalPipelineParameters }

export type StrategyStudyDefinition = {
  name: string
  description: string
  tags: string[]
  start_date: string
  end_date: string
  strategy: StrategyStudyStrategy
  benchmark: string
  initial_cash_fen: number
  execution: {
    reference_price: string
    slippage_bps: number
    max_volume_participation: number
    limit_order_policy: string
  }
}

export type StrategyStudy = {
  id: string
  definition: StrategyStudyDefinition
  config_hash: string
  catalog_hash: string
  status: StrategyStudyStatus
  stage: string
  task_id: string | null
  artifact_dir: string | null
  manifest_hash: string | null
  error: Record<string, unknown> | null
  created_at: string
  started_at: string | null
  completed_at: string | null
  metrics: Array<{
    name: string
    value: number
    unit: string | null
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

export type StrategyStudyQualityDisclosure = {
  calculation_mode: string
  risk_free_rate_annual: number
  undefined_metrics: Record<string, string>
  unavailable_dimensions: Record<string, string>
  attribution_method: string
  warnings: string[]
}

export type StrategyPerformanceArtifactRow = {
  [column: string]: string | number | boolean | null | undefined
  trade_date: string
  return: number
  benchmark_return: number
  cumulative_return: number
  benchmark_cumulative_return: number
  active_return: number
  nav: number
  benchmark_nav: number
  drawdown: number
  active_drawdown: number
}

export type PeriodReturnArtifactRow = {
  [column: string]: string | number | boolean | null | undefined
  year: number
  month?: number
  period_start: string
  period_end: string
  portfolio_return: number
  benchmark_return: number
  relative_return: number
}

export type ExecutionSummaryArtifactRow = {
  [column: string]: string | number | boolean | null | undefined
  side: string
  reason_code: string
  order_count: number
  requested_quantity: number
  filled_quantity: number
  unfilled_quantity: number
  priced_requested_notional_fen: number
  priced_filled_notional_fen: number
  unpriced_order_count: number
}

export type FactorSummaryArtifactRow = {
  [column: string]: string | number | boolean | null | undefined
  signal_variant: string
  factor_ref: string
  horizon: number
  rank_ic_mean: number | null
  rank_icir_unannualized: number | null
  adjusted_p_value: number | null
  long_short_mean: number | null
}

export type FactorSeriesArtifactRow = {
  [column: string]: string | number | boolean | null | undefined
  signal_variant: string
  factor_ref: string
  horizon?: number
  signal_date: string
  rank_ic?: number | null
  rank_ic_rolling_mean?: number | null
  pearson_ic?: number | null
  coverage?: number | null
  mean_return?: number | null
  long_short_return?: number | null
}

export type ArtifactRow = {
  [column: string]: string | number | boolean | null
}

export type StrategyArtifactRow =
  | StrategyPerformanceArtifactRow
  | PeriodReturnArtifactRow
  | ExecutionSummaryArtifactRow
  | FactorSummaryArtifactRow
  | FactorSeriesArtifactRow
  | ArtifactRow

export type StrategyStudyValidation = {
  config_hash: string
  normalized: StrategyStudyDefinition
}

export type StrategyCatalog = {
  strategies: string[]
  components: Record<string, string[]>
  component_schemas: Record<string, Array<{ model_id: string; params_schema: Record<string, unknown> }>>
  capability_rules: Array<Record<string, unknown>>
}

export type FactorDecisionMark = 'UNREVIEWED' | 'CANDIDATE' | 'DISCARDED'
export type FactorStudyStatus = 'QUEUED' | 'RUNNING' | 'SUCCEEDED' | 'FAILED' | 'CANCELLED'

export type FactorStudyDefinition = {
  name: string
  description: string
  tags: string[]
  start_date: string
  end_date: string
  correction: 'BONFERRONI' | 'BH_FDR'
  factor_ids: string[]
  universe: { name: 'CN_STOCK_STANDARD' }
  horizons: number[]
  quantiles: number
  industry: { taxonomy: 'SW2021'; unclassified_policy: 'EXCLUDE' | 'UNCLASSIFIED' } | null
  market_cap: { exposure: 'LOG_TOTAL_MARKET_VALUE' } | null
  cost_bps_scenarios: number[]
}

export type FactorStudyDecision = {
  signal_variant: string
  label_kind: string
  factor_ref: string
  horizon: number
  mark: Exclude<FactorDecisionMark, 'UNREVIEWED'>
  note: string
  actor: string
  updated_at: string
}

export type FactorStudy = {
  id: string
  definition: FactorStudyDefinition
  config_hash: string
  catalog_hash: string
  status: FactorStudyStatus
  stage: string
  task_id: string
  artifact_dir: string | null
  manifest_hash: string | null
  error: Record<string, unknown> | null
  created_at: string
  started_at: string | null
  completed_at: string | null
  metrics: Array<{ name: string; value: number; unit: string | null; p_value: number | null; adjusted_p_value: number | null }>
  artifacts: Array<{ artifact_type: string; relative_path: string; content_hash: string; byte_count: number; row_count: number | null; schema: Record<string, string> | null }>
  decisions: FactorStudyDecision[]
}

export type FactorStudyOverview = FactorStudy & {
  matrix_total: number
  candidate_count: number
  discarded_count: number
  unreviewed_count: number
}

export type FactorStudyMatrixRow = {
  signal_variant: string
  label_kind: string
  factor_ref: string
  horizon: number
  rank_ic_mean: number | null
  rank_ic_hac_t_stat: number | null
  rank_ic_adjusted_p_value: number | null
  monotonicity_mean: number | null
  gross_spread_mean: number | null
  break_even_cost_bps: number | null
  total_turnover_mean: number | null
  summary_metrics: Record<string, string | number | null>
  decision: FactorStudyDecision | null
}

export type FactorStudyCatalog = {
  factors: Array<{ factor_id: string }>
  universes: string[]
  corrections: Array<'BONFERRONI' | 'BH_FDR'>
  industry_policies: Array<'EXCLUDE' | 'UNCLASSIFIED'>
  label_kinds: string[]
}

export type FactorStudyValidation = {
  config_hash: string
  normalized: FactorStudyDefinition
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
      metric: 'pe_ttm' | 'pb' | 'ps_ttm'
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
  initialization: {
    status: 'NOT_STARTED' | 'IN_PROGRESS' | 'COMPLETED'
    years: number | null
    start_date: string | null
    end_date: string | null
    started_at: string | null
    completed_at: string | null
  }
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

export type DashboardSettings = {
  settings_path: string
  data_source_token: {
    configured: boolean
    source: 'DATA_ROOT_ENV' | 'PROCESS_ENVIRONMENT' | 'NONE'
    updated_at: string | null
  }
  data_source_rate_limit: {
    requests_per_minute: number
    source: 'DATA_ROOT_ENV' | 'PROCESS_ENVIRONMENT' | 'DEFAULT'
    updated_at: string | null
  }
  data_source_proxy: {
    url: string | null
    source: 'DATA_ROOT_ENV' | 'PROCESS_ENVIRONMENT' | 'NONE'
    updated_at: string | null
  }
  data_source_concurrency: {
    max_concurrent_requests: number
    source: 'DATA_ROOT_ENV' | 'PROCESS_ENVIRONMENT' | 'DEFAULT'
    updated_at: string | null
  }
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
