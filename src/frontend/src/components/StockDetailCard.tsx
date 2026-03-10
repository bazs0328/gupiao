import { useEffect, useState } from "react";
import type { ResearchDiagnosticsResponse, StockDetailResponse, WatchlistItem, WatchlistItemInput } from "../types";

type StockDetailCardProps = {
  stock: StockDetailResponse | null;
  watchlistItem: WatchlistItem | null;
  isSavingWatchlist: boolean;
  researchDiagnostics: ResearchDiagnosticsResponse | null;
  onLoadResearchDiagnostics: () => Promise<void>;
  onSaveWatchlist: (payload: WatchlistItemInput) => Promise<void>;
  onRemoveWatchlist: (code: string) => Promise<void>;
};

type WatchlistFormState = {
  note: string;
  target_entry: string;
  stop_loss: string;
  take_profit: string;
};

function makePolyline(points: StockDetailResponse["price_history"]) {
  if (!points.length) {
    return "";
  }
  const values = points.map((point) => point.close);
  const max = Math.max(...values);
  const min = Math.min(...values);
  return points
    .map((point, index) => {
      const x = (index / Math.max(points.length - 1, 1)) * 100;
      const y = max === min ? 50 : 100 - ((point.close - min) / (max - min)) * 100;
      return `${x},${y}`;
    })
    .join(" ");
}

function toNumberOrNull(value: string) {
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }
  const parsed = Number(trimmed);
  return Number.isFinite(parsed) ? parsed : null;
}

function percent(value: number) {
  return `${(value * 100).toFixed(1)}%`;
}

function renderTags(items: string[], emptyCopy: string, tone: "positive" | "warning" = "positive") {
  if (items.length === 0) {
    return <span className="tag-chip tag-chip-muted">{emptyCopy}</span>;
  }
  return items.map((item) => (
    <span key={item} className={`tag-chip ${tone === "warning" ? "tag-chip-warning" : "tag-chip-positive"}`}>
      {item}
    </span>
  ));
}

export function StockDetailCard({
  stock,
  watchlistItem,
  isSavingWatchlist,
  researchDiagnostics,
  onLoadResearchDiagnostics,
  onSaveWatchlist,
  onRemoveWatchlist
}: StockDetailCardProps) {
  const [watchlistForm, setWatchlistForm] = useState<WatchlistFormState>({
    note: "",
    target_entry: "",
    stop_loss: "",
    take_profit: ""
  });

  useEffect(() => {
    if (!stock) {
      return;
    }
    setWatchlistForm({
      note: watchlistItem?.note ?? "",
      target_entry: String(watchlistItem?.target_entry ?? stock.position_plan?.suggested_entry ?? stock.current_price),
      stop_loss: String(watchlistItem?.stop_loss ?? stock.position_plan?.stop_loss ?? ""),
      take_profit: String(watchlistItem?.take_profit ?? stock.position_plan?.take_profit ?? "")
    });
  }, [stock, watchlistItem]);

  if (!stock) {
    return <section className="panel detail-panel skeleton-card">从候选池或观察池里选一只股票查看计划。</section>;
  }

  const isDefaultRuleMode = stock.parameter_source === "default" && (stock.model_snapshot?.training_sample_count ?? 0) === 0;

  const primaryRisks = [...stock.minus_factors, ...stock.reason_not_to_buy_now, ...stock.ineligible_reasons].filter(
    (item, index, source) => source.indexOf(item) === index
  );

  return (
    <section className="panel detail-panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">当前焦点</p>
          <h2>
            {stock.name} <span>{stock.code}</span>
          </h2>
          <p className="price-copy">
            现价 {stock.current_price.toFixed(2)} · 信号日 {stock.signal_date} · {stock.industry}
          </p>
        </div>
        <div className="score-pill">{stock.total_score.toFixed(1)}</div>
      </div>

      <div className="detail-overview-grid">
        <div className="focus-card">
          <p className="detail-summary">{stock.explanation_summary}</p>

          <div className="detail-metric-grid">
            <div>
              <span>总分</span>
              <strong>{stock.total_score.toFixed(1)}</strong>
            </div>
            <div>
              <span>综合强度</span>
              <strong>{stock.model_snapshot?.confidence_score.toFixed(1) ?? "--"}</strong>
            </div>
            <div>
              <span>行业位置</span>
              <strong>{(stock.peer_percentiles.industry_total_score ?? 0).toFixed(1)}</strong>
            </div>
            <div>
              <span>下行风险</span>
              <strong>{stock.model_snapshot ? percent(stock.model_snapshot.downside_probability) : "--"}</strong>
            </div>
          </div>

          <div className="focus-chip-block">
            <div>
              <span className="status-label">为什么先看它</span>
              <div className="tag-row">{renderTags(stock.plus_factors, "先等更多正向信号")}</div>
            </div>
            <div>
              <span className="status-label">当前要小心</span>
              <div className="tag-row">{renderTags(primaryRisks, "暂无额外风险提醒", "warning")}</div>
            </div>
          </div>
        </div>

        <div className="plan-focus-card">
          <span className="status-label">操作计划</span>
          {stock.position_plan ? (
            <>
              <strong>优先等回到计划价位附近，再按波动率执行</strong>
              <p>止损和建议仓位会按近 20 日波动率自适应调整，不再统一固定。</p>
              <div className="detail-metric-grid">
                <div>
                  <span>建议介入</span>
                  <strong>{stock.position_plan.suggested_entry.toFixed(2)}</strong>
                </div>
                <div>
                  <span>自适应止损</span>
                  <strong>{stock.position_plan.stop_loss.toFixed(2)}</strong>
                </div>
                <div>
                  <span>止盈</span>
                  <strong>{stock.position_plan.take_profit.toFixed(2)}</strong>
                </div>
                <div>
                  <span>建议仓位</span>
                  <strong>{(stock.position_plan.suggested_weight * 100).toFixed(1)}%</strong>
                </div>
              </div>
            </>
          ) : (
            <>
              <strong>暂时先观察</strong>
              <p>当前没有明确的建仓计划，更适合等价格和证据继续靠拢。</p>
            </>
          )}
        </div>
      </div>

      <div className="detail-grid">
        <div className="chart-card">
          <div className="mini-chart-header">
            <span>最近 90 日收盘走势</span>
            <span>{stock.signal_date}</span>
          </div>
          <svg viewBox="0 0 100 100" className="mini-chart" preserveAspectRatio="none">
            <polyline fill="none" stroke="currentColor" strokeWidth="2.5" points={makePolyline(stock.price_history)} />
          </svg>
        </div>

        <div className="action-card">
          <div className="subsection-header">
            <h3>关键风险</h3>
            <span>{primaryRisks.length} 条</span>
          </div>
          <div className="signal-list">
            {primaryRisks.map((item) => (
              <div key={item} className="signal-card signal-warning">
                <strong>先确认</strong>
                <p>{item}</p>
              </div>
            ))}
            {primaryRisks.length === 0 ? <div className="empty-card">目前没有明显的额外风险提示。</div> : null}
          </div>
        </div>
      </div>

      <div className="action-card">
        <div className="subsection-header">
          <h3>观察池计划</h3>
          <span>{watchlistItem ? "已加入观察池" : "未加入观察池"}</span>
        </div>
        <div className="compact-grid">
          <label>
            <span>目标价</span>
            <input
              value={watchlistForm.target_entry}
              onChange={(event) => setWatchlistForm((current) => ({ ...current, target_entry: event.target.value }))}
            />
          </label>
          <label>
            <span>止损价</span>
            <input
              value={watchlistForm.stop_loss}
              onChange={(event) => setWatchlistForm((current) => ({ ...current, stop_loss: event.target.value }))}
            />
          </label>
          <label>
            <span>止盈价</span>
            <input
              value={watchlistForm.take_profit}
              onChange={(event) => setWatchlistForm((current) => ({ ...current, take_profit: event.target.value }))}
            />
          </label>
        </div>
        <textarea
          className="description-input compact-textarea"
          value={watchlistForm.note}
          placeholder="写下为什么看它、什么价位再看、什么条件算失效。"
          onChange={(event) => setWatchlistForm((current) => ({ ...current, note: event.target.value }))}
        />
        <div className="inline-actions">
          <button
            type="button"
            className="secondary-button"
            disabled={isSavingWatchlist}
            onClick={() =>
              void onSaveWatchlist({
                code: stock.code,
                note: watchlistForm.note,
                target_entry: toNumberOrNull(watchlistForm.target_entry),
                stop_loss: toNumberOrNull(watchlistForm.stop_loss),
                take_profit: toNumberOrNull(watchlistForm.take_profit)
              })
            }
          >
            {isSavingWatchlist ? "保存中..." : watchlistItem ? "更新观察池" : "加入观察池"}
          </button>
          {watchlistItem ? (
            <button
              type="button"
              className="ghost-button"
              disabled={isSavingWatchlist}
              onClick={() => void onRemoveWatchlist(stock.code)}
            >
              移出观察池
            </button>
          ) : null}
        </div>
      </div>

      <details
        className="disclosure-card"
        onToggle={(event) => {
          const open = (event.currentTarget as HTMLDetailsElement).open;
          if (open && !researchDiagnostics) {
            void onLoadResearchDiagnostics();
          }
        }}
      >
        <summary className="disclosure-summary">展开高级分析</summary>
        <div className="disclosure-body">
          {researchDiagnostics ? (
            <div className="financial-card">
              <h3>研究诊断</h3>
              <dl>
                <div>
                  <dt>状态</dt>
                  <dd>{researchDiagnostics.status}</dd>
                </div>
                <div>
                  <dt>参数版本</dt>
                  <dd>{researchDiagnostics.parameter_version ?? "暂无"}</dd>
                </div>
                <div>
                  <dt>样本覆盖</dt>
                  <dd>
                    {researchDiagnostics.sample_count} / 高质量 {researchDiagnostics.headline_sample_count}
                  </dd>
                </div>
                <div>
                  <dt>最近覆盖</dt>
                  <dd>{researchDiagnostics.as_of_date ?? "暂无"}</dd>
                </div>
              </dl>
              {researchDiagnostics.factor_drift.length ? (
                <div className="mini-list">
                  {researchDiagnostics.factor_drift.slice(0, 3).map((item) => (
                    <div key={item.factor} className="mini-card">
                      <strong>{item.factor}</strong>
                      <p>漂移 {item.drift.toFixed(2)}</p>
                    </div>
                  ))}
                </div>
              ) : null}
              {researchDiagnostics.warnings.length ? <p>{researchDiagnostics.warnings[0]}</p> : null}
            </div>
          ) : null}

          <div className="detail-grid">
            <div className="financial-card">
              <h3>财务快照</h3>
              {stock.financial_snapshot ? (
                <dl>
                  <div>
                    <dt>ROE</dt>
                    <dd>{stock.financial_snapshot.roe.toFixed(1)}%</dd>
                  </div>
                  <div>
                    <dt>营收同比</dt>
                    <dd>{stock.financial_snapshot.revenue_yoy.toFixed(1)}%</dd>
                  </div>
                  <div>
                    <dt>净利同比</dt>
                    <dd>{stock.financial_snapshot.profit_yoy.toFixed(1)}%</dd>
                  </div>
                  <div>
                    <dt>现金流质量</dt>
                    <dd>{stock.financial_snapshot.cashflow_quality.toFixed(2)}</dd>
                  </div>
                  <div>
                    <dt>PE / PB</dt>
                    <dd>
                      {stock.financial_snapshot.pe_ttm.toFixed(1)} / {stock.financial_snapshot.pb.toFixed(1)}
                    </dd>
                  </div>
                  <div>
                    <dt>披露日期</dt>
                    <dd>{stock.financial_snapshot.publish_date}</dd>
                  </div>
                </dl>
              ) : (
                <p>暂无财务快照。</p>
              )}
            </div>

            {stock.model_snapshot ? (
              <div className="financial-card">
                <h3>{isDefaultRuleMode ? "评分快照" : "模型快照"}</h3>
                <dl>
                  <div>
                    <dt>Alpha / 质量 / 风险</dt>
                    <dd>
                      {stock.model_snapshot.alpha_score.toFixed(1)} / {stock.model_snapshot.quality_score.toFixed(1)} /{" "}
                      {stock.model_snapshot.risk_score.toFixed(1)}
                    </dd>
                  </div>
                  <div>
                    <dt>下行风险概率</dt>
                    <dd>{percent(stock.model_snapshot.downside_probability)}</dd>
                  </div>
                  <div>
                    <dt>综合强度</dt>
                    <dd>{stock.model_snapshot.confidence_score.toFixed(1)}</dd>
                  </div>
                  {isDefaultRuleMode ? (
                    <div>
                      <dt>模式</dt>
                      <dd>规则默认</dd>
                    </div>
                  ) : (
                    <>
                      <div>
                        <dt>训练样本</dt>
                        <dd>{stock.model_snapshot.training_sample_count}</dd>
                      </div>
                      <div>
                        <dt>验证状态</dt>
                        <dd>{stock.model_snapshot.validation_health}</dd>
                      </div>
                    </>
                  )}
                  <div>
                    <dt>行业分位</dt>
                    <dd>{(stock.peer_percentiles.industry_total_score ?? 0).toFixed(1)}</dd>
                  </div>
                </dl>
              </div>
            ) : null}
          </div>

          {stock.evidence_snapshot ? (
            <div className="financial-card">
              <h3>高级证据</h3>
              <dl>
                <div>
                  <dt>板块</dt>
                  <dd>{stock.evidence_snapshot.board}</dd>
                </div>
                <div>
                  <dt>因子覆盖</dt>
                  <dd>{stock.evidence_snapshot.factor_coverage_pct.toFixed(1)}%</dd>
                </div>
                <div>
                  <dt>校准桶</dt>
                  <dd>
                    {stock.evidence_snapshot.calibration_bucket} / {stock.evidence_snapshot.bucket_sample_count}
                  </dd>
                </div>
                <div>
                  <dt>预期超额</dt>
                  <dd>{percent(stock.evidence_snapshot.expected_bucket_excess_return)}</dd>
                </div>
                <div>
                  <dt>一致性</dt>
                  <dd>{stock.evidence_snapshot.agreement_count} / 3</dd>
                </div>
                <div>
                  <dt>同步批次</dt>
                  <dd>{stock.evidence_snapshot.sync_run_id ?? "暂无"}</dd>
                </div>
              </dl>
            </div>
          ) : null}

          {stock.contribution_breakdown.length > 0 ? (
            <div className="report-section">
              <div className="subsection-header">
                <h3>贡献最大的 5 个因子</h3>
                <span>高级解释</span>
              </div>
              <div className="mini-list">
                {stock.contribution_breakdown.slice(0, 5).map((item) => (
                  <div key={item.factor} className="mini-card">
                    <strong>
                      {item.label} <span>{item.group}</span>
                    </strong>
                    <p>
                      分数 {item.score.toFixed(1)} / 贡献 {item.contribution.toFixed(2)}
                    </p>
                  </div>
                ))}
              </div>
            </div>
          ) : null}

          <div className="factor-list">
            {stock.factor_scores.map((factor) => (
              <div className="factor-row" key={factor.factor}>
                <div>
                  <strong>{factor.label}</strong>
                  <span>{factor.group}</span>
                </div>
                <div className="factor-score">
                  <span>
                    raw {factor.raw_value.toFixed(2)} / 贡献 {factor.contribution.toFixed(2)}
                  </span>
                  <strong>{factor.score.toFixed(1)}</strong>
                </div>
              </div>
            ))}
            {stock.factor_scores.length === 0 ? <div className="empty-card">暂无更细的因子明细。</div> : null}
          </div>

          <div className="detail-grid">
            <div className="action-card">
              <div className="subsection-header">
                <h3>事件日历</h3>
                <span>{stock.event_calendar.length} 项</span>
              </div>
              <div className="signal-list">
                {stock.event_calendar.map((item) => (
                  <div key={`${item.event_type}-${item.event_date}`} className={`signal-card signal-${item.severity}`}>
                    <strong>
                      {item.title} <span>{item.event_date}</span>
                    </strong>
                  </div>
                ))}
                {stock.event_calendar.length === 0 ? <div className="empty-card">近期没有新的事件提醒。</div> : null}
              </div>
            </div>

            <div className="action-card">
              <div className="subsection-header">
                <h3>相似历史样本</h3>
                <span>{stock.similar_history_samples.length} 条</span>
              </div>
              <div className="signal-list">
                {stock.similar_history_samples.map((item) => (
                  <div key={`${item.code}-${item.signal_date}`} className="signal-card">
                    <strong>
                      {item.name} <span>{item.signal_date}</span>
                    </strong>
                    <p>{item.summary}</p>
                  </div>
                ))}
                {stock.similar_history_samples.length === 0 ? <div className="empty-card">暂无相似样本。</div> : null}
              </div>
            </div>
          </div>
        </div>
      </details>
    </section>
  );
}
