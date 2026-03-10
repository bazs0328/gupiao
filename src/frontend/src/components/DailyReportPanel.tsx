import type { DailyReportResponse } from "../types";

type DailyReportPanelProps = {
  report: DailyReportResponse | null;
  onSelectCode: (code: string) => void;
};

function regimeLabel(regime: DailyReportResponse["market_regime"] | undefined) {
  switch (regime) {
    case "bullish":
      return "偏强";
    case "cautious":
      return "谨慎";
    default:
      return "中性";
  }
}

function formatPrice(value: number | null | undefined) {
  if (value == null) {
    return "--";
  }
  return value.toFixed(2);
}

export function DailyReportPanel({ report, onSelectCode }: DailyReportPanelProps) {
  if (!report) {
    return <section className="panel report-panel skeleton-card">生成候选概览中...</section>;
  }

  const leadCandidate = report.candidates[0] ?? null;
  const doNotItems =
    report.do_not_chase_flags.length > 0
      ? report.do_not_chase_flags
      : report.block_reasons.length > 0
        ? report.block_reasons
        : ["没有额外的追价提醒，但今天仍然只做计划内交易。"];

  return (
    <section className="panel report-panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">今日候选</p>
          <h2>{report.report_date}</h2>
        </div>
        <div className={`regime-pill regime-${report.market_regime}`}>{regimeLabel(report.market_regime)}</div>
      </div>

      <div className="report-hero">
        <div className="report-hero-copy">
          <p className="detail-summary">{report.summary}</p>
          <div className="tag-row">
            <span className={`tag-chip ${report.actionable ? "tag-chip-positive" : "tag-chip-muted"}`}>
              {report.actionable ? "可以开始筛" : "先等数据恢复"}
            </span>
            <span className="tag-chip">{report.candidates.length} 只候选</span>
            <span className="tag-chip">{report.watchlist_hits.length} 只命中观察池</span>
          </div>
          <div className="plan-strip">{report.capital_allocation_hint}</div>
        </div>

        <div className="lead-card">
          <span className="status-label">第一眼先看</span>
          {leadCandidate ? (
            <>
              <strong>
                {leadCandidate.name} <span>{leadCandidate.code}</span>
              </strong>
              <p>{leadCandidate.explanation_summary}</p>
              <p>
                现价 {formatPrice(leadCandidate.current_price)} / 建议介入{" "}
                {formatPrice(leadCandidate.position_plan?.suggested_entry)}
              </p>
              <p>止损和建议仓位会按近期波动率自适应调整。</p>
              <button type="button" className="primary-button" onClick={() => onSelectCode(leadCandidate.code)}>
                查看这只股票
              </button>
            </>
          ) : (
            <>
              <strong>今天先等等</strong>
              <p>没有特别突出的新票时，更适合复核观察池里的原计划。</p>
            </>
          )}
        </div>
      </div>

      {!report.actionable && report.block_reasons.length > 0 ? (
        <div className="warning-box">当前暂停筛选：{report.block_reasons.join("、")}</div>
      ) : null}

      <div className="metric-grid">
        <div>
          <span>今日候选</span>
          <strong>{report.candidates.length}</strong>
        </div>
        <div>
          <span>计划价位</span>
          <strong>{report.position_plans.length}</strong>
        </div>
        <div>
          <span>观察池命中</span>
          <strong>{report.watchlist_hits.length}</strong>
        </div>
      </div>

      <div className="report-section">
        <div className="subsection-header">
          <h3>今天先看</h3>
          <span>优先看最容易下决定的 1 到 4 只</span>
        </div>
        <div className="candidate-list">
          {report.candidates.slice(0, 4).map((candidate) => (
            <button
              key={candidate.code}
              type="button"
              className="candidate-card"
              onClick={() => onSelectCode(candidate.code)}
            >
              <strong>
                {candidate.name} <span>{candidate.code}</span>
              </strong>
              <em>
                现价 {formatPrice(candidate.current_price)} / 建议介入{" "}
                {formatPrice(candidate.position_plan?.suggested_entry)}
              </em>
              <p>{candidate.explanation_summary}</p>
              <div className="tag-row">
                {candidate.risk_flags.slice(0, 2).map((flag) => (
                  <span key={flag} className="tag-chip tag-chip-warning">
                    {flag}
                  </span>
                ))}
                {candidate.risk_flags.length === 0 ? <span className="tag-chip">计划位清晰</span> : null}
              </div>
            </button>
          ))}
          {report.candidates.length === 0 ? <div className="empty-card">今天没有必须立刻处理的新候选。</div> : null}
        </div>
      </div>

      <div className="report-section">
        <div className="subsection-header">
          <h3>为什么看</h3>
          <span>把今天最值得复核的动作翻成人话</span>
        </div>
        <div className="signal-list">
          {report.action_checklist.map((item) => (
            <div key={item} className="signal-card">
              <strong>关注点</strong>
              <p>{item}</p>
            </div>
          ))}
        </div>

        {report.watchlist_hits.length > 0 ? (
          <div className="mini-list">
            {report.watchlist_hits.map((item) => (
              <button key={item.code} type="button" className="mini-card" onClick={() => onSelectCode(item.code)}>
                <strong>
                  {item.name} <span>{item.code}</span>
                </strong>
                <p>{item.note || "这只股票已经在观察池里，建议先复核原计划。"}</p>
              </button>
            ))}
          </div>
        ) : null}
      </div>

      <div className="report-section">
        <div className="subsection-header">
          <h3>今天别做什么</h3>
          <span>先把容易犯错的动作排掉</span>
        </div>
        <div className="signal-list">
          {doNotItems.map((item) => (
            <div key={item} className="signal-card signal-warning">
              <strong>别急</strong>
              <p>{item}</p>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}
