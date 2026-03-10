import type { RankingRow } from "../types";

type RankingTableProps = {
  rows: RankingRow[];
  selectedCode: string | null;
  onSelect: (code: string) => void;
};

function formatWeight(value: number | undefined) {
  if (value == null) {
    return "待分配";
  }
  return `${(value * 100).toFixed(1)}%`;
}

function formatPrice(value: number | undefined) {
  if (value == null) {
    return "--";
  }
  return value.toFixed(2);
}

function actionLabel(row: RankingRow) {
  if (!row.actionable) {
    return "先观察";
  }
  return row.tier === "A" ? "优先看" : "次优先";
}

function tierClassName(tier: RankingRow["tier"]) {
  switch (tier) {
    case "A":
      return "a";
    case "B":
      return "b";
    default:
      return "watch";
  }
}

export function RankingTable({ rows, selectedCode, onSelect }: RankingTableProps) {
  return (
    <section className="panel table-panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">候选池</p>
          <h2>按顺序看，不要一次看太多</h2>
        </div>
        <span className="table-count">{rows.length} 只</span>
      </div>

      <p className="detail-summary">默认只看核心结论、价位和风险提醒。止损和仓位会随近期波动率调整，更多分项数据放在展开内容里。</p>

      <div className="ranking-card-grid">
        {rows.map((row, index) => (
          <article key={row.code} className={`ranking-card ${selectedCode === row.code ? "selected" : ""}`}>
            <button type="button" className="ranking-card-trigger" onClick={() => onSelect(row.code)}>
              <div className="ranking-card-topline">
                <span className="ranking-rank">TOP {index + 1}</span>
                <span className={`ranking-badge ranking-badge-${tierClassName(row.tier)}`}>{actionLabel(row)}</span>
              </div>

              <div className="ranking-card-header">
                <div>
                  <strong>{row.name}</strong>
                  <span>
                    {row.code} · {row.industry}
                  </span>
                </div>
                <div className="ranking-score">
                  <strong>{row.total_score.toFixed(1)}</strong>
                  <span>总分</span>
                </div>
              </div>

              <p>{row.explanation_summary}</p>

              <div className="ranking-meta-grid">
                <div>
                  <span>现价</span>
                  <strong>{row.current_price.toFixed(2)}</strong>
                </div>
                <div>
                  <span>综合强度</span>
                  <strong>{row.confidence_score.toFixed(1)}</strong>
                </div>
                <div>
                  <span>建议介入</span>
                  <strong>{formatPrice(row.position_plan?.suggested_entry)}</strong>
                </div>
                <div>
                  <span>建议仓位</span>
                  <strong>{formatWeight(row.position_plan?.suggested_weight)}</strong>
                </div>
              </div>

              <div className="tag-row">
                <span className="tag-chip">行业第 {row.peer_rank_in_industry}</span>
                {row.risk_flags.slice(0, 2).map((flag) => (
                  <span key={flag} className="tag-chip tag-chip-warning">
                    {flag}
                  </span>
                ))}
                {row.risk_flags.length === 0 ? <span className="tag-chip">风险提醒较少</span> : null}
              </div>
            </button>

            <details className="disclosure-card ranking-disclosure">
              <summary className="disclosure-summary">展开更多数据</summary>
              <div className="disclosure-body">
                <div className="detail-metric-grid">
                  <div>
                    <span>走势强度</span>
                    <strong>{(row.section_scores.technical ?? 0).toFixed(1)}</strong>
                  </div>
                  <div>
                    <span>基本面</span>
                    <strong>{(row.section_scores.fundamental ?? 0).toFixed(1)}</strong>
                  </div>
                  <div>
                    <span>风险约束</span>
                    <strong>{(row.section_scores.risk ?? 0).toFixed(1)}</strong>
                  </div>
                  <div>
                    <span>持有窗口</span>
                    <strong>{row.expected_holding_window}</strong>
                  </div>
                </div>

                {row.position_plan ? (
                  <div className="plan-strip">
                    建议介入 {row.position_plan.suggested_entry.toFixed(2)} / 自适应止损{" "}
                    {row.position_plan.stop_loss.toFixed(2)} / 止盈 {row.position_plan.take_profit.toFixed(2)}
                  </div>
                ) : null}

                {row.block_reasons.length > 0 ? (
                  <div className="warning-box">暂缓原因：{row.block_reasons.join("、")}</div>
                ) : null}

                {row.soft_penalties.length > 0 ? (
                  <div className="mini-list">
                    {row.soft_penalties.map((item) => (
                      <div key={item.label} className="mini-card">
                        <strong>{item.label}</strong>
                        <p>扣分 {item.points.toFixed(0)}</p>
                      </div>
                    ))}
                  </div>
                ) : null}
              </div>
            </details>
          </article>
        ))}

        {rows.length === 0 ? <div className="empty-card">同步完成后，这里会给出今天最值得先看的股票。</div> : null}
      </div>
    </section>
  );
}
